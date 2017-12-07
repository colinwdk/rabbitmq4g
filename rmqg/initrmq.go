package rmqg

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/satori/go.uuid"
	"github.com/streadway/amqp"
)

const (
	// chan
	ChannelBufferLength = 100

	//worker number
	ReceiverNum = 5
	SenderNum   = 5
)

type Rmqg struct {
	Url         string //addr url
	Exchanges   map[string]*ExchangeConfig
	Queues      map[string]*QueueConfig
	SendChannel chan MessageSend
}

type ExchangeConfig struct {
	ExchangeName string
	ExchangeType string

	//routingkeyQueues [][]interface{} // routingKey + *QueueConfig
	//routingKey []string
	BindQueues map[string][]*QueueConfig //string=routing_key
}

type QueueConfig struct {
	QueueName       string
	RoutingKey      []string
	NotifyPath      string
	NotifyTimeout   int
	RetryTimes      int
	RetryDuration   int
	BindingExchange string

	//project *ProjectConfig
}

type NotifyResponse int
type Message struct {
	queueConfig    QueueConfig
	amqpDelivery   *amqp.Delivery // message read from rabbitmq
	notifyResponse NotifyResponse // notify result from callback url
}

type Publishing amqp.Publishing
type MessageSend struct {
	//queueConfig    *QueueConfig
	Exchange   *ExchangeConfig
	RoutingKey string

	AmqpPublishing Publishing // message send to rabbitmq
	//notifyResponse NotifyResponse   // notify result from callback url
}

//*****************************

func (this *Rmqg) InitRmqp() {

}

func LogOnError(err error) {
	if err != nil {
		fmt.Printf("ERROR - %s\n", err)
	}
}

func PanicOnError(err error) {
	if err != nil {
		panic(err)
	}
}

func (qc *QueueConfig) DeclareExchanges(channel *amqp.Channel) {
	exchanges := []string{
		qc.WorkerExchangeName(),
		qc.RetryExchangeName(),
		qc.ErrorExchangeName(),
		qc.RequeueExchangeName(),
		//qc.BindingExchange,
	}

	for _, e := range exchanges {
		log.Printf("declaring exchange: %s\n", e)

		err := channel.ExchangeDeclare(e, "topic", true, false, false, false, nil)
		PanicOnError(err)
	}
}

func (qc *QueueConfig) DeclareQueues(channel *amqp.Channel) {
	var err error
	// 定义重试队列
	log.Printf("declaring retry queue: %s\n", qc.RetryQueueName())
	retryQueueOptions := map[string]interface{}{
		"x-dead-letter-exchange": qc.RequeueExchangeName(),
		"x-message-ttl":          int32(qc.RetryDurationWithDefault() * 1000),
	}
	_, err = channel.QueueDeclare(qc.RetryQueueName(), true, false, false, false, retryQueueOptions)
	PanicOnError(err)
	err = channel.QueueBind(qc.RetryQueueName(), "#", qc.RetryExchangeName(), false, nil)
	PanicOnError(err)

	// 定义错误队列
	log.Printf("declaring error queue: %s\n", qc.ErrorQueueName())
	_, err = channel.QueueDeclare(qc.ErrorQueueName(), true, false, false, false, nil)
	PanicOnError(err)
	err = channel.QueueBind(qc.ErrorQueueName(), "#", qc.ErrorExchangeName(), false, nil)
	PanicOnError(err)

	// 定义工作队列
	log.Printf("declaring worker queue: %s\n", qc.WorkerQueueName())
	workerQueueOptions := map[string]interface{}{
		"x-dead-letter-exchange": qc.RetryExchangeName(),
	}
	_, err = channel.QueueDeclare(qc.WorkerQueueName(), true, false, false, false, workerQueueOptions)
	PanicOnError(err)

	for _, key := range qc.RoutingKey {
		err = channel.QueueBind(qc.WorkerQueueName(), key, qc.WorkerExchangeName(), false, nil)
		PanicOnError(err)
	}

	// 最后，绑定工作队列 和 requeue Exchange
	err = channel.QueueBind(qc.WorkerQueueName(), "#", qc.RequeueExchangeName(), false, nil)
	PanicOnError(err)
}

//*******************
//*******************rmqg
func (this *Rmqg) DeclareQueues(channel *amqp.Channel) {
	var err error
	for _, q := range this.Queues {
		log.Printf("declaring queue: %s\n", q)
		_, err = channel.QueueDeclare(q.QueueName, true, false, false, false, nil)
		PanicOnError(err)
	}
}

func (this *Rmqg) DeclareExchanges(channel *amqp.Channel) {
	for _, e := range this.Exchanges {
		log.Printf("declaring exchange: %s\n", e)

		if err := channel.ExchangeDeclare(e.ExchangeName, e.ExchangeType, true, false, false, false, nil); err != nil {
			log.Printf("channel ExchangeDeclare error | %s | %s | %s \n", e.ExchangeName, e.ExchangeType, err)
			///PanicOnError(err)
			continue
		}

		for k, v := range e.BindQueues {
			for _, q := range v {
				if err := channel.QueueBind(q.QueueName, k, e.ExchangeName, false, nil); err != nil {
					log.Printf("QueueBind error | %s | %s | %s | %s \n", q.QueueName, k, e.ExchangeName, err)
				}
			}
		}
	}
}

func (this *Rmqg) SetupChannel() (*amqp.Connection, *amqp.Channel, error) {
	//url := os.Getenv("AMQP_URL")

	conn, err := amqp.Dial(this.Url)
	if err != nil {
		LogOnError(err)
		return nil, nil, err
	}

	channel, err := conn.Channel()
	if err != nil {
		LogOnError(err)
		return nil, nil, err
	}

	err = channel.Qos(1, 0, false)
	if err != nil {
		LogOnError(err)
		return nil, nil, err
	}

	log.Printf("setup channel success!")

	return conn, channel, nil
}

func (this *Rmqg) ReceiveMessages(queues []*QueueConfig, done <-chan struct{}) <-chan Message {
	out := make(chan Message, ChannelBufferLength)
	var wg sync.WaitGroup

	receiver := func(qc QueueConfig) {
		defer wg.Done()

	RECONNECT:
		for {
			_, channel, err := this.SetupChannel()
			if err != nil {
				PanicOnError(err)
			}

			msgs, err := channel.Consume(
				qc.WorkerQueueName(), // queue
				"",                   // consumer
				false,                // auto-ack
				false,                // exclusive
				false,                // no-local
				false,                // no-wait
				nil,                  // args
			)
			PanicOnError(err)

			for {
				select {
				case msg, ok := <-msgs:
					if !ok {
						log.Printf("receiver: channel is closed, maybe lost connection")
						time.Sleep(5 * time.Second)
						continue RECONNECT
					}
					msg.MessageId = fmt.Sprintf("%s", uuid.NewV4())
					message := Message{qc, &msg, 0}
					out <- message

					///message.Printf("receiver: received msg")
				case <-done:
					log.Printf("receiver: received a done signal")
					return
				}
			}
		}
	}

	for _, queue := range queues {
		wg.Add(ReceiverNum)
		for i := 0; i < ReceiverNum; i++ {
			go receiver(*queue)
		}
	}

	go func() {
		wg.Wait()
		log.Printf("all receiver is done, closing channel")
		close(out)
	}()

	return out
}

func (this *Rmqg) SendMessages() <-chan Message { //in <-chan MessageSend
	out := make(chan Message)

	var wg sync.WaitGroup

	resender := func() {
		defer wg.Done()

	RECONNECT:
		for {
			conn, channel, err := this.SetupChannel()
			if err != nil {
				PanicOnError(err)
			}

			for m := range this.SendChannel { //in
				/*for _, key := range m.queueConfig.RoutingKey {
					err := channel.Publish(
						m.queueConfig.QueueName, // publish to an exchange
						key,   // routing to 0 or more queues
						false, // mandatory
						false, // immediate
						*m.amqpPublishing,
					)

					if err == amqp.ErrClosed {
						time.Sleep(5 * time.Second)
						continue RECONNECT
					}
				}*/
				err := channel.Publish(
					m.Exchange.ExchangeName, // publish to an exchange
					m.RoutingKey,            // routing to 0 or more queues
					false,                   // mandatory
					false,                   // immediate
					amqp.Publishing(m.AmqpPublishing),
				)

				if err == amqp.ErrClosed {
					time.Sleep(5 * time.Second)
					continue RECONNECT
				}
			}

			// normally quit , we quit too
			conn.Close()
			break
		}
	}

	for i := 0; i < SenderNum; i++ {
		wg.Add(1)
		go resender()
	}

	go func() {
		wg.Wait()
		log.Printf("all resender is done, close out")
		close(out)
	}()

	return out
}

func cloneToPublishMsg(msg *amqp.Delivery) *amqp.Publishing {
	newMsg := amqp.Publishing{
		Headers: msg.Headers,

		ContentType:     msg.ContentType,
		ContentEncoding: msg.ContentEncoding,
		DeliveryMode:    msg.DeliveryMode,
		Priority:        msg.Priority,
		CorrelationId:   msg.CorrelationId,
		ReplyTo:         msg.ReplyTo,
		Expiration:      msg.Expiration,
		MessageId:       msg.MessageId,
		Timestamp:       msg.Timestamp,
		Type:            msg.Type,
		UserId:          msg.UserId,
		AppId:           msg.AppId,

		Body: msg.Body,
	}

	return &newMsg
}

//***************************************
//***************************************
func (qc QueueConfig) WorkerQueueName() string {
	return qc.QueueName
}
func (qc QueueConfig) RetryQueueName() string {
	return fmt.Sprintf("%s-retry", qc.QueueName)
}
func (qc QueueConfig) ErrorQueueName() string {
	return fmt.Sprintf("%s-error", qc.QueueName)
}
func (qc QueueConfig) RetryExchangeName() string {
	return fmt.Sprintf("%s-retry", qc.QueueName)
}
func (qc QueueConfig) RequeueExchangeName() string {
	return fmt.Sprintf("%s-retry-requeue", qc.QueueName)
}
func (qc QueueConfig) ErrorExchangeName() string {
	return fmt.Sprintf("%s-error", qc.QueueName)
}
func (qc QueueConfig) WorkerExchangeName() string {
	/*if qc.BindingExchange == "" {
		return qc.project.QueuesDefaultConfig.BindingExchange
	}*/
	return qc.BindingExchange
}

func (qc QueueConfig) RetryDurationWithDefault() int {
	//if qc.RetryDuration == 0 {
	//	return qc.project.QueuesDefaultConfig.RetryDuration
	//}
	return qc.RetryDuration
}
