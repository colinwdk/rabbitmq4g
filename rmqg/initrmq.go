package rmqg

import (
	"github.com/streadway/amqp"
	"log"
	"fmt"
	"sync"
	"github.com/satori/go.uuid"
	"time"
)

const (
	// chan
	ChannelBufferLength = 100

	//worker number
	ReceiverNum = 5
)

type Rmqg struct{
	url string//addr url

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

//*****************************

func (this *Rmqg)InitRmqp() {

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

func (qc *QueueConfig) DeclareExchange(channel *amqp.Channel) {
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

func (qc *QueueConfig) DeclareQueue(channel *amqp.Channel) {
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
func (this *Rmqg)setupChannel() (*amqp.Connection, *amqp.Channel, error) {
	//url := os.Getenv("AMQP_URL")

	conn, err := amqp.Dial(this.url)
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

func (this *Rmqg)ReceiveMessage(queues []*QueueConfig, done <-chan struct{}) <-chan Message {
	out := make(chan Message, ChannelBufferLength)
	var wg sync.WaitGroup

	receiver := func(qc QueueConfig) {
		defer wg.Done()

	RECONNECT:
		for {
			_, channel, err := this.setupChannel()
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