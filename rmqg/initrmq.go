package rmqg

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/satori/go.uuid"
	"github.com/streadway/amqp"
	"runtime"
)

const (
	// chan
	ChannelBufferLength = 100

	//worker number
	ReceiverNum = 1
	SenderNum   = 1
)

type Rmqg struct {
	Url         string //addr url
	Exchanges   map[string]*ExchangeConfig
	SendQueues  map[string]*QueueConfig
	SendChannel chan MessageSend

	RecvQueues  map[string]*QueueConfig
	RecvChannel chan MessageRecv
}

type ExchangeConfig struct {
	ExchangeName string
	ExchangeType string

	//routingkeyQueues [][]interface{} // routingKey + *QueueConfig
	//routingKey []string
	BindQueues map[string][]*QueueConfig //string=routing_key
}

type QueueConfig struct {
	QueueName  string
	RoutingKey []string
	//NotifyPath      string
	//NotifyTimeout   int
	//RetryTimes      int
	//RetryDuration   int
	//BindingExchange string

	//project *ProjectConfig
}

type NotifyResponse int
type Message struct {
	QueueConfig  *QueueConfig
	AmqpDelivery *amqp.Delivery // message read from rabbitmq
	//notifyResponse NotifyResponse // notify result from callback url
}

type MessageRecv struct {
	QueueConfig  *QueueConfig
	AmqpDelivery *amqp.Delivery // message read from rabbitmq
	//notifyResponse NotifyResponse // notify result from callback url
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

func PrintPanicStack() {
	if x := recover(); x != nil {
		log.Printf("%v", x)
		i := 0
		funcName, file, line, ok := runtime.Caller(i)
		for ok {
			log.Printf("frame %v:[func:%v,file:%v,line:%v]\n", i, runtime.FuncForPC(funcName).Name(), file, line)
			i++
			funcName, file, line, ok = runtime.Caller(i)
		}
	}
}

//*******************
//*******************rmqg
func (this *Rmqg) DeclareQueues(channel *amqp.Channel, queues map[string]*QueueConfig) {
	var err error
	for _, q := range queues { //this.SendQueues
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

func (this *Rmqg) RecvMessages(done <-chan struct{}) { //queues []*QueueConfig, // <-chan Message
	this.RecvChannel = make(chan MessageRecv, ChannelBufferLength)
	var wg sync.WaitGroup

	receiver := func(qc *QueueConfig) {
		defer wg.Done()
		defer PrintPanicStack()

	RECONNECT:
		for {
			_, channel, err := this.SetupChannel()
			if err != nil {
				//PanicOnError(err)
				time.Sleep(5 * time.Second)
				continue
			}

			msgs, err := channel.Consume(
				qc.QueueName, //qc.WorkerQueueName(), // queue
				"",           // consumer
				false,        // auto-ack
				false,        // exclusive
				false,        // no-local
				false,        // no-wait
				nil,          // args
			)
			if err != nil {
				///PanicOnError(err)
				time.Sleep(5 * time.Second)
				continue
			}

			for {
				select {
				case msg, ok := <-msgs:
					if !ok {
						log.Printf("receiver: channel is closed, maybe lost connection")
						time.Sleep(5 * time.Second)
						continue RECONNECT
					}
					msg.MessageId = fmt.Sprintf("%s", uuid.NewV4())
					message := MessageRecv{qc, &msg} //, 0
					//out <- message
					this.RecvChannel <- message
					msg.Ack(false)

				case <-done:
					log.Printf("receiver: received a done signal")
					return
				}
			}
		}
	}

	for _, queue := range this.RecvQueues { //queues
		wg.Add(ReceiverNum)
		for i := 0; i < ReceiverNum; i++ {
			go receiver(queue)
		}
	}

	go func() {
		wg.Wait()
		log.Printf("all receiver is done, closing channel")
		//close(out)
		close(this.RecvChannel)
	}()

}

func (this *Rmqg) SendMessages() { //in <-chan MessageSend //<-chan Message
	this.SendChannel = make(chan MessageSend, ChannelBufferLength) //declare send channel

	var wg sync.WaitGroup
	resender := func() {
		defer wg.Done()
		defer PrintPanicStack()

	RECONNECT:
		for {
			conn, channel, err := this.SetupChannel()
			if err != nil {
				//PanicOnError(err)
				time.Sleep(5 * time.Second)
				continue
			}

			for m := range this.SendChannel { //in
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
		log.Printf("all sender is done, close out")
		close(this.SendChannel)
	}()

	//return out
}
