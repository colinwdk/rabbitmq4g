package main

import (
	"time"

	"fmt"

	"github.com/colinwdk/rabbitmq4g/rmqg"
	"github.com/streadway/amqp"
)

func main() {
	qg := rmqg.Rmqg{
		Url:        "amqp://test:test123@192.168.146.128:5672/",
		Exchanges:  make(map[string]*rmqg.ExchangeConfig, 5),
		SendQueues: make(map[string]*rmqg.QueueConfig, 5),
		//SendChannel: make(chan rmqg.MessageSend, 100),
		RecvQueues: make(map[string]*rmqg.QueueConfig, 5),
	}

	//********Declare queue
	queue1 := rmqg.QueueConfig{
		QueueName: "dy1",
	}
	queue2 := rmqg.QueueConfig{
		QueueName: "dy2",
	}
	qg.SendQueues["dy1"] = &queue1
	qg.SendQueues["dy2"] = &queue2

	qg.RecvQueues["dy1"] = &queue1
	qg.RecvQueues["dy2"] = &queue2

	//********Declare exchange
	exchange := rmqg.ExchangeConfig{
		ExchangeName: "wdk",
		ExchangeType: "topic",
		BindQueues:   make(map[string][]*rmqg.QueueConfig, 5),
	}

	//********Declare bind
	exchange.BindQueues["ai.*"] = []*rmqg.QueueConfig{
		&queue1,
		&queue2,
	}
	qg.Exchanges["wdk"] = &exchange

	//***************************************
	_, channel, err := qg.SetupChannel()
	if err != nil {
		fmt.Println("err:", err)
	}

	qg.DeclareQueues(channel, qg.SendQueues)
	qg.DeclareExchanges(channel)
	qg.SendMessages()

	done := make(chan struct{}, 1)
	qg.RecvMessages(done)
	//***************************************

	m := rmqg.MessageSend{
		Exchange:   &exchange,
		RoutingKey: "ai.ai",
		AmqpPublishing: rmqg.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "application/json",
			ContentEncoding: "",
			Body:            []byte("AiAi1" + time.Now().String()),
			DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
			Priority:        0,              // 0-9
			// a bunch of application/implementation-specific fields
		},
	}
	go func() {
		for {
			m.AmqpPublishing.Body = []byte("AiAi1" + time.Now().String())
			qg.SendChannel <- m
			time.Sleep(time.Millisecond * 500)
		}
	}()

	go func() {
		for {
			info, ok := <-qg.RecvChannel
			if ok {
				fmt.Println("rrrrrrrrrrrrrrrrrr:", info.QueueConfig.QueueName, string(info.AmqpDelivery.Body))
			} else {
				fmt.Println("nnnnnnnnnnnnnnnnnnnnnnnnn")
				time.Sleep(time.Second * 1)
			}
		}
	}()

	time.Sleep(time.Second * 10)
	close(done)
	time.Sleep(time.Hour * 20)

}
