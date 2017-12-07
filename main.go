package main

import (
	"time"

	"github.com/colinwdk/rabbitmq4g/rmqg"
	"github.com/streadway/amqp"
)

func main() {
	qg := rmqg.Rmqg{
		Url:         "amqp://test:test123@192.168.146.128:5672/",
		Exchanges:   make(map[string]*rmqg.ExchangeConfig, 5),
		Queues:      make(map[string]*rmqg.QueueConfig, 5),
		SendChannel: make(chan rmqg.MessageSend, 100),
	}

	queue1 := rmqg.QueueConfig{
		QueueName: "dy1",
	}
	queue2 := rmqg.QueueConfig{
		QueueName: "dy2",
	}
	qg.Queues["dy1"] = &queue1
	qg.Queues["dy2"] = &queue2

	//
	exchange := rmqg.ExchangeConfig{
		ExchangeName: "wdk",
		ExchangeType: "topic",
		BindQueues:   make(map[string][]*rmqg.QueueConfig, 5),
	}

	exchange.BindQueues["ai.*"] = []*rmqg.QueueConfig{
		&queue1,
		&queue2,
	}

	qg.Exchanges["wdk"] = &exchange

	_, channel, err := qg.SetupChannel()
	if err != nil {
	}

	qg.DeclareQueues(channel)
	qg.DeclareExchanges(channel)

	qg.SendMessages()

	m := rmqg.MessageSend{
		Exchange:   &exchange,
		RoutingKey: "ai.ai",
		AmqpPublishing: rmqg.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "application/json",
			ContentEncoding: "",
			Body:            []byte("AiAi"),
			DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
			Priority:        0,              // 0-9
			// a bunch of application/implementation-specific fields
		},
	}
	for {
		qg.SendChannel <- m
		time.Sleep(time.Second * 2)
	}

}
