package rmqg

import "fmt"

func (this *Rmqg) InitRmqp() {
	qg := Rmqg{
		Url:        "amqp://test:test123@192.168.146.128:5672/",
		Exchanges:  make(map[string]*ExchangeConfig, 5),
		SendQueues: make(map[string]*QueueConfig, 5),
		//SendChannel: make(chan rmqg.MessageSend, 100),
		RecvQueues: make(map[string]*QueueConfig, 5),

		SenderCount:1,
		RecverCount:1,
	}

	//********Declare queue
	queue1 := QueueConfig{
		QueueName: "dy1",
	}
	queue2 := QueueConfig{
		QueueName: "dy2",
	}
	qg.SendQueues[queue1.QueueName] = &queue1
	qg.SendQueues[queue2.QueueName] = &queue2

	qg.RecvQueues[queue1.QueueName] = &queue1
	qg.RecvQueues[queue2.QueueName] = &queue2

	//********Declare exchange
	exchange := ExchangeConfig{
		ExchangeName: "wdk",
		ExchangeType: "topic",
		BindQueues:   make(map[string][]*QueueConfig, 5),
	}

	//********Declare bind
	exchange.BindQueues["ai.*"] = []*QueueConfig{
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
	//***************************************

	qg.SendMessages()
	done := make(chan struct{}, 1)
	qg.RecvMessages(done)
	//***************************************

}