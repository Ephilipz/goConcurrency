package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/Ephilipz/goConcurrency/pubsub/pubsub"
)

var topics = map[string]string{
	"BTC": "BITCOIN",
	"ETH": "ETHEREUM",
	"DOT": "POLKADOT",
	"SOL": "SOLANA",
}

// / publishes to random topics
func publishPrices(broker *pubsub.Broker) {
	topicIds := make([]string, 0, len(topics))
	topicValues := make([]string, 0, len(topics))
	for k, v := range topics {
		topicIds = append(topicIds, k)
		topicValues = append(topicValues, v)
	}
	for {
		randomTopic := topicValues[rand.Intn(len(topics))]
		msg := fmt.Sprintf("%f", rand.Float64())
		go broker.Publish(randomTopic, msg)
		randomDuration := time.Duration(rand.Intn(4)) * time.Second
		time.Sleep(randomDuration)
	}
}

func main() {
	doneSignal := make(chan struct{})
	go func() {
		time.Sleep(time.Second * 30)
		close(doneSignal)
	}()

	broker := pubsub.NewBroker()
	sub1 := broker.AddSubscriber()
	broker.Subscribe(sub1, topics["BTC"])
	broker.Subscribe(sub1, topics["ETH"])

	sub2 := broker.AddSubscriber()
	broker.Subscribe(sub2, topics["ETH"])
	broker.Subscribe(sub2, topics["SOL"])

	go publishPrices(broker)
	go sub1.Listen()
	go sub2.Listen()

	<-doneSignal
	fmt.Println("Done!")
}
