package pubsub

import (
	"fmt"
	"sync"
)

type Subscribers map[string]*Subscriber

type Broker struct {
	subscribers Subscribers
	topics      map[string]Subscribers
	mut         sync.RWMutex
}

func NewBroker() *Broker {
	return &Broker{
		subscribers: Subscribers{},
		topics:      map[string]Subscribers{},
	}
}

func (b *Broker) Subscribe(s *Subscriber, topic string) {
	b.mut.Lock()
	defer b.mut.Unlock()

	if b.topics[topic] == nil {
		b.topics[topic] = Subscribers{}
	}
	s.AddTopic(topic)
	b.topics[topic][s.id] = s
	fmt.Printf("%s subscribed for topic: %s\n", s.id[0:5], topic)
}

func (b *Broker) Unsubscribe(subId string, topic string) {
	b.mut.RLock()
	defer b.mut.RUnlock()
	s := b.subscribers[subId]
	delete(b.topics[topic], subId)
	s.RemoveTopic(topic)
}

// / publishes the message to the topic
func (b *Broker) Publish(topic string, msg string) {
	b.mut.RLock()
	brokerTopics := b.topics[topic]
	b.mut.RUnlock()

	for _, s := range brokerTopics {
		m := &Message{Topic: topic, Body: msg}
		if !s.active {
			return
		}
		go (func(s *Subscriber) {
			s.Signal(m)
		})(s)
	}
}

// / boradcasts string to topics
func (b *Broker) Broadcast(msg string, topics []string) {
	for _, topic := range topics {
		for _, subscriber := range b.topics[topic] {
			m := Message{Body: msg, Topic: topic}
			go (func(s *Subscriber) {
				s.Signal(&m)
			})(subscriber)
		}
	}
}

func (b *Broker) AddSubscriber() *Subscriber {
	b.mut.Lock()
	defer b.mut.Unlock()
	id, subscriber := NewSubscriber()
	b.subscribers[id] = subscriber
	return subscriber
}

// / removes the subscriber from the broker then destructs it
func (b *Broker) RemoveSubscriber(subscriber *Subscriber) {
	for topic := range subscriber.topics {
		b.Unsubscribe(subscriber.id, topic)
	}

	b.mut.Lock()
	delete(b.subscribers, subscriber.id)
	b.mut.Unlock()
	subscriber.Desctruct()
}
