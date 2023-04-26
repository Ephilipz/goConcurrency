package pubsub

import (
	"fmt"
	"sync"

	"github.com/google/uuid"
)

type Subscriber struct {
	id       string
	messages chan *Message
	topics   map[string]struct{}
	active   bool
	mutex    sync.RWMutex
}

func NewSubscriber() (string, *Subscriber) {
	id := uuid.New().String()
	return id, &Subscriber{
		id:       id,
		messages: make(chan *Message),
		topics:   map[string]struct{}{},
		active:   true,
	}
}

func (s *Subscriber) AddTopic(topic string) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	s.topics[topic] = struct{}{}
}

func (s *Subscriber) GetTopics() []string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	topics := []string{}
	for topic := range s.topics {
		topics = append(topics, topic)
	}
	return topics
}

func (s *Subscriber) RemoveTopic(topic string) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	delete(s.topics, topic)
}

func (s *Subscriber) Signal(msg *Message) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	if s.active {
		s.messages <- msg
	}
}

// / listens to subscriber messages and prints them
func (s *Subscriber) Listen() {
	for {
		if msg, ok := <-s.messages; ok {
			fmt.Printf("Subscriber %s, received %s, from topic %s\n", s.id[0:5], msg.Body, msg.Topic)
		}
	}
}

func (s *Subscriber) Desctruct() {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	s.active = false
	close(s.messages)
}
