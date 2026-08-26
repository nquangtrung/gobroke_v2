package server

import (
	"sync"

	"trontria.com/gobroke/v2/internal/command"
)

type Topic struct {
	name        string
	mu          sync.Mutex
	subscribers []*SubscriberConnection
	messages    map[string][]command.PublishableCommand
}

func newTopic(name string) *Topic {
	return &Topic{
		name:        name,
		subscribers: []*SubscriberConnection{},
		messages:    make(map[string][]command.PublishableCommand),
	}
}

func (t *Topic) AddSubscriber(subscriber *SubscriberConnection) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.subscribers = append(t.subscribers, subscriber)
}

func (t *Topic) RemoveSubscriber(subscriber *SubscriberConnection) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for i, sub := range t.subscribers {
		if sub.conn == subscriber.conn {
			t.subscribers = append(t.subscribers[:i], t.subscribers[i+1:]...)
			break
		}
	}
}

func (t *Topic) Broadcast(publisher *PublisherConnection, cmd command.PublishableCommand) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.messages[publisher.id] = append(t.messages[publisher.id], cmd)
	for _, subscriber := range t.subscribers {
		msg := command.NewMessageCommandFromCommand(publisher.id, cmd)
		err := command.WriteCommandAndWaitForAck(subscriber.conn, msg)
		if err != nil {
			// TODO handle error, maybe remove subscriber if connection is broken
			continue
		}
	}
	return nil
}

func (t *Topic) GetSubscribers() []*SubscriberConnection {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]*SubscriberConnection(nil), t.subscribers...)
}

func (t *Topic) CloseAllSubscribers() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, subscriber := range t.subscribers {
		subscriber.conn.Close()
	}
	t.subscribers = nil
}
