package server

import (
	"net"

	"github.com/google/uuid"
)

type Connection interface {
	ID() string
	Conn() net.Conn
}

type PublisherConnection struct {
	conn net.Conn
	id   string
}

func (p *PublisherConnection) ID() string {
	return p.id
}

func (p *PublisherConnection) Conn() net.Conn {
	return p.conn
}

func newPublisherConnection(conn net.Conn) *PublisherConnection {
	return &PublisherConnection{
		conn: conn,
		id:   uuid.New().String(),
	}
}

type SubscriberConnection struct {
	conn  net.Conn
	id    string
	topic string
}

func (s *SubscriberConnection) ID() string {
	return s.id
}

func (s *SubscriberConnection) Conn() net.Conn {
	return s.conn
}

func newSubscriberConnection(conn net.Conn, topic string) *SubscriberConnection {
	return &SubscriberConnection{
		conn:  conn,
		id:    uuid.New().String(),
		topic: topic,
	}
}
