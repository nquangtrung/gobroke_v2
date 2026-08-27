package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"

	"github.com/google/uuid"
	"trontria.com/gobroke/v2/internal/command"
)

type SubscriberConnection struct {
	conn  net.Conn
	id    string
	topic string
	log   *log.Logger
}

func (s *SubscriberConnection) ID() string {
	return s.id
}

func (s *SubscriberConnection) Conn() net.Conn {
	return s.conn
}

func (s *SubscriberConnection) Loop(ctx context.Context, b *Broker) {
	conn := s.conn
	log := s.log
	log.Printf("Handling subscriber connection from %s", conn.RemoteAddr())
	for {
		log.Printf("Waiting for commands from subscriber %s", s.id)
		cmds, err := command.ReadCommands(conn, b.params.KeepAlive)
		var netErr net.Error
		switch {
		case errors.Is(err, net.ErrClosed):
			log.Printf("Connection closed broker %s", s.id)
			return
		case errors.Is(err, io.EOF):
			log.Printf("Connection closed by subscriber %s", s.id)
			return
		case errors.As(err, &netErr) && netErr.Timeout():
			log.Printf("Timeout reading from subscriber %s: %v", s.id, err)
			conn.Close()
			return
		case err != nil:
			log.Printf("Failed to read command from subscriber %s: %v", s.id, err)
			continue
		}

		for _, cmd := range cmds {
			switch cmd := cmd.(type) {
			case *command.KeepAliveCommand:
				log.Printf("Received keep-alive command from subscriber %s", s.id)
			case *command.AckCommand, *command.NackCommand:
				log.Printf("Received %s command from subscriber %s", cmd.Action(), s.id)
			case *command.UnsubscribeCommand:
				log.Printf("Received unsubscribe command from subscriber %s", s.id)
				b.unsubscribe(s)
				command.WriteCommand(conn, command.NewAckCommand(cmd))
				return
			default:
				log.Printf("Unexpected command from subscriber %s: %s", s.id, cmd.Action())
				command.WriteCommand(conn, command.NewNackCommand(cmd))
			}
		}
	}
}

func newSubscriberConnection(conn net.Conn, topic string) *SubscriberConnection {
	return &SubscriberConnection{
		conn:  conn,
		id:    uuid.New().String(),
		topic: topic,
		log:   log.New(log.Writer(), fmt.Sprintf("[Subscriber:%s] ", uuid.New().String()), log.LstdFlags),
	}
}
