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

type PublisherConnection struct {
	conn net.Conn
	id   string
	log  *log.Logger
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
		log:  log.New(log.Writer(), fmt.Sprintf("[Publisher:%s] ", uuid.New().String()), log.LstdFlags),
	}
}

func (p *PublisherConnection) Loop(ctx context.Context, b *Broker) {
	conn := p.conn
	log := log.New(log.Writer(), fmt.Sprintf("[Publisher:%s] ", p.id), log.LstdFlags)
	log.Printf("Handling publisher connection from %s", p.id)
	for {
		cmds, err := command.ReadCommands(conn, b.params.KeepAlive)
		var netErr net.Error

		switch {
		case errors.Is(err, net.ErrClosed):
			log.Printf("Connection closed by broker")
			return
		case errors.Is(err, io.EOF):
			log.Printf("Connection closed by publisher %s", p.id)
			b.removePublisher(p)
			return
		case errors.As(err, &netErr) && netErr.Timeout():
			log.Printf("Timeout reading from publisher %s: %v", p.id, err)
			conn.Close()
			b.removePublisher(p)
			return
		case err != nil:
			log.Printf("Failed to read command from publisher %s: %v", p.id, err)
			continue
		}

		for _, cmd := range cmds {
			switch cmd := cmd.(type) {
			case *command.PublishCommand:
				log.Printf("Received publish command from %s with params: %v", p.id, cmd.Params())
				go command.WriteCommand(conn, command.NewAckCommand(cmd))
				go b.publishToSubscribers(p, cmd)
			case *command.AckCommand, *command.NackCommand:
				log.Printf("Received %s command from publisher %s", cmd.Action(), p.id)
			case *command.KeepAliveCommand:
				log.Printf("Received keep-alive command from publisher %s", p.id)
			default:
				log.Printf("Unexpected command from publisher %s: %s", p.id, cmd.Action())
				command.WriteCommand(conn, command.NewNackCommand(cmd))
			}
		}
	}
}
