package publisher

import (
	"fmt"
	"log"
	"net"

	"trontria.com/gobroke/v2/internal/command"
	"trontria.com/gobroke/v2/internal/netter"
)

type Publisher struct {
	params PublisherParams
	conn   net.Conn
}

func (p *Publisher) Stop() error {
	log.Println("Stopping publisher...")
	if p.conn != nil {
		err := p.conn.Close()
		if err != nil {
			log.Printf("Failed to close connection: %v", err)
			return err
		}
		log.Println("Publisher stopped.")
	}
	return nil
}

func (p *Publisher) handshakeWithServer() error {
	if p.conn == nil {
		return fmt.Errorf("Connection not provided")
	}

	cmd := command.NewHandshakeCommand(command.Publisher)
	err := command.WriteCommandAndWaitForAck(p.conn, cmd)

	return err
}

func (p *Publisher) Start() error {
	log.Println("Starting publisher...")
	conn, err := netter.CreateClientConnection(p.params.Type)
	if err != nil {
		p.Stop()
		return err
	}
	p.conn = conn

	log.Printf("Publisher connected on %s", conn.LocalAddr())

	err = p.handshakeWithServer()
	if err != nil {
		p.Stop()
		return err
	}
	log.Println("Handshake successful.")
	return nil
}

func (p *Publisher) Publish(topic string, message string) error {
	if p.conn == nil {
		return fmt.Errorf("Connection not provided")
	}

	cmd := command.NewPublishCommand(topic, message)
	err := command.WriteCommand(p.conn, cmd)
	if err != nil {
		log.Printf("Failed to send publish command: %v", err)
		return err
	}

	response, err := command.ReadCommand(p.conn)
	if err != nil {
		log.Printf("Failed to read publish response: %v", err)
		return err
	}

	switch {
	case response.IsAckOf(cmd):
		log.Printf("Publish command acknowledged for topic: %s", topic)
	case response.IsNackOf(cmd):
		return fmt.Errorf("publish command was not acknowledged for topic: %s", topic)
	default:
		return fmt.Errorf("unexpected response for publish command: %s", response.Command)
	}

	log.Printf("Published message: %s", message)
	return nil
}

type PublisherParams struct {
	Type netter.ConnectionType
}

func New(params PublisherParams) *Publisher {
	return &Publisher{params: params}
}
