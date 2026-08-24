package subscriber

import (
	"errors"
	"fmt"
	"log"
	"net"

	"trontria.com/gobroke/v2/internal/command"
	"trontria.com/gobroke/v2/internal/netter"
)

type Subscriber struct {
	params SubscriberParams
	conn   net.Conn
}

func (s *Subscriber) Stop() error {
	if s.conn != nil {
		err := s.conn.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Subscriber) handshakeWithServer() error {
	if s.conn == nil {
		return errors.New("Connection not established")
	}

	cmd := command.NewHandshakeCommand(command.Subscriber, s.params.Topic)
	err := command.WriteCommandAndWaitForAck(s.conn, cmd)

	return err
}

func (s *Subscriber) receive() (*command.BaseCommand, error) {
	if s.conn == nil {
		return nil, errors.New("Connection not established")
	}

	cmd, err := command.ReadCommand(s.conn)
	if err != nil {
		return nil, err
	}

	return cmd, nil
}

func (s *Subscriber) handlePublishCommand(cmd *command.BaseCommand) error {
	if len(cmd.Params) < 2 {
		return errors.New("Invalid publish command: missing parameters")
	}

	topic := cmd.Params[0]
	message := cmd.Params[1]

	log.Printf("Received message on topic %s: %s", topic, message)

	if topic != s.params.Topic {
		return fmt.Errorf("Receive invalid topic: expected %s, got %s", s.params.Topic, topic)
	}

	return nil
}
func (s *Subscriber) waitForMessage() error {
	for {
		log.Println("Waiting for message...")
		cmd, err := s.receive()
		if err != nil {
			return err
		}

		switch {
		case cmd.IsPublish():
			err := s.handlePublishCommand(cmd)
			err = command.WriteAckOrNack(s.conn, cmd, err)
			if err != nil {
				log.Printf("Failed to send ACK/NACK: %v", err)
			}
		default:
			log.Printf("Unexpected command received: %s", cmd.Command)
			nackCmd := command.NewNackCommand(cmd)
			err = command.WriteCommand(s.conn, nackCmd)
			if err != nil {
				log.Printf("Failed to send NACK: %v", err)
			}
		}
	}
}

func (s *Subscriber) Start() error {
	conn, err := netter.CreateClientConnection(s.params.Type)
	if err != nil {
		s.Stop()
		return err
	}
	s.conn = conn

	err = s.handshakeWithServer()
	if err != nil {
		s.Stop()
		return err
	}

	go s.waitForMessage()

	return nil
}

type SubscriberParams struct {
	Type    netter.ConnectionType
	Topic   string
	Handler func(topic string, message string)
}

func New(params SubscriberParams) *Subscriber {
	return &Subscriber{
		params: params,
	}
}
