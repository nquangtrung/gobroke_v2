package subscriber

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"trontria.com/gobroke/v2/internal/command"
	"trontria.com/gobroke/v2/internal/netter"
	"trontria.com/gobroke/v2/internal/utils"
)

type Subscriber struct {
	params SubscriberParams
	conn   net.Conn
	worker *utils.Worker
	id     string
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

func (s *Subscriber) handleMessageCommand(cmd *command.MessageCommand) error {
	from := cmd.From()
	topic := cmd.Topic()
	message := cmd.Message()

	log.Printf("Received message on topic %s: %s, from %s", topic, message, from)

	if topic != s.params.Topic {
		return fmt.Errorf("Receive invalid topic: expected %s, got %s", s.params.Topic, topic)
	}

	s.worker.Do(func() {
		s.params.Handler(topic, message)
	})

	return nil
}
func (s *Subscriber) receiveLoop() {
	for {
		log.Println("Waiting for message...")
		cmds, err := command.ReadCommands(s.conn, s.params.KeepAlive)
		var netErr net.Error
		switch {
		case errors.Is(err, io.EOF):
			log.Println("Connection closed by server.")
			return
		case errors.Is(err, net.ErrClosed):
			log.Println("Connection closed, stop receiving command.")
			return
		case errors.As(err, &netErr) && err.(net.Error).Timeout():
			err := command.WriteCommand(s.conn, command.NewKeepAliveCommand())
			if err != nil {
				log.Printf("Failed to send keep-alive command: %v, the server might have gone away", err)
				return
			}
			continue
		case err != nil:
			log.Printf("Failed to receive command: %v", err)
			continue
		}

		for _, cmd := range cmds {
			switch cmd := cmd.(type) {
			case *command.MessageCommand:
				err := s.handleMessageCommand(cmd)
				err = command.WriteAckOrNack(s.conn, cmd, err)
				if err != nil {
					log.Printf("Failed to send ACK/NACK: %v", err)
				}
			case *command.AckCommand, *command.NackCommand:
				log.Printf("Received %s command from server: %s", cmd.Action(), cmd.String())
			case *command.ConfigCommand:
				log.Printf("Received config command from server: %s", cmd.String())
				config, _ := cmd.Config()
				s.handleConfig(config)
			default:
				log.Printf("Unexpected command received: %s", cmd.Action())
				nackCmd := command.NewNackCommand(cmd)
				err = command.WriteCommand(s.conn, nackCmd)
				if err != nil {
					log.Printf("Failed to send NACK: %v", err)
				}
			}
		}
	}
}

func (s *Subscriber) handleConfig(config map[string]any) {
	log.Printf("Received config: %v", config)
	if keepAlive, ok := config["keep_alive"].(time.Duration); ok {
		s.params.KeepAlive = keepAlive / 2
	}
	if id, ok := config["id"].(string); ok {
		s.id = id
	}
}

func (s *Subscriber) Start() error {
	conn, err := netter.CreateClientConnection(netter.ConnectionParams{
		Type:       s.params.Type,
		Address:    s.params.Address,
		SocketPath: s.params.SocketPath,
	})
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

	go s.receiveLoop()

	return nil
}

type SubscriberParams struct {
	Type       netter.ConnectionType
	Address    string
	SocketPath string

	Topic   string
	Handler func(topic string, message string)

	MaxWorker int
	KeepAlive time.Duration
}

func New(params SubscriberParams) *Subscriber {
	resolvedParams := SubscriberParams{
		Type:       params.Type,
		Address:    params.Address,
		SocketPath: params.SocketPath,
		Topic:      params.Topic,
		Handler:    params.Handler,
		MaxWorker:  utils.Ternary(params.MaxWorker <= 0, 1, params.MaxWorker),
		KeepAlive:  utils.Ternary(params.KeepAlive <= 0, 30*time.Second, params.KeepAlive),
	}
	return &Subscriber{
		params: resolvedParams,
		worker: utils.NewWorker(resolvedParams.MaxWorker),
	}
}
