package publisher

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

type Publisher struct {
	params PublisherParams
	conn   net.Conn

	pending command.PendingQueue
	retries utils.RetryMap[command.Command]
	buffer  utils.DroppableBuffer[command.Command]

	stopChan chan struct{}

	id string
}

func (p *Publisher) Stop() error {
	// This will stop the receiveLoop goroutine
	log.Println("Stopping publisher...")
	if p.conn != nil {
		err := p.conn.Close()
		if err != nil {
			log.Printf("Failed to close connection: %v", err)
			return err
		}
		log.Println("Publisher stopped.")
	}

	// This should automatically stop the publishLoop goroutine
	close(p.stopChan)

	// Drain the buffer and send any remaining commands
	p.buffer.Drain(func(cmd command.Command) {
		log.Printf("Draining command from buffer: %v", cmd)
		command.WriteCommand(p.conn, cmd)
	})

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
	log.Println("Starting publisher...", p.params)
	conn, err := netter.CreateClientConnection(netter.ConnectionParams{
		Type:       p.params.Type,
		Address:    p.params.Address,
		SocketPath: p.params.SocketPath,
	})
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

	go p.receiveLoop()
	go p.publishLoop()

	return nil
}

func (p *Publisher) receiveLoop() {
	for {
		cmds, err := command.ReadCommands(p.conn)
		switch {
		case errors.Is(err, net.ErrClosed):
			log.Println("Connection closed, stop receiving command.")
			return
		case errors.Is(err, io.EOF):
			log.Printf("Connection closed by server: %v", err)
			return
		case err != nil:
			log.Printf("Failed to read publish response: %v", err)
			continue
		}

		for _, cmd := range cmds {
			switch cmd := cmd.(type) {
			case *command.AckCommand:
				log.Printf("Received ACK command: %v", cmd)
				p.pending.GetAndRemoveCommandFromAck(cmd)
			case *command.NackCommand:
				log.Printf("Received NACK command: %v", cmd)
				rejectedCmd := p.pending.GetAndRemoveCommandFromAck(cmd)
				if rejectedCmd == nil {
					continue
				}
				log.Printf("Command %v was rejected by the server.", rejectedCmd)
				retries := p.retries.GetRetryCount(&rejectedCmd)
				if retries < p.params.MaxRetries {
					log.Printf("Retrying command %v (attempt %d)", rejectedCmd, retries+1)
					p.retries.AddRetry(&rejectedCmd)
					p.buffer.Add(rejectedCmd)
				} else {
					log.Printf("Max retries reached for command %v. Giving up.", rejectedCmd)
				}
			case *command.ConfigCommand:
				log.Printf("Received config command: %v", cmd)
				config, err := cmd.Config()
				if err != nil {
					log.Printf("Failed to parse config command: %v", err)
					continue
				}
				p.handleConfig(config)
			default:
				log.Printf("Received unexpected command: %v", cmd)
			}
		}
	}
}

func (p *Publisher) handleConfig(config map[string]any) {
	log.Printf("Received config: %v", config)
	if keepAlive, ok := config["keep_alive"].(time.Duration); ok {
		p.params.KeepAlive = keepAlive / 2
	}
	if id, ok := config["id"].(string); ok {
		p.id = id
	}
}

func (p *Publisher) publishLoop() {
	for {
		timeout := time.After(p.params.KeepAlive)
		select {
		case cmd := <-p.buffer.Channel():
			err := command.WriteCommand(p.conn, cmd)
			if err != nil {
				log.Printf("Failed to send publish command: %v", err)
			} else {
				log.Printf("Published message: %v", cmd)
				p.pending.AddCommand(cmd)
			}
		case <-timeout:
			log.Println("Keep-alive timeout reached, sending heartbeat.")
			heartbeatCmd := command.NewKeepAliveCommand()
			err := command.WriteCommand(p.conn, heartbeatCmd)
			if err != nil {
				log.Printf("Failed to send heartbeat command, server might have gone away: %v", err)
				p.Stop()
			} else {
				log.Println("Heartbeat sent successfully.")
			}
		case <-p.stopChan:
			log.Println("Stopping publish loop.")
			p.stopChan = nil
			return
		}
	}
}

func (p *Publisher) Publish(topic string, message string) error {
	p.buffer.Add(command.NewPublishCommand(topic, message))
	return nil
}

type PublisherParams struct {
	Type       netter.ConnectionType
	Address    string
	SocketPath string

	BufferSize int
	Timeout    time.Duration
	MaxRetries int
	Drop       utils.DropType
	KeepAlive  time.Duration
}

func New(params PublisherParams) *Publisher {
	resolvedParams := PublisherParams{
		Type:       params.Type,
		Address:    utils.Ternary(params.Address != "", params.Address, "localhost:7749"),
		SocketPath: utils.Ternary(params.SocketPath != "", params.SocketPath, "/tmp/gobroke.sock"),
		BufferSize: utils.Ternary(params.BufferSize == 0, 10, params.BufferSize),
		Timeout:    utils.Ternary(params.Timeout == 0, time.Second*1, params.Timeout),
		MaxRetries: utils.Ternary(params.MaxRetries == 0, 3, params.MaxRetries),
		Drop:       utils.Ternary(params.Drop == 0, utils.DropNewest, params.Drop),
		KeepAlive:  utils.Ternary(params.KeepAlive == 0, time.Second*30, params.KeepAlive),
	}

	return &Publisher{
		params: resolvedParams,
		buffer: *utils.NewDroppableBuffer[command.Command](
			resolvedParams.BufferSize,
			resolvedParams.Drop,
			resolvedParams.Timeout,
		),
		stopChan: make(chan struct{}),
	}
}
