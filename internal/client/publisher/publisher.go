package publisher

import (
	"errors"
	"fmt"
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

	awaitingAck command.CommandQueue
	retries     utils.RetryMap[command.BaseCommand]
	buffer      utils.DroppableBuffer[*command.BaseCommand]

	stopChan chan struct{}
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
	p.buffer.Drain(func(cmd *command.BaseCommand) {
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

	go p.receiveLoop()
	go p.publishLoop()
	return nil
}

func (p *Publisher) receiveLoop() {
	for {
		cmds, err := command.ReadCommands(p.conn)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				log.Println("Connection closed, stop receiving command.")
				return
			} else {
				log.Printf("Failed to read publish response: %v", err)
			}
		}

		for _, cmd := range cmds {
			switch {
			case cmd.IsAck():
				log.Printf("Received ACK command: %v", cmd)
				p.awaitingAck.GetAndRemoveCommandFromAck(cmd)
			case cmd.IsNack():
				log.Printf("Received NACK command: %v", cmd)
				rejectedCmd := p.awaitingAck.GetAndRemoveCommandFromAck(cmd)
				if rejectedCmd == nil {
					continue
				}
				log.Printf("Command %v was rejected by the server.", rejectedCmd)
				retries := p.retries.GetRetryCount(rejectedCmd)
				if retries < p.params.MaxRetries {
					log.Printf("Retrying command %v (attempt %d)", rejectedCmd, retries+1)
					p.retries.AddRetry(rejectedCmd)
					p.buffer.Add(rejectedCmd)
				} else {
					log.Printf("Max retries reached for command %v. Giving up.", rejectedCmd)
				}
			default:
				log.Printf("Received unexpected command: %v", cmd)
			}
		}
	}
}

func (p *Publisher) publishLoop() {
	for {
		select {
		case cmd := <-p.buffer.Channel():
			err := command.WriteCommand(p.conn, cmd)
			if err != nil {
				log.Printf("Failed to send publish command: %v", err)
			} else {
				log.Printf("Published message: %v", cmd)
				p.awaitingAck.AddCommand(cmd)
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
	BufferSize int
	Timeout    time.Duration
	MaxRetries int
	Drop       utils.DropType
}

func New(params PublisherParams) *Publisher {
	resolvedParams := PublisherParams{
		Type:       params.Type,
		BufferSize: utils.Ternary(params.BufferSize == 0, 10, params.BufferSize),
		Timeout:    utils.Ternary(params.Timeout == 0, time.Second*1, params.Timeout),
		MaxRetries: utils.Ternary(params.MaxRetries == 0, 3, params.MaxRetries),
		Drop:       utils.Ternary(params.Drop == 0, utils.DropNewest, params.Drop),
	}

	return &Publisher{
		params: resolvedParams,
		buffer: *utils.NewDroppableBuffer[*command.BaseCommand](
			resolvedParams.BufferSize,
			resolvedParams.Drop,
			resolvedParams.Timeout,
		),
		stopChan: make(chan struct{}),
	}
}
