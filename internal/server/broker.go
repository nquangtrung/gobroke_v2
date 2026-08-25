package server

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"trontria.com/gobroke/v2/internal/command"
	"trontria.com/gobroke/v2/internal/netter"
	"trontria.com/gobroke/v2/internal/utils"
)

type PublisherConnection struct {
	conn net.Conn
}

type SubscriberConnection struct {
	conn net.Conn
}

type Broker struct {
	params BrokerParams
	socket net.Listener

	publishersMutex sync.Mutex
	publishers      []PublisherConnection

	topicsMutex sync.Mutex
	topics      map[string]*Topic
}

func (b *Broker) publisherLoop(conn net.Conn) {
	// Placeholder for handling publisher logic
	log.Printf("Handling publisher connection from %s", conn.RemoteAddr())
	for {
		cmds, err := command.ReadCommands(conn, b.params.KeepAlive)
		var netErr net.Error

		switch {
		case errors.Is(err, net.ErrClosed):
			log.Printf("Connection closed broker %s", conn.RemoteAddr())
			return
		case errors.Is(err, io.EOF):
			log.Printf("Connection closed by publisher %s", conn.RemoteAddr())
			return
		case errors.As(err, &netErr) && netErr.Timeout():
			log.Printf("Timeout reading from publisher %s: %v", conn.RemoteAddr(), err)
			conn.Close()
			return
		case err != nil:
			log.Printf("Failed to read command from publisher %s: %v", conn.RemoteAddr(), err)
			continue
		}

		for _, cmd := range cmds {
			switch {
			case cmd.IsPublish():
				log.Printf("Received publish command from %s with params: %v", conn.RemoteAddr(), cmd.Params)
				command.WriteCommand(conn, command.NewAckCommand(cmd))
				b.publishToSubscribers(cmd)
			case cmd.IsKeepAlive():
				log.Printf("Received keep-alive command from publisher %s", conn.RemoteAddr())
			default:
				log.Printf("Unexpected command from publisher %s: %s", conn.RemoteAddr(), cmd.Command)
				command.WriteCommand(conn, command.NewNackCommand(cmd))
			}
		}
	}
}

func (b *Broker) publishToSubscribers(cmd *command.BaseCommand) error {
	b.topicsMutex.Lock()
	defer b.topicsMutex.Unlock()

	topicName := cmd.Params[0]
	topic, exists := b.topics[topicName]
	if !exists {
		log.Printf("No subscribers for topic %s", topicName)
		return fmt.Errorf("no subscribers for topic %s", topicName)
	}

	return topic.Broadcast(cmd)
}

func (b *Broker) handleSubscriber(conn net.Conn, cmd *command.BaseCommand) {
	b.topicsMutex.Lock()
	defer b.topicsMutex.Unlock()

	topicName := cmd.Params[1]
	topic, exists := b.topics[topicName]
	if !exists {
		topic = NewTopic(topicName)
		b.topics[topicName] = topic
	}

	subscriber := SubscriberConnection{conn: conn}
	topic.AddSubscriber(subscriber)
	log.Printf("Subscriber %s added to topic %s", conn.RemoteAddr(), topicName)

	b.subscriberLoop(conn)
}

func (b *Broker) subscriberLoop(conn net.Conn) {
	for {
		cmds, err := command.ReadCommands(conn, b.params.KeepAlive)
		var netErr net.Error
		switch {
		case errors.Is(err, net.ErrClosed):
			log.Printf("Connection closed broker %s", conn.RemoteAddr())
			return
		case errors.Is(err, io.EOF):
			log.Printf("Connection closed by subscriber %s", conn.RemoteAddr())
			return
		case errors.As(err, &netErr) && netErr.Timeout():
			log.Printf("Timeout reading from subscriber %s: %v", conn.RemoteAddr(), err)
			conn.Close()
			return
		case err != nil:
			log.Printf("Failed to read command from subscriber %s: %v", conn.RemoteAddr(), err)
			continue
		}

		for _, cmd := range cmds {
			switch {
			case cmd.IsKeepAlive():
				log.Printf("Received keep-alive command from subscriber %s", conn.RemoteAddr())
			default:
				log.Printf("Unexpected command from subscriber %s: %s", conn.RemoteAddr(), cmd.Command)
				command.WriteCommand(conn, command.NewNackCommand(cmd))
			}
		}
	}
}

func (b *Broker) handShakeWithClient(conn net.Conn) error {
	cmds, err := command.ReadCommands(conn, time.Second*5)
	if err != nil {
		log.Printf("Failed to read command: %v", err)
		return err
	}

	cmd := cmds[0]
	switch {
	case !cmd.IsHandshake():
		log.Printf("Expected handshake command, got: %s", cmd.Command)
		command.WriteCommand(conn, command.NewNackCommand(cmd))
		return fmt.Errorf("expected handshake command, but got: %s", cmd.Command)
	case cmd.Params[0] == string(command.Publisher):
		b.publishersMutex.Lock()
		b.publishers = append(b.publishers, PublisherConnection{conn: conn})
		b.publishersMutex.Unlock()
		log.Printf("Registered new publisher from %s", conn.RemoteAddr())
		go b.publisherLoop(conn)
		command.WriteCommand(conn, command.NewAckCommand(cmd))
	case cmd.Params[0] == string(command.Subscriber):
		log.Printf("Registered new subscriber from %s", conn.RemoteAddr())
		go b.handleSubscriber(conn, cmd)
		command.WriteCommand(conn, command.NewAckCommand(cmd))
	default:
		log.Printf("Unknown client type: %s", cmd.Params[0])
		command.WriteCommand(conn, command.NewNackCommand(cmd))
		return fmt.Errorf("unknown client type: %s", cmd.Params[0])
	}

	return nil
}

func (b *Broker) handleConnection(conn net.Conn) {
	log.Printf("Accepted connection from %s", conn.RemoteAddr())

	err := b.handShakeWithClient(conn)
	if err != nil {
		log.Printf("Handshake failed: %v", err)
		return
	}
	log.Printf("Handshake successful with %s", conn.RemoteAddr())
}

func (b *Broker) Start() {
	defer func() {
		log.Println("Accept loop stopped.")
	}()
	log.Println("Starting server...")
	socket, err := netter.CreateServerSocket(netter.ConnectionParams{
		Type:       b.params.Type,
		Address:    b.params.Address,
		SocketPath: b.params.SocketPath,
	})
	b.socket = socket
	if err != nil {
		log.Fatalf("Failed to create socket: %v", err)
	}

	log.Printf("Server listening on %s", socket.Addr())
	for {
		conn, err := socket.Accept()
		switch {
		case errors.Is(err, net.ErrClosed):
			log.Println("Server socket closed, stopping accept loop.")
			return
		case err != nil:
			log.Printf("Failed to accept connection: %v", err)
			continue
		}

		go b.handleConnection(conn)
	}
}

func (b *Broker) stopPublishers() {
	b.publishersMutex.Lock()
	defer b.publishersMutex.Unlock()

	for _, publisher := range b.publishers {
		err := publisher.conn.Close()
		if err != nil {
			log.Printf("Failed to close publisher connection: %v", err)
		} else {
			log.Printf("Closed publisher connection: %s", publisher.conn.RemoteAddr())
		}
	}
	b.publishers = nil
}

func (b *Broker) stopSubscribers() {
	b.topicsMutex.Lock()
	defer b.topicsMutex.Unlock()

	for _, topic := range b.topics {
		topic.CloseAllSubscribers()
	}
	b.topics = make(map[string]*Topic)
}

func (b *Broker) Stop() error {
	log.Println("Stopping broker...")
	b.stopPublishers()
	b.stopSubscribers()
	if b.socket != nil {
		err := b.socket.Close()
		if err != nil {
			log.Printf("Failed to close connection: %v", err)
			return err
		}
		log.Println("Broker stopped.")
	}
	return nil
}

type BrokerParams struct {
	Type       netter.ConnectionType
	Address    string
	SocketPath string
	KeepAlive  time.Duration
}

func New(params BrokerParams) *Broker {
	resolvedParams := BrokerParams{
		Type:       params.Type,
		Address:    utils.Ternary(params.Address == "", "localhost:7749", params.Address),
		SocketPath: utils.Ternary(params.SocketPath == "", "/tmp/gobroke.sock", params.SocketPath),
		KeepAlive:  utils.Ternary(params.KeepAlive == 0, time.Second*30, params.KeepAlive),
	}
	return &Broker{
		params: resolvedParams,
		topics: make(map[string]*Topic),
	}
}
