package server

import (
	"errors"
	"fmt"
	"log"
	"net"
	"sync"

	"trontria.com/gobroke/v2/internal/command"
	"trontria.com/gobroke/v2/internal/netter"
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

func (b *Broker) handlePublisher(conn net.Conn) {
	// Placeholder for handling publisher logic
	log.Printf("Handling publisher connection from %s", conn.RemoteAddr())
	for {
		cmd, err := command.ReadCommand(conn)
		if err != nil {
			log.Printf("Error reading command from publisher %s: %v", conn.RemoteAddr(), err)
			return
		}

		switch {
		case cmd.IsPublish():
			log.Printf("Received publish command from %s with params: %v", conn.RemoteAddr(), cmd.Params)
			command.WriteCommand(conn, command.NewAckCommand(cmd))
			b.publishToSubscribers(cmd)
		default:
			log.Printf("Unexpected command from publisher %s: %s", conn.RemoteAddr(), cmd.Command)
			command.WriteCommand(conn, command.NewNackCommand(cmd))
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
}

func (b *Broker) handShakeWithClient(conn net.Conn) error {
	cmd, err := command.ReadCommand(conn)
	if err != nil {
		log.Printf("Failed to read command: %v", err)
		return err
	}

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
		go b.handlePublisher(conn)
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
	log.Println("Starting server...")
	socket, err := netter.CreateServerSocket(b.params.Type)
	b.socket = socket
	if err != nil {
		log.Fatalf("Failed to create socket: %v", err)
	}

	defer socket.Close()
	log.Printf("Server listening on %s", socket.Addr())
	for {
		conn, err := socket.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			if errors.Is(err, net.ErrClosed) {
				break
			} else {
				continue
			}
		}
		go b.handleConnection(conn)
	}
}

func (b *Broker) Stop() error {
	log.Println("Stopping broker...")
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
	Type netter.ConnectionType
}

func New(params BrokerParams) *Broker {
	return &Broker{
		params: params,
		topics: make(map[string]*Topic),
	}
}
