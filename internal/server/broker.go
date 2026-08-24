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

	subscribersMutex sync.Mutex
	subscribers      []SubscriberConnection
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
		default:
			log.Printf("Unexpected command from publisher %s: %s", conn.RemoteAddr(), cmd.Command)
			command.WriteCommand(conn, command.NewNackCommand(cmd))
		}
	}
}

func (b *Broker) publishToSubscribers(command command.BaseCommand) {
	b.subscribersMutex.Lock()
	defer b.subscribersMutex.Unlock()

	for _, subscriber := range b.subscribers {
		go func() {
			log.Printf("Publishing to subscriber %s: %v", subscriber.conn.RemoteAddr(), command.Params)
		}()
	}
}

func (b *Broker) handleSubscriber(conn net.Conn) {
	// Placeholder for handling subscriber logic
	log.Printf("Handling subscriber connection from %s", conn.RemoteAddr())
}

func (b *Broker) handShakeWithClient(conn net.Conn) error {
	cmd, err := command.ReadCommand(conn)
	if err != nil {
		log.Printf("Failed to read command: %v", err)
		return err
	}

	if !cmd.IsHandshake() {
		log.Printf("Expected handshake command, got: %s", cmd.Command)
		return fmt.Errorf("expected handshake command, but got: %s", cmd.Command)
	}

	command.WriteCommand(conn, command.NewAckCommand(cmd))
	log.Printf("Received handshake command with params: %v", cmd.Params)

	if cmd.Params[0] == string(command.Publisher) {
		b.publishersMutex.Lock()
		b.publishers = append(b.publishers, PublisherConnection{conn: conn})
		b.publishersMutex.Unlock()
		log.Printf("Registered new publisher from %s", conn.RemoteAddr())
		go b.handlePublisher(conn)
	} else if cmd.Params[0] == string(command.Subscriber) {
		b.subscribersMutex.Lock()
		b.subscribers = append(b.subscribers, SubscriberConnection{conn: conn})
		b.subscribersMutex.Unlock()
		log.Printf("Registered new subscriber from %s", conn.RemoteAddr())
		go b.handleSubscriber(conn)
	} else {
		log.Printf("Unknown client type: %s", cmd.Params[0])
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

func (b *Broker) Stop() {
	if b.socket != nil {
		b.socket.Close()
	}
}

type BrokerParams struct {
	Type netter.ConnectionType
}

func New(params BrokerParams) *Broker {
	return &Broker{params: params}
}
