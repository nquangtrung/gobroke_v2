package server

import (
	"errors"
	"fmt"
	"log"
	"net"
	"sync"

	netHelper "trontria.com/gobroke/v2/internal/net"
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
}

func (b *Broker) handleSubscriber(conn net.Conn) {
	// Placeholder for handling subscriber logic
	log.Printf("Handling subscriber connection from %s", conn.RemoteAddr())
}

func (b *Broker) handShakeWithClient(conn net.Conn) error {
	command, err := netHelper.ReadCommand(conn)
	if err != nil {
		log.Printf("Failed to read command: %v", err)
		return err
	}

	if !command.IsHandshake() {
		log.Printf("Expected handshake command, got: %s", command.Command)
		return fmt.Errorf("expected handshake command, but got: %s", command.Command)
	}

	netHelper.WriteCommand(conn, netHelper.NewAckCommand(command))
	log.Printf("Received handshake command with params: %v", command.Params)

	if command.Params[0] == string(netHelper.Publisher) {
		b.publishersMutex.Lock()
		b.publishers = append(b.publishers, PublisherConnection{conn: conn})
		b.publishersMutex.Unlock()
		log.Printf("Registered new publisher from %s", conn.RemoteAddr())
		go b.handlePublisher(conn)
	} else if command.Params[0] == string(netHelper.Subscriber) {
		b.subscribersMutex.Lock()
		b.subscribers = append(b.subscribers, SubscriberConnection{conn: conn})
		b.subscribersMutex.Unlock()
		log.Printf("Registered new subscriber from %s", conn.RemoteAddr())
		go b.handleSubscriber(conn)
	} else {
		log.Printf("Unknown client type: %s", command.Params[0])
		return fmt.Errorf("unknown client type: %s", command.Params[0])
	}
	return nil
}

func (b *Broker) handleConnection(conn net.Conn) {
	defer func() {
		conn.Close()
		log.Printf("Closed connection from %s", conn.RemoteAddr())
	}()

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
	socket, err := netHelper.CreateServerSocket(b.params.Type)
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
	Type netHelper.ConnectionType
}

func New(params BrokerParams) *Broker {
	return &Broker{params: params}
}
