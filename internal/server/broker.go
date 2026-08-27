package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
	"trontria.com/gobroke/v2/internal/command"
	"trontria.com/gobroke/v2/internal/netter"
	"trontria.com/gobroke/v2/internal/utils"
)

type Broker struct {
	params BrokerParams
	socket net.Listener

	publishersMutex sync.Mutex
	publishers      []*PublisherConnection

	topicsMutex sync.Mutex
	topics      map[string]*Topic

	wg sync.WaitGroup
}

func (b *Broker) publisherLoop(_ context.Context, publisher *PublisherConnection) {
	conn := publisher.conn
	log := log.New(log.Writer(), fmt.Sprintf("[Publisher:%s] ", publisher.id), log.LstdFlags)
	log.Printf("Handling publisher connection from %s", publisher.id)
	for {
		cmds, err := command.ReadCommands(conn, b.params.KeepAlive)
		var netErr net.Error

		switch {
		case errors.Is(err, net.ErrClosed):
			log.Printf("Connection closed broker %s", conn.RemoteAddr())
			return
		case errors.Is(err, io.EOF):
			log.Printf("Connection closed by publisher %s", publisher.id)
			return
		case errors.As(err, &netErr) && netErr.Timeout():
			log.Printf("Timeout reading from publisher %s: %v", publisher.id, err)
			conn.Close()
			return
		case err != nil:
			log.Printf("Failed to read command from publisher %s: %v", publisher.id, err)
			continue
		}

		for _, cmd := range cmds {
			switch {
			case cmd.IsPublish():
				log.Printf("Received publish command from %s with params: %v", publisher.id, cmd.Params())
				go command.WriteCommand(conn, command.NewAckCommand(cmd))
				go b.publishToSubscribers(publisher, cmd.(command.PublishableCommand))
			case cmd.IsAck() || cmd.IsNack():
				log.Printf("Received %s command from publisher %s", cmd.Action(), publisher.id)
			case cmd.IsKeepAlive():
				log.Printf("Received keep-alive command from publisher %s", publisher.id)
			default:
				log.Printf("Unexpected command from publisher %s: %s", publisher.id, cmd.Action())
				command.WriteCommand(conn, command.NewNackCommand(cmd))
			}
		}
	}
}

func (b *Broker) publishToSubscribers(publisher *PublisherConnection, cmd command.PublishableCommand) {
	b.topicsMutex.Lock()
	defer b.topicsMutex.Unlock()

	topicName := cmd.Params()[0]
	topic, exists := b.topics[topicName]
	if !exists {
		log.Printf("No subscribers for topic %s", topicName)
		return
	}

	go topic.Broadcast(publisher, cmd)
}

func (b *Broker) addSubscriber(conn net.Conn, cmd *command.HandshakeCommand) *SubscriberConnection {
	b.topicsMutex.Lock()
	defer b.topicsMutex.Unlock()

	topicName := cmd.Topic()
	topic, exists := b.topics[topicName]
	if !exists {
		topic = newTopic(topicName)
		b.topics[topicName] = topic
	}

	subscriber := newSubscriberConnection(conn, topicName)
	topic.AddSubscriber(subscriber)
	log.Printf("Subscriber %s (%s) added to topic %s", conn.RemoteAddr(), subscriber.id, topicName)

	return subscriber
}

func (b *Broker) unsubscribe(subscriber *SubscriberConnection) {
	b.topicsMutex.Lock()
	defer b.topicsMutex.Unlock()

	for _, topic := range b.topics {
		if topic.name != subscriber.topic {
			continue
		}
		topic.RemoveSubscriber(subscriber)
		log.Printf("Subscriber %s unsubscribed from topic %s", subscriber.id, topic.name)
		return
	}
	log.Printf("Subscriber %s was not subscribed to any topic", subscriber.id)
}

func (b *Broker) subscriberLoop(_ context.Context, subscriber *SubscriberConnection) {
	conn := subscriber.conn
	log := log.New(log.Writer(), fmt.Sprintf("[Subscriber:%s] ", subscriber.id), log.LstdFlags)
	log.Printf("Handling subscriber connection from %s", conn.RemoteAddr())
	for {
		log.Printf("Waiting for commands from subscriber %s", subscriber.id)
		cmds, err := command.ReadCommands(conn, b.params.KeepAlive)
		var netErr net.Error
		switch {
		case errors.Is(err, net.ErrClosed):
			log.Printf("Connection closed broker %s", subscriber.id)
			return
		case errors.Is(err, io.EOF):
			log.Printf("Connection closed by subscriber %s", subscriber.id)
			return
		case errors.As(err, &netErr) && netErr.Timeout():
			log.Printf("Timeout reading from subscriber %s: %v", subscriber.id, err)
			conn.Close()
			return
		case err != nil:
			log.Printf("Failed to read command from subscriber %s: %v", subscriber.id, err)
			continue
		}

		for _, cmd := range cmds {
			switch cmd := cmd.(type) {
			case *command.KeepAliveCommand:
				log.Printf("Received keep-alive command from subscriber %s", subscriber.id)
			case *command.AckCommand, *command.NackCommand:
				log.Printf("Received %s command from subscriber %s", cmd.Action(), subscriber.id)
			case *command.UnsubscribeCommand:
				log.Printf("Received unsubscribe command from subscriber %s", subscriber.id)
				b.unsubscribe(subscriber)
				command.WriteCommand(conn, command.NewAckCommand(cmd))
			default:
				log.Printf("Unexpected command from subscriber %s: %s", subscriber.id, cmd.Action())
				command.WriteCommand(conn, command.NewNackCommand(cmd))
			}
		}
	}
}

func (b *Broker) addPublisher(conn net.Conn) *PublisherConnection {
	b.publishersMutex.Lock()
	defer b.publishersMutex.Unlock()

	id := uuid.NewString()
	publisher := newPublisherConnection(conn)
	b.publishers = append(b.publishers, publisher)
	log.Printf("Added new publisher from %s, with ID: %s", conn.RemoteAddr(), id)

	return publisher
}

func (b *Broker) handShakeWithClient(ctx context.Context, conn net.Conn) error {
	cmds, err := command.ReadCommands(conn, time.Second*5)
	if err != nil {
		log.Printf("Failed to read command: %v", err)
		return err
	}

	cmd := cmds[0]
	switch cmd := cmd.(type) {
	case *command.HandshakeCommand:
		switch cmd.ClientType() {
		case command.Publisher:
			log.Printf("Registered new publisher from %s", conn.RemoteAddr())
			publisher := b.addPublisher(conn)
			b.wg.Go(func() { b.publisherLoop(ctx, publisher) })
			command.WriteCommand(publisher.conn, command.NewAckCommand(cmd))
			b.sendConfig(publisher)
		case command.Subscriber:
			log.Printf("Registered new subscriber from %s", conn.RemoteAddr())
			subscriber := b.addSubscriber(conn, cmd)
			b.wg.Go(func() { b.subscriberLoop(ctx, subscriber) })
			command.WriteCommand(subscriber.conn, command.NewAckCommand(cmd))
			b.sendConfig(subscriber)
		default:
			log.Printf("Unknown client type: %s", cmd.Topic())
			command.WriteCommand(conn, command.NewNackCommand(cmd))
			return fmt.Errorf("unknown client type: %s", cmd.Topic())
		}
	default:
		log.Printf("Unknown client type: %s", cmd.Params()[0])
		command.WriteCommand(conn, command.NewNackCommand(cmd))
		return fmt.Errorf("unknown client type: %s", cmd.Params()[0])
	}

	return nil
}

func (b *Broker) handleConnection(ctx context.Context, conn net.Conn) {
	log.Printf("Accepted connection from %s", conn.RemoteAddr())

	err := b.handShakeWithClient(ctx, conn)
	if err != nil {
		log.Printf("Handshake failed: %v", err)
		return
	}
	log.Printf("Handshake successful with %s", conn.RemoteAddr())
}

func (b *Broker) sendConfig(conn Connection) error {
	config := map[string]any{
		"keep_alive": b.params.KeepAlive.Seconds(),
		"id":         conn.ID(),
	}

	cmd := command.NewCommandConfigFromConfig(config)
	err := command.WriteCommand(conn.Conn(), cmd)
	return err
}

func (b *Broker) acceptLoop(ctx context.Context, socket net.Listener) {
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

		b.wg.Go(func() { b.handleConnection(ctx, conn) })
	}
}

func (b *Broker) Wait() {
	b.wg.Wait()
}

func (b *Broker) waitForCancel(ctx context.Context) {
	<-ctx.Done()
	b.Stop()
}

func (b *Broker) Start(ctx context.Context) {
	log.SetPrefix("[Broker] ")
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
	b.wg.Go(func() { b.acceptLoop(ctx, socket) })
	b.wg.Go(func() { b.waitForCancel(ctx) })
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
