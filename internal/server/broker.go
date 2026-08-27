package server

import (
	"context"
	"errors"
	"fmt"
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

type ConnectHandlerStartFn func()

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

func (b *Broker) addPublisher(conn net.Conn) *PublisherConnection {
	b.publishersMutex.Lock()
	defer b.publishersMutex.Unlock()

	id := uuid.NewString()
	publisher := newPublisherConnection(conn)
	b.publishers = append(b.publishers, publisher)
	log.Printf("Added new publisher from %s, with ID: %s", conn.RemoteAddr(), id)

	return publisher
}

func (b *Broker) removePublisher(publisher *PublisherConnection) {
	b.publishersMutex.Lock()
	defer b.publishersMutex.Unlock()

	for i, p := range b.publishers {
		if p == publisher {
			b.publishers = append(b.publishers[:i], b.publishers[i+1:]...)
			log.Printf("Removed publisher %s", publisher.id)
			return
		}
	}
	log.Printf("Publisher %s not found for removal", publisher.id)
}

func (b *Broker) handShakeWithPublisher(ctx context.Context, conn net.Conn, cmd *command.HandshakeCommand) (ConnectHandlerStartFn, error) {
	log.Printf("Registered new publisher from %s", conn.RemoteAddr())
	publisher := b.addPublisher(conn)
	command.WriteCommand(publisher.conn, command.NewAckCommand(cmd))
	b.sendConfig(publisher)
	return func() { publisher.Loop(ctx, b) }, nil
}

func (b *Broker) handShakeWithSubscriber(ctx context.Context, conn net.Conn, cmd *command.HandshakeCommand) (ConnectHandlerStartFn, error) {
	log.Printf("Registered new subscriber from %s", conn.RemoteAddr())
	subscriber := b.addSubscriber(conn, cmd)
	command.WriteCommand(subscriber.conn, command.NewAckCommand(cmd))
	b.sendConfig(subscriber)
	return func() { subscriber.Loop(ctx, b) }, nil
}

func (b *Broker) handShakeWithClient(ctx context.Context, conn net.Conn) (ConnectHandlerStartFn, error) {
	cmds, err := command.ReadCommands(conn, time.Second*5)
	if err != nil {
		log.Printf("Failed to read command: %v", err)
		return nil, err
	}

	cmd := cmds[0]
	switch cmd := cmd.(type) {
	case *command.HandshakeCommand:
		switch cmd.ClientType() {
		case command.Publisher:
			return b.handShakeWithPublisher(ctx, conn, cmd)
		case command.Subscriber:
			return b.handShakeWithSubscriber(ctx, conn, cmd)
		default:
			log.Printf("Unknown client type: %s", cmd.Topic())
			command.WriteCommand(conn, command.NewNackCommand(cmd))
			return nil, fmt.Errorf("unknown client type: %s", cmd.Topic())
		}
	default:
		log.Printf("Unknown client type: %s", cmd.Params()[0])
		command.WriteCommand(conn, command.NewNackCommand(cmd))
		return nil, fmt.Errorf("unknown client type: %s", cmd.Params()[0])
	}
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

		loop, err := b.handShakeWithClient(ctx, conn)
		if err != nil {
			log.Printf("Failed to handle connection: %v", err)
			continue
		}

		b.wg.Go(func() { loop() })
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
		params:     resolvedParams,
		topics:     make(map[string]*Topic),
		publishers: make([]*PublisherConnection, 0),
	}
}
