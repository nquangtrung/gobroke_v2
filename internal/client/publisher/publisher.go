package publisher

import (
	"fmt"
	"log"
	"net"

	netHelper "trontria.com/gobroke/v2/internal/net"
)

type Publisher struct {
	params ProviderParams
	conn   net.Conn
}

func (p *Publisher) Stop() error {
	log.Println("Stopping publisher...")
	if p.conn != nil {
		err := p.conn.Close()
		if err != nil {
			log.Printf("Failed to close connection: %v", err)
			return err
		}
		log.Println("Publisher stopped.")
	}
	return nil
}

func (p *Publisher) handshakeWithServer() error {
	if p.conn == nil {
		return fmt.Errorf("Connection not provided")
	}

	command := netHelper.NewHandshakeCommand(netHelper.Publisher)
	err := netHelper.WriteCommand(p.conn, command)
	if err != nil {
		log.Printf("Failed to send handshake command: %v", err)
		return err
	}

	response, err := netHelper.ReadCommand(p.conn)
	if err != nil {
		log.Printf("Failed to read handshake response: %v", err)
		return err
	}

	if !response.IsAckOf(command) {
		return fmt.Errorf("expected ACK of handshake command, but got: %s", response.Command)
	}

	return nil
}

func (p *Publisher) Start() error {
	log.Println("Starting publisher...")
	conn, err := netHelper.CreateClientConnection(p.params.Type)
	if err != nil {
		log.Fatalf("Failed to create socket: %v", err)
	}
	p.conn = conn
	defer conn.Close()

	log.Printf("Publisher connected on %s", conn.LocalAddr())

	err = p.handshakeWithServer()
	if err != nil {
		log.Fatalf("Handshake failed: %v", err)
	}
	log.Println("Handshake successful.")
	return nil
}

type ProviderParams struct {
	Type netHelper.ConnectionType
}

func New(params ProviderParams) *Publisher {
	return &Publisher{params: params}
}
