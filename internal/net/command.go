package net

import (
	"fmt"
	"log"
	"net"
	"strings"
)

type Command string

const (
	HandshakeCommand Command = "HANDSHAKE"
	AckCommand       Command = "ACK"
)

type BaseCommand struct {
	Command Command
	Params  []string
}

func (c *BaseCommand) String() string {
	return fmt.Sprintf("%s %s", c.Command, strings.Join(c.Params, " "))
}

func (c *BaseCommand) IsAck() bool {
	return c.Command == "ACK"
}

func (c *BaseCommand) IsAckOf(cmd *BaseCommand) bool {
	return c.Command == "ACK" && Command(c.Params[0]) == cmd.Command
}

func (c *BaseCommand) IsHandshake() bool {
	return c.Command == "HANDSHAKE"
}

func NewCommandFromBytes(commandBytes []byte) (*BaseCommand, error) {
	if len(commandBytes) == 0 {
		return nil, fmt.Errorf("command bytes are empty")
	}

	commandStr := strings.Trim(string(commandBytes), "\x00")
	return NewCommandFromString(commandStr)
}

func NewCommandFromString(commandStr string) (*BaseCommand, error) {
	if commandStr == "" {
		return nil, fmt.Errorf("command string is empty")
	}

	parts := strings.Split(strings.Trim(commandStr, "\n"), " ")
	if len(parts) == 0 {
		return nil, fmt.Errorf("command string is empty")
	}

	return NewCommand(Command(parts[0]), parts[1:]...), nil
}

func NewCommand(command Command, params ...string) *BaseCommand {
	return &BaseCommand{
		Command: command,
		Params:  params,
	}
}

func NewAckCommand(cmd *BaseCommand) *BaseCommand {
	return NewCommand("ACK", cmd.String())
}

func WriteCommand(conn net.Conn, command *BaseCommand) error {
	if conn == nil {
		return fmt.Errorf("connection is nil")
	}

	if command == nil {
		return fmt.Errorf("command is nil")
	}

	commandStr := command.String()
	_, err := conn.Write([]byte(commandStr + "\n"))

	if err != nil {
		log.Printf("Failed to write command: %v", err)
		return err
	}

	log.Printf("Sent command: %s", commandStr)
	return nil
}

func ReadCommand(conn net.Conn) (*BaseCommand, error) {
	buf := make([]byte, 4096)

	// Read data from the connection.
	n, err := conn.Read(buf)
	if err != nil {
		log.Printf("Failed to read data: %v", err)
		return nil, err
	}

	log.Printf("Received data %v", string(buf[:n]))
	command, err := NewCommandFromBytes(buf[:n])

	return command, err
}
