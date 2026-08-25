package command

import (
	"fmt"
	"log"
	"net"
	"strings"

	"trontria.com/gobroke/v2/internal/utils"
)

type Command string

const (
	Handshake Command = "HANDSHAKE"
	Ack       Command = "ACK"
	Nack      Command = "NACK"
	Publish   Command = "PUBLISH"
)

type BaseCommand struct {
	Command Command
	Params  []string
}

func (c *BaseCommand) String() string {
	return fmt.Sprintf("%s %s", c.Command, strings.Join(c.Params, " "))
}

func (c *BaseCommand) IsAck() bool {
	return c.Command == Ack
}
func (c *BaseCommand) IsNack() bool {
	return c.Command == Nack
}

func (c *BaseCommand) IsAckOf(cmd *BaseCommand) bool {
	if c.Command != Ack && c.Command != Nack {
		return false
	}

	if len(c.Params) == 0 {
		return false
	}

	if Command(c.Params[0]) != cmd.Command {
		return false
	}

	for i, param := range c.Params[1:] {
		if i >= len(cmd.Params) || param != cmd.Params[i] {
			return false
		}
	}

	return true
}

func (c *BaseCommand) IsNackOf(cmd *BaseCommand) bool {
	return c.Command == Nack && Command(c.Params[0]) == cmd.Command
}

func (c *BaseCommand) IsHandshake() bool {
	return c.Command == Handshake
}

func (c *BaseCommand) IsPublish() bool {
	return c.Command == Publish
}

func (c *BaseCommand) IsSame(cmd BaseCommand) bool {
	if c.Command != cmd.Command {
		return false
	}
	if len(c.Params) != len(cmd.Params) {
		return false
	}
	for i, param := range c.Params {
		if param != cmd.Params[i] {
			return false
		}
	}
	return true
}

func NewCommandsFromBytes(commandBytes []byte) ([]*BaseCommand, error) {
	if len(commandBytes) == 0 {
		return nil, fmt.Errorf("command bytes are empty")
	}

	commandStr := strings.Trim(string(commandBytes), "\x00")
	return NewCommandsFromString(commandStr)
}

func NewCommandsFromString(commandStr string) ([]*BaseCommand, error) {
	commands := utils.Filter(strings.Split(commandStr, "\n"), func(cmd string) bool {
		return cmd != ""
	})
	return utils.Map(commands, func(cmdStr string) (*BaseCommand, error) {
		parts := strings.Split(strings.Trim(cmdStr, "\n"), " ")
		if len(parts) == 0 {
			return nil, fmt.Errorf("command string is empty")
		}

		return NewCommand(Command(parts[0]), parts[1:]...), nil
	})
}

func NewCommand(command Command, params ...string) *BaseCommand {
	return &BaseCommand{
		Command: command,
		Params:  params,
	}
}

func NewAckCommand(cmd *BaseCommand) *BaseCommand {
	return NewCommand(Ack, cmd.String())
}

func NewNackCommand(cmd *BaseCommand) *BaseCommand {
	return NewCommand(Nack, cmd.String())
}

func NewPublishCommand(topic string, message string) *BaseCommand {
	return NewCommand(Publish, topic, message)
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

func ReadCommands(conn net.Conn) ([]*BaseCommand, error) {
	buf := make([]byte, 4096)

	// Read data from the connection.
	n, err := conn.Read(buf)
	if err != nil {
		return nil, fmt.Errorf("failed to read from connection: %w", err)
	}

	log.Printf("Received data %v", string(buf[:n]))
	commands, err := NewCommandsFromBytes(buf[:n])

	return commands, err
}

func WriteCommandAndWaitForAck(conn net.Conn, command *BaseCommand) error {
	err := WriteCommand(conn, command)
	if err != nil {
		return err
	}

	response, err := ReadCommands(conn)
	if err != nil {
		return err
	}

	if !response[0].IsAckOf(command) {
		return fmt.Errorf("expected ACK for command %s, but got: %s", command.String(), response[0].String())
	}

	return nil
}

func WriteAckOrNack(conn net.Conn, command *BaseCommand, err error) error {
	if err != nil {
		log.Printf("Sending NACK for command %s due to error: %v", command.String(), err)
		nackCmd := NewNackCommand(command)
		return WriteCommand(conn, nackCmd)
	} else {
		log.Printf("Sending ACK for command %s", command.String())
		ackCmd := NewAckCommand(command)
		return WriteCommand(conn, ackCmd)
	}
}
