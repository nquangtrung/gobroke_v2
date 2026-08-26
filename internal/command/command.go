package command

import (
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"trontria.com/gobroke/v2/internal/utils"
)

type Action string

const (
	Handshake Action = "HANDSHAKE"
	Ack       Action = "ACK"
	Nack      Action = "NACK"
	Publish   Action = "PUBLISH"
	Message   Action = "MESSAGE"
	KeepAlive Action = "KEEPALIVE"
	Config    Action = "CONFIG"
)

type Command interface {
	String() string

	IsAck() bool
	IsNack() bool
	IsConfig() bool
	IsHandshake() bool
	IsPublish() bool
	IsKeepAlive() bool
	IsSame(cmd Command) bool

	Action() Action
	Params() []string
}

type BaseCommand struct {
	action Action
	params []string
}

func (c *BaseCommand) Action() Action {
	return c.action
}

func (c *BaseCommand) Params() []string {
	return c.params
}

func (c *BaseCommand) String() string {
	return fmt.Sprintf("%s %s", c.Action(), strings.Join(c.Params(), " "))
}

func (c *BaseCommand) IsAck() bool {
	return c.action == Ack
}

func (c *BaseCommand) IsNack() bool {
	return c.action == Nack
}

func (c *BaseCommand) IsConfig() bool {
	return c.action == Config
}

func (c *BaseCommand) IsHandshake() bool {
	return c.action == Handshake
}

func (c *BaseCommand) IsPublish() bool {
	return c.action == Publish
}
func (c *BaseCommand) IsKeepAlive() bool {
	return c.action == KeepAlive
}

func (c *BaseCommand) IsSame(cmd Command) bool {
	if c.action != cmd.Action() {
		return false
	}
	if len(c.params) != len(cmd.Params()) {
		return false
	}
	for i, param := range c.params {
		if param != cmd.Params()[i] {
			return false
		}
	}
	return true
}

func NewCommandsFromBytes(commandBytes []byte) ([]Command, error) {
	if len(commandBytes) == 0 {
		return nil, fmt.Errorf("command bytes are empty")
	}

	commandStr := strings.Trim(string(commandBytes), "\x00")
	return NewCommandsFromString(commandStr)
}

func NewCommandFromString(cmdStr string) (Command, error) {
	parts := strings.Split(strings.Trim(cmdStr, "\n"), " ")
	if len(parts) == 0 {
		return nil, fmt.Errorf("command string is empty")
	}

	switch Action(parts[0]) {
	case Handshake:
		return NewHandshakeCommand(ClientType(parts[1]), parts[2:]...), nil
	case Ack:
		return NewAckCommand(NewCommand(Action(parts[1]), parts[2:]...)), nil
	case Nack:
		return NewNackCommand(NewCommand(Action(parts[1]), parts[2:]...)), nil
	case Publish:
		return NewPublishCommand(parts[1], parts[2]), nil
	case Message:
		return NewMessageCommand(parts[1], parts[2], parts[3]), nil
	case KeepAlive:
		return NewKeepAliveCommand(), nil
	case Config:
		return NewCommandConfig(parts[1]), nil
	default:
		return NewCommand(Action(parts[0]), parts[1:]...), nil
	}
}

func NewCommandsFromString(commandStr string) ([]Command, error) {
	commands := utils.Filter(strings.Split(commandStr, "\n"), func(cmd string) bool {
		return cmd != ""
	})

	return utils.Map(commands, func(cmdStr string) (Command, error) {
		return NewCommandFromString(cmdStr)
	})
}

func NewCommand(action Action, params ...string) Command {
	return &BaseCommand{
		action: action,
		params: params,
	}
}

func WriteCommand(conn net.Conn, command Command) error {
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

func ReadCommands(conn net.Conn, timeout ...time.Duration) ([]Command, error) {
	buf := make([]byte, 4096)

	// Read data from the connection.
	if len(timeout) > 0 {
		conn.SetReadDeadline(time.Now().Add(timeout[0])) // Set a read deadline to avoid blocking indefinitely
	}
	n, err := conn.Read(buf)
	if err != nil {
		return nil, err
	}

	log.Printf("Received data %v", string(buf[:n]))
	commands, err := NewCommandsFromBytes(buf[:n])

	return commands, err
}

func WriteCommandAndWaitForAck(conn net.Conn, command Command) error {
	err := WriteCommand(conn, command)
	if err != nil {
		return err
	}

	response, err := ReadCommands(conn)
	if err != nil {
		return err
	}

	if !response[0].(IsOfer).IsOf(command) || !response[0].IsAck() {
		return fmt.Errorf("expected ACK for command %s, but got: %s", command.String(), response[0].String())
	}

	return nil
}

func WriteAckOrNack(conn net.Conn, command Command, err error) error {
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
