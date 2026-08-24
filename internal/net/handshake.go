package net

type ClientType string

const (
	Publisher  ClientType = "PUBLISHER"
	Subscriber ClientType = "SUBSCRIBER"
)

func NewHandshakeCommand(clientType ClientType) *BaseCommand {
	return NewCommand(HandshakeCommand, string(clientType))
}
