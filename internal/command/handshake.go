package command

type ClientType string

const (
	Publisher  ClientType = "PUBLISHER"
	Subscriber ClientType = "SUBSCRIBER"
)

func NewHandshakeCommand(clientType ClientType) *BaseCommand {
	return NewCommand(Handshake, string(clientType))
}
