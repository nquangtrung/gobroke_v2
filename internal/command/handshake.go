package command

type ClientType string

const (
	Publisher  ClientType = "PUBLISHER"
	Subscriber ClientType = "SUBSCRIBER"
)

func NewHandshakeCommand(clientType ClientType, topic ...string) *BaseCommand {
	if len(topic) > 0 {
		return NewCommand(Handshake, string(clientType), topic[0])
	}

	return NewCommand(Handshake, string(clientType))
}
