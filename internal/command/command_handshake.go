package command

type ClientType string

const (
	Publisher  ClientType = "PUBLISHER"
	Subscriber ClientType = "SUBSCRIBER"
)

type HandshakeCommand struct {
	BaseCommand
}

func NewHandshakeCommand(clientType ClientType, topic ...string) Command {
	if len(topic) > 0 {
		return &HandshakeCommand{
			BaseCommand: BaseCommand{
				action: Handshake,
				params: []string{string(clientType), topic[0]},
			},
		}
	}

	return &HandshakeCommand{
		BaseCommand: BaseCommand{
			action: Handshake,
			params: []string{string(clientType)},
		},
	}
}
