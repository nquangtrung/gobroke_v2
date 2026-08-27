package command

type ClientType string

const (
	Publisher  ClientType = "PUBLISHER"
	Subscriber ClientType = "SUBSCRIBER"
)

type HandshakeCommand struct {
	BaseCommand
}

func (c *HandshakeCommand) ClientType() ClientType {
	if len(c.params) > 0 {
		return ClientType(c.params[0])
	}
	return ""
}

func (c *HandshakeCommand) Topic() string {
	if len(c.params) > 1 {
		return c.params[1]
	}
	return ""
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
