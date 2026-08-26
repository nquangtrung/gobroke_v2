package command

type MessageCommand struct {
	BaseCommand
}

func (c *MessageCommand) From() string {
	params := c.Params()
	if len(params) == 0 {
		return ""
	}
	return params[0]
}

func (c *MessageCommand) Topic() string {
	params := c.Params()
	if len(params) < 2 {
		return ""
	}
	return params[1]
}

func (c *MessageCommand) Message() string {
	params := c.Params()
	if len(params) < 3 {
		return ""
	}
	return params[2]
}

func NewMessageCommand(from string, topic string, message string) *MessageCommand {
	return &MessageCommand{
		BaseCommand: BaseCommand{
			action: Message,
			params: []string{from, topic, message},
		},
	}
}

func NewMessageCommandFromCommand(from string, cmd PublishableCommand) *MessageCommand {
	return NewMessageCommand(from, cmd.Topic(), cmd.Message())
}
