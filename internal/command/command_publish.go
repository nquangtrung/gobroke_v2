package command

type PublishableCommand interface {
	Command
	Topic() string
	Message() string
}

type PublishCommand struct {
	BaseCommand
}

func (c *PublishCommand) Topic() string {
	params := c.Params()
	if len(params) == 0 {
		return ""
	}
	return params[0]
}

func (c *PublishCommand) Message() string {
	params := c.Params()
	if len(params) < 2 {
		return ""
	}
	return params[1]
}

func NewPublishCommand(topic string, message string) Command {
	return &PublishCommand{
		BaseCommand: BaseCommand{
			action: Publish,
			params: []string{topic, message},
		},
	}
}
