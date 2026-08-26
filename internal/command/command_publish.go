package command

func NewPublishCommand(topic string, message string) *BaseCommand {
	return NewCommand(Publish, topic, message)
}

func NewMessageCommand(from string, cmd *BaseCommand) *BaseCommand {
	if !cmd.IsPublish() {
		return nil
	}

	return NewCommand(Message, append([]string{from}, cmd.Params...)...)
}
