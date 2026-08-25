package command

func NewPublishCommand(topic string, message string) *BaseCommand {
	return NewCommand(Publish, topic, message)
}
