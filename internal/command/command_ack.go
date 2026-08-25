package command

func NewAckCommand(cmd *BaseCommand) *BaseCommand {
	return NewCommand(Ack, cmd.String())
}

func NewNackCommand(cmd *BaseCommand) *BaseCommand {
	return NewCommand(Nack, cmd.String())
}
