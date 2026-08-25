package command

func NewKeepAliveCommand() *BaseCommand {
	return NewCommand(KeepAlive)
}
