package command

type KeepAliveCommand struct {
	BaseCommand
}

func NewKeepAliveCommand() Command {
	return &KeepAliveCommand{
		BaseCommand: BaseCommand{
			action: KeepAlive,
			params: []string{},
		},
	}
}
