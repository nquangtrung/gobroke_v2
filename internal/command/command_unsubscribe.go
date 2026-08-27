package command

type UnsubscribeCommand struct {
	BaseCommand
}

func NewUnsubscribeCommand() Command {
	return &UnsubscribeCommand{
		BaseCommand: BaseCommand{
			action: Unsubscribe,
			params: []string{},
		},
	}
}
