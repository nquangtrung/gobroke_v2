package command

type AckCommand struct {
	WrapCommand
}

type NackCommand struct {
	WrapCommand
}

func NewAckCommand(cmd Command) Command {
	return &AckCommand{
		WrapCommand: WrapCommand{
			BaseCommand: BaseCommand{
				action: Ack,
				params: append([]string{
					string(cmd.Action()),
				}, cmd.Params()...),
			},
		},
	}
}

func NewNackCommand(cmd Command) Command {
	return &NackCommand{
		WrapCommand: WrapCommand{
			BaseCommand: BaseCommand{
				action: Nack,
				params: append([]string{
					string(cmd.Action()),
				}, cmd.Params()...),
			},
		},
	}
}
