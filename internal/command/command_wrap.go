package command

type IsOfer interface {
	IsOf(cmd Command) bool
}

type WrapCommand struct {
	BaseCommand
}

func (c *WrapCommand) IsOf(cmd Command) bool {
	if len(c.params) == 0 {
		return false
	}

	if Action(c.params[0]) != cmd.Action() {
		return false
	}

	if len(c.params) != len(cmd.Params())+1 {
		return false
	}

	for i, param := range cmd.Params() {
		if c.params[i+1] != param {
			return false
		}
	}

	return true
}

func NewWrapCommand(action Action, cmd Command) Command {
	return &WrapCommand{
		BaseCommand: BaseCommand{
			action: action,
			params: append([]string{
				string(cmd.Action()),
			}, cmd.Params()...),
		},
	}
}
