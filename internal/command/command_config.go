package command

import (
	"encoding/json"
	"errors"

	"trontria.com/gobroke/v2/internal/utils"
)

type ConfigCommand struct {
	BaseCommand
}

func NewCommandConfigFromConfig(config map[string]any) Command {
	jsonConfig := utils.Must(json.Marshal(config))
	return &ConfigCommand{
		BaseCommand: BaseCommand{
			action: Config,
			params: []string{string(jsonConfig)},
		},
	}
}

func NewCommandConfig(config string) Command {
	return &ConfigCommand{
		BaseCommand: BaseCommand{
			action: Config,
			params: []string{config},
		},
	}
}

func ParseCommandConfig(cmd Command) (map[string]any, error) {
	if cmd.Action() != Config {
		return nil, errors.New("Invalid command: expected CONFIG")
	}

	var config map[string]any
	err := json.Unmarshal([]byte(cmd.Params()[0]), &config)
	if err != nil {
		return nil, err
	}

	return config, nil
}
