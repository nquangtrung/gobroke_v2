package command

import (
	"encoding/json"
	"errors"

	"trontria.com/gobroke/v2/internal/utils"
)

func NewCommandConfig(config map[string]any) *BaseCommand {
	jsonConfig := utils.Must(json.Marshal(config))
	return NewCommand(Config, string(jsonConfig))
}

func ParseCommandConfig(cmd *BaseCommand) (map[string]any, error) {
	if cmd.Command != Config {
		return nil, errors.New("Invalid command: expected CONFIG")
	}

	var config map[string]any
	err := json.Unmarshal([]byte(cmd.Params[0]), &config)
	if err != nil {
		return nil, err
	}

	return config, nil
}
