package command

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewCommandFromString(t *testing.T) {
	tests := []struct {
		commandStr string
		expected   *BaseCommand
	}{
		{"HANDSHAKE PUBLISHER", &BaseCommand{action: Handshake, params: []string{"PUBLISHER"}}},
		{"HANDSHAKE PUBLISHER\n", &BaseCommand{action: Handshake, params: []string{"PUBLISHER"}}},
		{"HANDSHAKE SUBSCRIBER", &BaseCommand{action: Handshake, params: []string{"SUBSCRIBER"}}},
		{"HANDSHAKE SUBSCRIBER\n", &BaseCommand{action: Handshake, params: []string{"SUBSCRIBER"}}},
	}

	for _, test := range tests {
		cmd, err := NewCommandsFromString(test.commandStr)
		assert.NoError(t, err)
		assert.Equal(t, test.expected.action, cmd[0].Action())
		assert.Equal(t, test.expected.params, cmd[0].Params())
	}
}

func TestNewCommandFromBytes(t *testing.T) {
	tests := []struct {
		commandBytes []byte
		expected     *BaseCommand
	}{
		{[]byte("HANDSHAKE PUBLISHER\x00"), &BaseCommand{action: Handshake, params: []string{"PUBLISHER"}}},
		{[]byte("HANDSHAKE PUBLISHER\n\x00\x00\x00\x00"), &BaseCommand{action: Handshake, params: []string{"PUBLISHER"}}},
		{[]byte("HANDSHAKE SUBSCRIBER\x00\x00"), &BaseCommand{action: Handshake, params: []string{"SUBSCRIBER"}}},
		{[]byte("HANDSHAKE SUBSCRIBER\n\x00\x00\x00\x00\x00\x00"), &BaseCommand{action: Handshake, params: []string{"SUBSCRIBER"}}},
	}

	for _, test := range tests {
		cmd, err := NewCommandsFromBytes(test.commandBytes)
		assert.NoError(t, err)
		assert.Equal(t, test.expected.action, cmd[0].Action())
		assert.Equal(t, test.expected.params, cmd[0].Params())
	}
}
