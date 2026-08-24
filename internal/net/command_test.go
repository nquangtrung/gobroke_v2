package net

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewCommandFromString(t *testing.T) {
	tests := []struct {
		commandStr string
		expected   *BaseCommand
	}{
		{"HANDSHAKE PUBLISHER", &BaseCommand{Command: HandshakeCommand, Params: []string{"PUBLISHER"}}},
		{"HANDSHAKE PUBLISHER\n", &BaseCommand{Command: HandshakeCommand, Params: []string{"PUBLISHER"}}},
		{"HANDSHAKE SUBSCRIBER", &BaseCommand{Command: HandshakeCommand, Params: []string{"SUBSCRIBER"}}},
		{"HANDSHAKE SUBSCRIBER\n", &BaseCommand{Command: HandshakeCommand, Params: []string{"SUBSCRIBER"}}},
	}

	for _, test := range tests {
		cmd, err := NewCommandFromString(test.commandStr)
		assert.NoError(t, err)
		assert.Equal(t, test.expected.Command, cmd.Command)
		assert.Equal(t, test.expected.Params, cmd.Params)
	}
}

func TestNewCommandFromBytes(t *testing.T) {
	tests := []struct {
		commandBytes []byte
		expected     *BaseCommand
	}{
		{[]byte("HANDSHAKE PUBLISHER\x00"), &BaseCommand{Command: HandshakeCommand, Params: []string{"PUBLISHER"}}},
		{[]byte("HANDSHAKE PUBLISHER\n\x00\x00\x00\x00"), &BaseCommand{Command: HandshakeCommand, Params: []string{"PUBLISHER"}}},
		{[]byte("HANDSHAKE SUBSCRIBER\x00\x00"), &BaseCommand{Command: HandshakeCommand, Params: []string{"SUBSCRIBER"}}},
		{[]byte("HANDSHAKE SUBSCRIBER\n\x00\x00\x00\x00\x00\x00"), &BaseCommand{Command: HandshakeCommand, Params: []string{"SUBSCRIBER"}}},
	}

	for _, test := range tests {
		cmd, err := NewCommandFromBytes(test.commandBytes)
		assert.NoError(t, err)
		assert.Equal(t, test.expected.Command, cmd.Command)
		assert.Equal(t, test.expected.Params, cmd.Params)
	}
}
