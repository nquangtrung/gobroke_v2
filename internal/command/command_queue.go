package command

import (
	"sync"
)

type CommandQueue struct {
	awaitingAckCommandsMutex sync.Mutex
	awaitingAckCommands      []*BaseCommand
}

func (cq *CommandQueue) AddCommand(cmd *BaseCommand) {
	cq.awaitingAckCommandsMutex.Lock()
	defer cq.awaitingAckCommandsMutex.Unlock()

	cq.awaitingAckCommands = append(cq.awaitingAckCommands, cmd)
}

func (cq *CommandQueue) RemoveCommand(cmd *BaseCommand) {
	cq.awaitingAckCommandsMutex.Lock()
	defer cq.awaitingAckCommandsMutex.Unlock()

	for i, c := range cq.awaitingAckCommands {
		if c.IsSame(*cmd) {
			cq.awaitingAckCommands = append(cq.awaitingAckCommands[:i], cq.awaitingAckCommands[i+1:]...)
			break
		}
	}
}

func (cq *CommandQueue) GetAndRemoveCommandFromAck(ackCommand *BaseCommand) *BaseCommand {
	cq.awaitingAckCommandsMutex.Lock()
	defer cq.awaitingAckCommandsMutex.Unlock()

	for i := 0; i < len(cq.awaitingAckCommands); i++ {
		if ackCommand.IsAckOf(cq.awaitingAckCommands[i]) {
			cmd := cq.awaitingAckCommands[i]
			cq.awaitingAckCommands = append(cq.awaitingAckCommands[:i], cq.awaitingAckCommands[i+1:]...)
			return cmd
		}
	}

	return nil
}
