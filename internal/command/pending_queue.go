package command

import (
	"sync"
)

type PendingQueue struct {
	awaitingAckCommandsMutex sync.Mutex
	awaitingAckCommands      []Command
}

func (cq *PendingQueue) AddCommand(cmd Command) {
	cq.awaitingAckCommandsMutex.Lock()
	defer cq.awaitingAckCommandsMutex.Unlock()

	cq.awaitingAckCommands = append(cq.awaitingAckCommands, cmd)
}

func (cq *PendingQueue) RemoveCommand(cmd Command) {
	cq.awaitingAckCommandsMutex.Lock()
	defer cq.awaitingAckCommandsMutex.Unlock()

	for i, c := range cq.awaitingAckCommands {
		if c.IsSame(cmd) {
			cq.awaitingAckCommands = append(cq.awaitingAckCommands[:i], cq.awaitingAckCommands[i+1:]...)
			break
		}
	}
}

func (cq *PendingQueue) GetAndRemoveCommandFromAck(ackCommand IsOfer) Command {
	cq.awaitingAckCommandsMutex.Lock()
	defer cq.awaitingAckCommandsMutex.Unlock()

	for i := 0; i < len(cq.awaitingAckCommands); i++ {
		if ackCommand.IsOf(cq.awaitingAckCommands[i]) {
			cmd := cq.awaitingAckCommands[i]
			cq.awaitingAckCommands = append(cq.awaitingAckCommands[:i], cq.awaitingAckCommands[i+1:]...)
			return cmd
		}
	}

	return nil
}
