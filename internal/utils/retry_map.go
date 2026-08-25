package utils

import "sync"

type RetryMap[T any] struct {
	retriesMutex sync.Mutex
	retries      map[*T]int
}

func (rcm *RetryMap[T]) AddRetry(cmd *T) {
	rcm.retriesMutex.Lock()
	defer rcm.retriesMutex.Unlock()

	if rcm.retries == nil {
		rcm.retries = make(map[*T]int)
	}

	rcm.retries[cmd] = rcm.retries[cmd] + 1
}

func (rcm *RetryMap[T]) GetRetryCount(cmd *T) int {
	rcm.retriesMutex.Lock()
	defer rcm.retriesMutex.Unlock()

	return rcm.retries[cmd]
}

func (rcm *RetryMap[T]) RemoveRetry(cmd *T) {
	rcm.retriesMutex.Lock()
	defer rcm.retriesMutex.Unlock()

	delete(rcm.retries, cmd)
}
