package subscriber

import "log"

type Worker struct {
	guard chan struct{}
}

func (w *Worker) Do(f func(message string), message string) {
	log.Printf("Worker received message: %s queue: %d/%d", message, len(w.guard), cap(w.guard))
	w.guard <- struct{}{}
	go func() {
		defer func() { <-w.guard }()
		f(message)
	}()
}

func NewWorker(maxWorker int) *Worker {
	return &Worker{
		guard: make(chan struct{}, maxWorker),
	}
}
