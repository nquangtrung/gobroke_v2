package utils

import "log"

type Worker struct {
	guard chan struct{}
}

func (w *Worker) Stop() {
	close(w.guard)
}

func (w *Worker) Do(f func()) {
	log.Printf("Spawning worker routine - capacity: %d/%d", len(w.guard), cap(w.guard))
	w.guard <- struct{}{}
	go func() {
		defer func() { <-w.guard }()
		f()
	}()
}

func NewWorker(maxWorker int) *Worker {
	return &Worker{
		guard: make(chan struct{}, maxWorker),
	}
}
