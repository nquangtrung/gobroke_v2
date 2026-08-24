package utils

import "log"

type Processor[T any] interface {
	Process(data T)
	CleanUp(channel chan T)
}

type BaseRunner[T any] struct {
	channel     chan T
	stopChannel chan bool
	processor   Processor[T]
}

func NewBaseRunner[T any](buffer int, processor Processor[T]) *BaseRunner[T] {
	return &BaseRunner[T]{
		channel:     make(chan T, buffer),
		stopChannel: make(chan bool),
		processor:   processor,
	}
}
func (r BaseRunner[T]) Receive() chan T {
	return r.channel
}

func (r *BaseRunner[T]) Start() {
	for {
		select {
		case data := <-r.channel:
			r.processor.Process(data)
		case <-r.stopChannel:
			r.Drain()
			close(r.channel)
			close(r.stopChannel)
			r.processor.CleanUp(r.channel)
			return
		}
	}
}

func (r *BaseRunner[T]) Drain() {
	log.Printf("draining the rest of the channel: %d", len(r.channel))
	for len(r.channel) > 0 {
		data := <-r.channel
		r.processor.Process(data)
	}
}

func (r *BaseRunner[T]) Stop() {
	r.stopChannel <- true
}
