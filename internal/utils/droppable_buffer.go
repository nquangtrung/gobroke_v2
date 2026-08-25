package utils

import "time"

type DropType int

const (
	DropOldest DropType = iota
	DropNewest
)

type DroppableBuffer[T any] struct {
	buffer  chan T
	size    int
	drop    DropType
	timeout time.Duration

	receiving bool
}

func (db *DroppableBuffer[T]) Drain(fn func(T)) {
	db.receiving = false

	for len(db.buffer) > 0 {
		item := <-db.buffer
		fn(item)
	}

	close(db.buffer)
}

func (db *DroppableBuffer[T]) Add(item T) {
	if !db.receiving {
		return
	}

	timeout := time.After(db.timeout)
	select {
	case db.buffer <- item:
	case <-timeout:
		switch db.drop {
		case DropOldest:
			<-db.buffer // Remove the oldest item
			db.buffer <- item
		case DropNewest:
			// Do nothing, drop the new item
		}
	}
}

func (db *DroppableBuffer[T]) Channel() chan T {
	return db.buffer
}

func (db *DroppableBuffer[T]) Get() T {
	return <-db.buffer
}

func (db *DroppableBuffer[T]) Size() int {
	return len(db.buffer)
}

func (db *DroppableBuffer[T]) IsFull() bool {
	return len(db.buffer) >= db.size
}

func NewDroppableBuffer[T any](size int, drop DropType, timeout time.Duration) *DroppableBuffer[T] {
	return &DroppableBuffer[T]{
		buffer:    make(chan T, size),
		size:      size,
		drop:      drop,
		timeout:   timeout,
		receiving: true,
	}
}
