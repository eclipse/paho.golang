package queue

import (
	"errors"
	"io"
)

var (
	ErrEmpty = errors.New("empty queue")
)

// Queue provides the functionality needed to manage queued messages
type Queue interface {
	// Wait returns a channel that is closed when there is something in the queue (will return a closed channel if the
	// queue is empty at the time of the call)
	Wait() chan struct{}

	// Enqueue add item to the queue.
	Enqueue(p io.Reader) error

	// Peek retrieves the oldest item from the queue (without removing it)
	Peek() (io.ReadCloser, error)

	// Dequeue removes the oldest item from the queue (without returning it)
	Dequeue() error
}
