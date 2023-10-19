package memory

import (
	"bytes"
	"fmt"
	"io"
	"sync"

	"github.com/eclipse/paho.golang/autopaho/queue"
)

// A queue implementation that stores all data in RAM

// Queue - basic memory based queue
type Queue struct {
	mu              sync.Mutex
	messages        [][]byte
	waiting         []chan<- struct{} // closed when something arrives in the queue
	waitingForEmpty []chan<- struct{} // closed when queue is empty
}

// New creates a new memory-based queue
func New() *Queue {
	return &Queue{}
}

// Wait returns a channel that is closed when there is something in the queue
func (q *Queue) Wait() chan struct{} {
	c := make(chan struct{})
	q.mu.Lock()
	if len(q.messages) > 0 {
		q.mu.Unlock()
		close(c)
		return c
	}
	q.waiting = append(q.waiting, c)
	q.mu.Unlock()
	return c
}

// WaitForEmpty returns a channel that is closed when the queue is empty
func (q *Queue) WaitForEmpty() chan struct{} {
	c := make(chan struct{})
	q.mu.Lock()
	if len(q.messages) == 0 {
		q.mu.Unlock()
		close(c)
		return c
	}
	q.waitingForEmpty = append(q.waitingForEmpty, c)
	q.mu.Unlock()
	return c
}

// Enqueue add item to the queue.
func (q *Queue) Enqueue(p io.Reader) error {
	var b bytes.Buffer
	_, err := b.ReadFrom(p)
	if err != nil {
		return fmt.Errorf("Queue.Push failed to read into buffer: %w", err)
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.messages = append(q.messages, b.Bytes())
	for _, c := range q.waiting {
		close(c)
	}
	q.waiting = q.waiting[:0]
	return nil
}

// Peek retrieves the oldest item from the queue (without removing it)
func (q *Queue) Peek() (io.ReadCloser, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.messages) == 0 {
		return nil, queue.ErrEmpty
	}
	return io.NopCloser(bytes.NewReader(q.messages[0])), nil
}

// Dequeue removes the oldest item from the queue (without returning it)
func (q *Queue) Dequeue() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.messages) == 0 {
		for _, c := range q.waitingForEmpty {
			close(c)
		}
		q.waitingForEmpty = q.waitingForEmpty[:0]

		return queue.ErrEmpty
	}
	q.messages = q.messages[1:]
	return nil
}
