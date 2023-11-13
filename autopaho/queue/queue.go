package queue

import (
	"errors"
	"io"
)

var (
	ErrEmpty = errors.New("empty queue")
)

// Entry - permits access to a queue entry
// Users must call one of Leave, Remove, or Error when done with the entry (and before calling Peek again)
// Note that `Reader()` must noth be called after calling Leave, Remove, or Error
type Entry interface {
	Reader() (io.Reader, error) // Provides access to the file contents, subsequent calls may return the same reader
	Leave() error               // Leave the entry in the queue (same entry will be returned on subsequent calls to Peek).
	Remove() error              // Remove this entry from the queue. Returns queue.ErrEmpty if queue is empty after operation
	Error() error               // Flag that this entry has an error (remove from queue, potentially retaining data with error flagged)
}

// Queue provides the functionality needed to manage queued messages
type Queue interface {
	// Wait returns a channel that is closed when there is something in the queue (will return a closed channel if the
	// queue is empty at the time of the call)
	Wait() chan struct{}

	// Enqueue add item to the queue.
	Enqueue(p io.Reader) error

	// Peek retrieves the oldest item from the queue without removing it
	// Users must call one of Close, Remove, or Error when done with the entry, and before calling Peek again.
	// Warning: Peek is not safe for concurrent use (it may return the same Entry leading to unpredictable results)
	Peek() (Entry, error)
}
