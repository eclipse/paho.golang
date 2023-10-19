package file

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/eclipse/paho.golang/autopaho/queue"
)

// A queue implementation that stores all data on disk
// This will be slow when there are a lot of messages queued. That is because the directory is read with every call to
// Peek/DeQueue (could cache some of this in RAM, but there is a reasonable chance that the OS does this for us).

const (
	folderPermissions = os.FileMode(0770)
	filePermissions   = os.FileMode(0666)
)

var (
	maxTime = time.Unix(1<<63-62135596801, 999999999)
)

// Queue - basic file based queue
type Queue struct {
	mu              sync.Mutex
	path            string
	prefix          string
	extension       string
	queueEmpty      bool              // true is the queue is currently empty
	waiting         []chan<- struct{} // closed when something arrives in the queue
	waitingForEmpty []chan<- struct{} // closed when queue is empty
}

// New creates a new file-based queue. Note that a file is written, read and deleted as part of this process to check
// that the path is usable.
// NOTE: Order is maintained using file ModTime, so there may be issues if the interval between messages is less than
// the file system ModTime resolution.
func New(path string, prefix string, extension string) (*Queue, error) {
	if len(extension) > 0 && extension[0] != '.' {
		extension = "." + extension
	}

	folderExists := true
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			folderExists = false
		}
		return nil, fmt.Errorf("stat on folder failed: %w", err)
	}
	if !folderExists {
		if err := os.MkdirAll(path, folderPermissions); err != nil {
			return nil, fmt.Errorf("failed to create store folder: %w", err)
		}
	}

	// Better to fail fast; so we check that a file can be written/read with the passed in info
	fn := filepath.Join(path, prefix+"TEST"+extension)
	if err := os.WriteFile(fn, []byte("test"), filePermissions); err != nil {
		return nil, fmt.Errorf("failed to write test file to specified folder: %w", err)
	}
	if _, err := os.ReadFile(fn); err != nil {
		return nil, fmt.Errorf("failed to read test file from specified folder: %w", err)
	}
	if err := os.Remove(fn); err != nil {
		return nil, fmt.Errorf("failed to remove test file from specified folder: %w", err)
	}

	q := &Queue{
		path:      path,
		prefix:    prefix,
		extension: extension,
	}

	_, err := q.oldestEntry()
	if err == io.EOF {
		q.queueEmpty = true
	} else if err != nil {
		return nil, fmt.Errorf("failed checking for oldest entry: %w", err)
	}

	return q, nil

}

// Wait returns a channel that is closed when there is something in the queue
func (q *Queue) Wait() chan struct{} {
	c := make(chan struct{})
	q.mu.Lock()
	if !q.queueEmpty {
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
	if q.queueEmpty {
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
	q.mu.Lock()
	defer q.mu.Unlock()
	err := q.put(p)
	if err == nil && q.queueEmpty {
		q.queueEmpty = false
		for _, c := range q.waiting {
			close(c)
		}
		q.waiting = q.waiting[:0]
	}
	return err
}

// Peek retrieves the oldest item from the queue (without removing it)
func (q *Queue) Peek() (io.ReadCloser, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.queueEmpty {
		return nil, queue.ErrEmpty
	}
	e, err := q.get()
	if err == io.EOF {
		q.queueEmpty = true
		return nil, queue.ErrEmpty
	}
	return e, err
}

// Dequeue removes the oldest item from the queue (without returning it)
func (q *Queue) Dequeue() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.queueEmpty {
		return queue.ErrEmpty
	}
	err := q.dequeue()
	if err == io.EOF {
		q.queueEmpty = true
		for _, c := range q.waitingForEmpty {
			close(c)
		}
		q.waitingForEmpty = q.waitingForEmpty[:0]
		return queue.ErrEmpty
	}
	return err
}

// put writes out an item to disk
func (q *Queue) put(p io.Reader) error {
	f, err := os.CreateTemp(q.path, q.prefix+"*"+q.extension)
	if err != nil {
		return err
	}

	if _, err = io.Copy(f, p); err != nil {
		f.Close()
		_ = os.Remove(f.Name()) // Attempt to remove the partial file (not much we can do if this fails)
		return err
	}
	if err = f.Close(); err != nil {
		f.Close()
		_ = os.Remove(f.Name()) // Attempt to remove the partial file (not much we can do if this fails)
		return err
	}
	return nil
}

// get() returns a ReadCloser that accesses the oldest file available
func (q *Queue) get() (io.ReadCloser, error) {
	fn, err := q.oldestEntry()
	if err != nil {
		return nil, err
	}
	f, err := os.Open(fn)
	if err != nil {
		return nil, err
	}
	return f, nil
}

// dequeue() removes the oldest file available
func (q *Queue) dequeue() error {
	fn, err := q.oldestEntry()
	if err != nil {
		return err
	}
	if err = os.Remove(fn); err != nil {
		return err
	}
	return nil
}

// oldestEntry returns the filename of the oldest entry in the queue (if any - io.EOF means none)
func (q *Queue) oldestEntry() (string, error) {
	entries, err := os.ReadDir(q.path)
	if err != nil {
		return "", fmt.Errorf("failed to read dir: %w", err)
	}

	// Search for the oldest file
	var oldFn string
	oldTime := maxTime

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		fn := entry.Name()
		if match, err := filepath.Match(q.prefix+"*"+q.extension, fn); err != nil {
			return "", fmt.Errorf("failed to read match %s: %w", fn, err)
		} else if !match {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return "", fmt.Errorf("failed to retrieve file info for %s: %w", fn, err)
		}
		fileModTime := info.ModTime()
		if !fileModTime.Before(oldTime) {
			continue
		}
		oldFn = filepath.Join(q.path, fn)
		oldTime = fileModTime
	}
	if oldTime.Equal(maxTime) {
		return "", io.EOF
	}
	return oldFn, nil
}
