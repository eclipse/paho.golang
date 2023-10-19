package memory

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/autopaho/queue"
)

// TestMemoryQueue some basic tests of the queue
func TestMemoryQueue(t *testing.T) {
	q := New()

	if _, err := q.Peek(); !errors.Is(err, queue.ErrEmpty) {
		t.Errorf("expected ErrEmpty, got %s", err)
	}

	if err := q.Dequeue(); !errors.Is(err, queue.ErrEmpty) {
		t.Errorf("expected ErrEmpty, got %s", err)
	}

	queueNotEmpty := make(chan struct{})
	go func() {
		<-q.Wait()
		close(queueNotEmpty)
	}()
	time.Sleep(time.Nanosecond) // let go routine run
	select {
	case <-queueNotEmpty:
		t.Fatalf("Wait should not return until something is in queue")
	default:
	}
	testEntry := []byte("This is a test")
	if err := q.Enqueue(bytes.NewReader(testEntry)); err != nil {
		t.Fatalf("error adding to queue: %s", err)
	}
	select {
	case <-queueNotEmpty:
	case <-time.After(time.Second):
		t.Fatalf("Wait should return when something is in queue")
	}

	const entryFormat = "Queue entry %d for testing"
	for i := 0; i < 10; i++ {
		if err := q.Enqueue(bytes.NewReader([]byte(fmt.Sprintf(entryFormat, i)))); err != nil {
			t.Fatalf("error adding entry %d: %s", i, err)
		}
	}
	if err := q.Dequeue(); err != nil {
		t.Fatalf("error dequeue entry: %s", err)
	}

	for i := 0; i < 10; i++ {
		r, err := q.Peek()
		if err != nil {
			t.Fatalf("error peeking entry %d: %s", i, err)
		}
		buf := &bytes.Buffer{}
		if _, err = buf.ReadFrom(r); err != nil {
			t.Fatalf("error reading entry %d: %s", i, err)
		}
		if err = r.Close(); err != nil {
			t.Fatalf("error closing queue entry %d: %s", i, err)
		}

		expected := []byte(fmt.Sprintf(entryFormat, i))
		if bytes.Compare(expected, buf.Bytes()) != 0 {
			t.Fatalf("expected \"%s\", got \"%s\"", expected, buf.Bytes())
		}
		if err = q.Dequeue(); err != nil {
			t.Fatalf("error dequeue entry %d: %s", i, err)
		}
	}

	if _, err := q.Peek(); !errors.Is(err, queue.ErrEmpty) {
		t.Errorf("expected ErrEmpty, got %s", err)
	}

	if err := q.Dequeue(); !errors.Is(err, queue.ErrEmpty) {
		t.Errorf("expected ErrEmpty, got %s", err)
	}
}
