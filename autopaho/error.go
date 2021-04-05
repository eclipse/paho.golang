package autopaho

import (
	"sync"

	"github.com/ChIoT-Tech/paho.golang/paho"
)

// errorHandler provides the onClientError callback function that will be called by the Paho library. The sole aim
// of this is to pass a single error onto the error channel (the library may send multiple errors; only the first
// will be processed).
type errorHandler struct {
	debug paho.Logger

	mu      sync.Mutex
	errChan chan error // receives connection errors

	userFun func(error) // User provided onClientError function
}

// onClientError called by the paho library when an error occurs. We assume that the error is always fatal
func (e *errorHandler) onClientError(err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.errChan != nil {
		e.debug.Printf("received error: %s", err)
		e.errChan <- err
		e.errChan = nil
	} else {
		e.debug.Printf("received extra error: %s", err)
	}
	if e.userFun != nil {
		go e.userFun(err)
	}
}
