package autopaho

import (
	"fmt"
	"sync"

	"github.com/eclipse/paho.golang/paho"
)

// errorHandler provides the onClientError callback function that will be called by the Paho library. The sole aim
// of this is to pass a single error onto the error channel (the library may send multiple errors; only the first
// will be processed).
type errorHandler struct {
	debug paho.Logger

	mu      sync.Mutex
	errChan chan error // receives connection errors

	userOnClientError      func(error)            // User provided onClientError function
	userOnServerDisconnect func(*paho.Disconnect) // User provided OnServerDisconnect function
}

// onClientError called by the paho library when an error occurs. We assume that the error is always fatal
func (e *errorHandler) onClientError(err error) {
	e.handleError(err)
	if e.userOnClientError != nil {
		go e.userOnClientError(err)
	}
}

// onClientError called by the paho library when the server requests a disconnection (for example as part of a
// clean broker shutdown). We want to begin attempting to reconnect when this occurs (and pass a detectable error
// to the user)
func (e *errorHandler) onServerDisconnect(d *paho.Disconnect) {
	e.handleError(&DisconnectError{err: fmt.Sprintf("server requested disconnect (reason: %d)", d.ReasonCode)})
	if e.userOnServerDisconnect != nil {
		go e.userOnServerDisconnect(d)
	}
}

// handleError ensures that only a single error is sent to the channel (all errors go to the users OnClientError function)
func (e *errorHandler) handleError(err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.errChan != nil {
		e.debug.Printf("received error: %s", err)
		e.errChan <- err
		e.errChan = nil
	} else {
		e.debug.Printf("received extra error: %s", err)
	}
}

// DisconnectError will be passed when the server requests disconnection (allows this error type to be detected)
type DisconnectError struct{ err string }

func (d *DisconnectError) Error() string {
	return d.err
}
