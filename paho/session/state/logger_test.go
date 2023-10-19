package state

import "sync"

// logger_test implements a logger than can be passed a testing.T (which will only output logs for failed tests)

// testLogger contains the logging functions provided by testing.T
type testLogger interface {
	Log(args ...interface{})
	Logf(format string, args ...interface{})
}

// The testLog type is an adapter to allow the use of testing.T as a paho.Logger.
// With this implementation, log messages will only be output when a test fails (and will be associated with the test).
type testLog struct {
	sync.Mutex
	l      testLogger
	prefix string
}

// Println prints a line to the log
// Println its arguments in the test log (only printed if the test files or appropriate arguments passed to go test).
func (t *testLog) Println(v ...interface{}) {
	t.Lock()
	defer t.Unlock()
	if t.l != nil {
		t.l.Log(append([]interface{}{t.prefix}, v...)...)
	}
}

// Printf formats its arguments according to the format, analogous to fmt.Printf, and
// records the text in the test log (only printed if the test files or appropriate arguments passed to go test).
func (t *testLog) Printf(format string, v ...interface{}) {
	t.Lock()
	defer t.Unlock()
	if t.l != nil {
		t.l.Logf(t.prefix+format, v...)
	}
}

// Stop prevents future logging
func (t *testLog) Stop() {
	t.Lock()
	defer t.Unlock()
	t.l = nil
}
