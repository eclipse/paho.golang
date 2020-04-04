package paho

import "github.com/netdata/paho.golang/packets"

type LogLevel byte

const (
	_ LogLevel = iota
	LevelTrace
	LevelDebug
	LevelWarn
	LevelError
)

type LogEntry struct {
	Level         LogLevel
	Message       string
	Error         error
	ControlPacket *packets.ControlPacket
}

func (c *Client) log(level LogLevel, msg string, opts ...func(*LogEntry)) {
	fn := c.Logger
	if fn == nil {
		return
	}
	e := LogEntry{
		Level:   level,
		Message: msg,
	}
	for _, opt := range opts {
		opt(&e)
	}
	fn(e)
}
