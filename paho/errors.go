package paho

import "fmt"

type DisconnectError struct {
	Disconnect *Disconnect
	Err        error
}

func (d *DisconnectError) Error() string {
	return fmt.Sprintf("%s - %d: %s", d.Err, d.Disconnect.ReasonCode, d.Disconnect.Properties.ReasonString)
}
