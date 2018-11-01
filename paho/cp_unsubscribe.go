package paho

import "github.com/eclipse/paho.golang/packets"

type (
	// Unsubscribe is a representation of an MQTT unsubscribe packet
	Unsubscribe struct {
		Topics     []string
		Properties *UnsubscribeProperties
	}

	// UnsubscribeProperties is a struct of the properties that can be set
	// for a Unsubscribe packet
	UnsubscribeProperties struct {
		User map[string]string
	}
)

// Packet returns a packets library Unsubscribe from the paho Unsubscribe
// on which it is called
func (u *Unsubscribe) Packet() *packets.Unsubscribe {
	return &packets.Unsubscribe{
		Topics: u.Topics,
		Properties: &packets.Properties{
			User: u.Properties.User,
		},
	}
}
