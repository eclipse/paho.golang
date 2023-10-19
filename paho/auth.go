package paho

// Auther is the interface for something that implements the extended authentication
// flows in MQTT v5
type Auther interface {
	Authenticate(*Auth) *Auth // Authenticate will be called when an AUTH packet is received.
	Authenticated()           // Authenticated will be called when CONNACK is received
}
