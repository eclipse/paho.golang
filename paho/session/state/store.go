package state

import (
	"io"
)

// storer must be implemented by session state stores
type storer interface {
	Put(packetID uint16, packetType byte, w io.WriterTo) error // Store the packet
	Get(packetID uint16) (io.ReadCloser, error)                // Retrieve the packet with the specified in ID
	Delete(id uint16) error                                    // Removes the message with the specified store ID

	// Quarantine sets the message with the specified store ID into an error state; this may mean deleting it or storing
	// it somewhere separate. This is intended for use when a corrupt packet is detected (as this may result in data
	// loss, it's beneficial to have access to corrupt packets for analysis).
	Quarantine(id uint16) error

	List() ([]uint16, error) // Returns packet IDs in the order they were Put
	Reset() error            // Clears the store (deleting all messages)
}
