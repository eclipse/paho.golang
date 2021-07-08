// build +unittest

package autopaho

import (
	"bytes"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
)

func TestClientConfig_buildConnectPacket(t *testing.T) {
	broker, _ := url.Parse("tcp://127.0.0.1:1883")

	config := ClientConfig{
		BrokerUrls:        []*url.URL{broker},
		KeepAlive:         5,
		ConnectRetryDelay: 5 * time.Second,
		ConnectTimeout:    5 * time.Second,

		// extends the lower-level paho.ClientConfig
		ClientConfig: paho.ClientConfig{
			ClientID: "test",
		},
	}

	// Validate initial state
	cp := config.buildConnectPacket()

	if cp.WillMessage != nil {
		t.Errorf("Expected empty Will message, got: %v", cp.WillMessage)
	}

	if cp.UsernameFlag != false || cp.Username != "" {
		t.Errorf("Expected absent/empty username, got: flag=%v username=%v", cp.UsernameFlag, cp.Username)
	}

	if cp.PasswordFlag != false || len(cp.Password) > 0 {
		t.Errorf("Expected absent/empty password, got: flag=%v password=%v", cp.PasswordFlag, cp.Password)
	}

	// Set some common parameters
	config.SetUsernamePassword("testuser", []byte("testpassword"))
	config.SetWillMessage(fmt.Sprintf("client/%s/state", config.ClientID), []byte("disconnected"), 1, true)

	cp = config.buildConnectPacket()

	if cp.UsernameFlag == false || cp.Username != "testuser" {
		t.Errorf("Expected a username, got: flag=%v username=%v", cp.UsernameFlag, cp.Username)
	}

	pmatch := bytes.Compare(cp.Password, []byte("testpassword"))

	if cp.PasswordFlag == false || len(cp.Password) == 0 || pmatch != 0 {
		t.Errorf("Expected a password, got: flag=%v password=%v", cp.PasswordFlag, cp.Password)
	}

	if cp.WillMessage == nil {
		t.Error("Expected a Will message, found nil")
	}

	if cp.WillMessage.Topic != "client/test/state" {
		t.Errorf("Will message topic did not match expected [%v], found [%v]", "client/test/state", cp.WillMessage.Topic)
	}

	if cp.WillMessage.QoS != byte(1) {
		t.Errorf("Will message QOS did not match expected [1]: found [%v]", cp.WillMessage.QoS)
	}

	if cp.WillMessage.Retain != true {
		t.Errorf("Will message Retain did not match expected [true]: found [%v]", cp.WillMessage.Retain)
	}

	if *(cp.WillProperties.WillDelayInterval) != 10 { // assumes default 2x keep alive
		t.Errorf("Will message Delay Interval did not match expected [10]: found [%v]", *(cp.Properties.WillDelayInterval))
	}

	// Set an override method for the connect packet
	config.SetConnectPacketConfigurator(func(c *paho.Connect) *paho.Connect {
		delay := uint32(200)
		c.WillProperties.WillDelayInterval = &delay
		return c
	})

	cp = config.buildConnectPacket()

	if *(cp.WillProperties.WillDelayInterval) != 200 { // verifies the override
		t.Errorf("Will message Delay Interval did not match expected [200]: found [%v]", *(cp.Properties.WillDelayInterval))
	}

}
