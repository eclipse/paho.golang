package main

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Retrieve config from environmental variables

// Configuration will be pulled from the environment using the following keys
const (
	envServerURL = "pubdemo_serverURL" // server URL
	envClientID  = "pubdemo_clientID"  // client id to connect with
	envTopic     = "pubdemo_topic"     // topic to publish on
	envQos       = "pubdemo_qos"       // qos to utilise when publishing

	envKeepAlive            = "pubdemo_keepAlive"            // seconds between keep alive packets
	envConnectRetryDelay    = "pubdemo_connectRetryDelay"    // milliseconds to delay between connection attempts
	envDelayBetweenMessages = "pubdemo_delayBetweenMessages" // milliseconds delay between published messages

	envSessionFolder = "pubdemo_sessionfolder" // folder used to persist the session state (if empty state will be held in RAM)

	envPrintMessages = "pubdemo_printMessages" // If "true" then published messages will be written to the console
	envDebug         = "pubdemo_debug"         // if "true" then the libraries will be instructed to print debug info
)

// config holds the configuration
type config struct {
	serverURL *url.URL // MQTT server URL
	clientID  string   // Client ID to use when connecting to server
	topic     string   // Topic to subscribe to
	qos       byte     // QOS to use when subscribing

	keepAlive            uint16        // seconds between keepalive packets
	connectRetryDelay    time.Duration // Period between connection attempts
	delayBetweenMessages time.Duration // Period between publishing messages

	sessionFolder string // path where session state should be stored (if blank this will be held in RAM)

	printMessages bool // If true then published messages will be written to the console
	debug         bool // autopaho and paho debug output requested
}

// getConfig - Retrieves the configuration from the environment
func getConfig() (config, error) {
	var cfg config
	var err error

	srvURL, err := stringFromEnv(envServerURL)
	if err != nil {
		return config{}, err
	}
	cfg.serverURL, err = url.Parse(srvURL)
	if err != nil {
		return config{}, fmt.Errorf("environmental variable %s must be a valid URL (%w)", envServerURL, err)
	}

	if cfg.clientID, err = stringFromEnv(envClientID); err != nil {
		return config{}, err
	}
	if cfg.topic, err = stringFromEnv(envTopic); err != nil {
		return config{}, err
	}

	iQos, err := intFromEnv(envQos)
	if err != nil {
		return config{}, err
	}
	cfg.qos = byte(iQos)

	iKa, err := intFromEnv(envKeepAlive)
	if err != nil {
		return config{}, err
	}
	cfg.keepAlive = uint16(iKa)

	cfg.sessionFolder = os.Getenv(envSessionFolder)

	if cfg.connectRetryDelay, err = milliSecondsFromEnv(envConnectRetryDelay); err != nil {
		return config{}, err
	}
	if cfg.delayBetweenMessages, err = milliSecondsFromEnv(envDelayBetweenMessages); err != nil {
		return config{}, err
	}

	if cfg.printMessages, err = booleanFromEnv(envPrintMessages); err != nil {
		return config{}, err
	}
	if cfg.debug, err = booleanFromEnv(envDebug); err != nil {
		return config{}, err
	}

	return cfg, nil
}

// stringFromEnv - Retrieves a string from the environment and ensures it is not blank (ort non-existent)
func stringFromEnv(key string) (string, error) {
	s := os.Getenv(key)
	if len(s) == 0 {
		return "", fmt.Errorf("environmental variable %s must not be blank", key)
	}
	return s, nil
}

// intFromEnv - Retrieves an integer from the environment (must be present and valid)
func intFromEnv(key string) (int, error) {
	s := os.Getenv(key)
	if len(s) == 0 {
		return 0, fmt.Errorf("environmental variable %s must not be blank", key)
	}
	i, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("environmental variable %s must be an integer", key)
	}
	return i, nil
}

// milliSecondsFromEnv - Retrieves milliseconds (as time.Duration) from the environment (must be present and valid)
func milliSecondsFromEnv(key string) (time.Duration, error) {
	s := os.Getenv(key)
	if len(s) == 0 {
		return 0, fmt.Errorf("environmental variable %s must not be blank", key)
	}
	i, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("environmental variable %s must be an integer", key)
	}
	return time.Duration(i) * time.Millisecond, nil
}

// booleanFromEnv - Retrieves boolean from the environment (must be present and valid)
func booleanFromEnv(key string) (bool, error) {
	s := os.Getenv(key)
	if len(s) == 0 {
		return false, fmt.Errorf("environmental variable %s must not be blank", key)
	}
	switch strings.ToUpper(s) {
	case "TRUE", "T", "1":
		return true, nil
	case "FALSE", "F", "0":
		return false, nil
	default:
		return false, fmt.Errorf("environmental variable %s be a valid boolean option (is %s)", key, s)
	}
}
