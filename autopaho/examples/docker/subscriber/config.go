package main

import (
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

// Retrieve config from environmental variables

// Configuration will be pulled from the environment using the following keys
const (
	envServerURL = "subdemo_serverURL" // server URL
	envClientID  = "subdemo_clientID"  // client id to connect with
	envTopic     = "subdemo_topic"     // topic to publish on
	envQos       = "subdemo_qos"       // qos to utilise when publishing

	envKeepAlive         = "subdemo_keepAlive"         // seconds between keepalive packets
	envConnectRetryDelay = "subdemo_connectRetryDelay" // milliseconds to delay between connection attempts

	envWriteToStdOut = "subdemo_writeToStdout" // if "true" then received packets will be written stdout
	envWriteToDisk   = "subdemo_writeToDisk"   // if "true" then received packets will be written to file
	envOutputFile    = "subdemo_OutputFile"    // name of file to use if above is true

	envDebug = "subdemo_debug" // if "true" then the libraries will be instructed to print debug info
)

// config holds the configuration
type config struct {
	serverURL *url.URL // MQTT server URL
	clientID  string   // Client ID to use when connecting to server
	topic     string   // Topic on which to publish messaged
	qos       byte     // QOS to use when publishing

	keepAlive         uint16        // seconds between keepalive packets
	connectRetryDelay time.Duration // Period between connection attempts

	writeToStdOut  bool   // If true received messages will be written to stdout
	writeToDisk    bool   // if true received messages will be written to below file
	outputFileName string // filename to save messages to

	debug bool // autopaho and paho debug output requested
}

// getConfig - Retrieves the configuration from the environment
func getConfig() (config, error) {
	exeDir, _ := os.Executable()
	_ = godotenv.Load(path.Join(filepath.Dir(exeDir), ".env")) // Load environment variables from .env file (will not override existing variables)

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

	if cfg.connectRetryDelay, err = milliSecondsFromEnv(envConnectRetryDelay); err != nil {
		return config{}, err
	}

	if cfg.writeToStdOut, err = booleanFromEnv(envWriteToStdOut); err != nil {
		return config{}, err
	}
	if cfg.writeToDisk, err = booleanFromEnv(envWriteToDisk); err != nil {
		return config{}, err
	}
	if cfg.outputFileName, err = stringFromEnv(envOutputFile); cfg.writeToDisk && err != nil {
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
