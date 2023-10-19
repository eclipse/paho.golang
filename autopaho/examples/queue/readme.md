Queue
===

Users often want to call `Publish` and then move on to other things on the assumption that the message will eventually 
get to the broker (even if the connection is down at the time the message is sent). This example shows how to implement
this with `autopaho`; it sends a large number of QOS1/2 messages and confirms that they are correctly received (using a 
second connection).

The main reason for writing this example was to load test the library (sending a large number of messages and, 
potentially, simulating network outages).

## MQTT V5 

Note that a linked concept is the MQTT V5 "Receive maximum" functionality. This allows the server to tell the client the
maximum number of inflight QOS1/2 messages that it will accept. The client then needs to limit inflight messages to 
comply; this may mean that the client needs to queue messages to avoid exceeding the limit. How this is configured will 
vary depending upon the broker (with Mosquitto its the `max_inflight_messages` setting and defaults to 20).

Note that, by default, a memory-based queue is used. This example demonstrates storage on disk thus enabling queued
messages to survive an application restart.

## Running with the docker Mosquitto instance and testing network outages

Use three consoles; one each for:
* Mosquitto 
* The example
* Simulating network outages

### Mosquitto

**Note**: Before running this ensure that `mosquitto.conf` contains `persistence true` (this is not in the default 
config and without it tests that include restarting the broker will fail).

Change to the `autopaho/examples/docker` folder and run `docker compose up mosquitto` this will start Mosquitto and the
log will be output. Note that logging significantly impacts performance so if you are testing that edit the 
`autopaho/examples/docker/binds/mosquitto/config/mosquitto.conf` and comment out `log_type all`.

### The example

Build and run the example (i.e. `go build` then `queue`). Note that `queue~.msg` files will be written to the current 
folder.

### Simulating Network Outages

Due to the use of QOS1/2 the test app should receive 100% of the messages sent. To confirm that everything is working
as anticipated, we can simulate network issues.

There are a variety of options:
 * Stop and start the Mosquitto container
 * Kill the network
   * Identify the network with `docker network ls` (probably called `docker_paho-test-net`)
   * Identify the mosquitto container with `docker container ls` (probably be `docker-mosquitto-1`)
   * disconnect: `docker network disconnect docker_paho-test-net docker-mosquitto-1`
   * reconnect: `docker network connect docker_paho-test-net docker-mosquitto-1`
