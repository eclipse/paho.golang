Eclipse Paho MQTT Go client
============================

This repository contains the source code for the [Eclipse Paho](http://eclipse.org/paho) MQTT Go client library.

Installation and Build
----------------------

This client is designed to work with the standard Go tools, so installation is as easy as:

```bash
go get github.com/eclipse/paho.golang
```

Folder Structure
----------------

The main library is in the `paho` folder (so for general usage `import "github.com/eclipse/paho.golang/paho"`). There are 
examples off this folder in `paho/cmd` and extensions in `paho/extensions`.

`autopaho` (`import "github.com/eclipse/paho.golang/autopaho"`) is a fairly simple wrapper that automates the connection 
process (`mqtt` and `mqtts`) and will automatically reconnect should the connection drop. For many users this package
will provide a simple way to connect and publish/subscribe as well as demonstrating how to use the `paho.golang/paho`.
`autopaho/examples/docker` provides a full example using docker to run a publisher and subscriber (connecting to 
mosquitto).  


Reporting bugs
--------------

Please report bugs by raising issues for this project in github [https://github.com/eclipse/paho.golang/issues](https://github.com/eclipse/paho.golang/issues)

More information
----------------

Discussion of the Paho clients takes place on the [Eclipse paho-dev mailing list](https://dev.eclipse.org/mailman/listinfo/paho-dev).

General questions about the MQTT protocol are discussed in the [MQTT Google Group](https://groups.google.com/forum/?hl=en-US&fromgroups#!forum/mqtt).

There is much more information available via the [MQTT community site](http://mqtt.org).
