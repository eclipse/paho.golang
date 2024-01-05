Router Example
===

AutoPaho provides the option to specify callbacks (`ClientConfig.OnPublishReceived`) that will be called everytime
a message is received. It's fairly common for users to want multiple callbacks with the message topic determining which
callback is called. Routers can provide this functionality.

To use them first create a router:

```
router := paho.NewStandardRouter()
```

Configure `ClientConfig.OnPublishReceived` so the router is called:

```go
autopaho.ClientConfig{
    OnPublishReceived: []func (paho.PublishReceived) (bool, error){
    func (pr paho.PublishReceived) (bool, error) {
        router.Route(pr.Packet.Packet())
        return true, nil // we assume that the router handles all messages (todo: amend router API)
    }},
}
```

Now you can add/remove routes: 

```
router.DefaultHandler(func(p *paho.Publish) { fmt.Printf("defaulthandler received message with topic: %s\n", p.Topic) })
router.RegisterHandler("test/test/#", func(p *paho.Publish) { fmt.Printf("test/test/# received message with topic: %s\n", p.Topic) })
router.UnregisterHandler("test/test/#")
```
