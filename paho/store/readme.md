store
=====

Contains implementations of `session.storer`. These are used to store session states; that is store packets,  
packets indexed by a uint32, with the ability to retrieve in the order written.

The MQTT client will open two stores; one for PUBLISH packets (and related acknowledgements) originating from the server
(i.e. the packet identifier was assigned by the server) and the other for messages originating on the client 
(these may well share the same basic backend).

as per section 4.1 in the spec; The Session State in the Client consists of:
> · QoS 1 and QoS 2 messages which have been sent to the Server, but have not been completely acknowledged.
> · QoS 2 messages which have been received from the Server, but have not been completely acknowledged.

This means that we only need to store PUBLISH (QOS1+) related outbound packets (if we have not responded, the packet
will be resent). So the only things needed are:
 `PUBLISH` - This is the only situation where the message body is needed (so we can resend it).
 `PUBREC` - response to inbound PUBLISH. The only packet originating on the server that will be stored.
 `PUBREL` - relating to a PUBLISH we initiated
We do need to store the entire packet because, when resending it, this should include any properties.

The vast majority of operations will be writes. The only time data should be read is:
   * At startup an inventory of packets held will be needed (need to know which identifiers are allocated)
   * When connection is established, we load the packets, so they can be retransmitted

Design goals:
  - Simplicity
  - Store agnostic (allow for memory, disk files, database, REDIS etc)
  - Minimise data transfers (only read when data is needed)
  - Allow for properties (we use `packets.ControlPacket` so that a whole packet is stored; when resending we
    need to include properties etc).