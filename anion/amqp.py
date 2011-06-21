"""
AMQP Messaging Client Protocol 

These are extensions and fixes to the txamqp library
"""

import os
from time import time

from twisted.internet import defer
from twisted.internet import protocol

from txamqp import spec
from txamqp.content import Content
from txamqp.client import TwistedDelegate
from txamqp.protocol import AMQClient, Frame
from txamqp.queue import TimeoutDeferredQueue

import anion
SPEC_PATH = os.path.join(anion.__path__[0], 'amqp0-8.xml')


class AMQPEvents(TwistedDelegate):
    """
    AMQP methods the broker calls on the client.

    These handlers are for asynchronous methods (so, responses to
    synchronous methods can't be intercepted here).
    """

    def close(self, reason):
        """
        We can do things here when the connection closes
        """
        return TwistedDelegate.close(self, reason)

    def basic_return(self, chan, msg):
        """
        The node can decide how to handle returned messages. Probably, the
        entity that sent it can have an error event called on it (if it
        still exists!)
        """
        self.client.basic_return_queue.put(msg)

    def basic_deliver(self, chan, msg):
        """
        This propagates into the node, where it can be routed to the
        endpoint entity based on a context built upon a channel.
        """
        self.client.factory.deliverMessage(msg)

    def channel_flow(self, chan, msg):
        """
        This could throttle the actual client protocol and/or trigger a
        node flow control event, which different entities could respond to
        in their own way..
        """

    def channel_alert(self, chan, msg):
        """
        This should propagete into the general node messaging, as a node
        defined alert event.
        """


class AMQPProtocol(AMQClient):
    """
    """

    next_channel_id = 0

    def channel(self, id=None):
        """Overrides AMQClient. Changes: 
            1) no need to return deferred. The channelLock doesn't protect
            against any race conditions; the channel reference is returned,
            so any number of those references could exist already. 
            2) auto channel numbering
            3) replace deferred queue for basic_deliver(s) with simple
               buffer(list)
        """
        if id is None:
            self.next_channel_id += 1
            id = self.next_channel_id
        try:
            ch = self.channels[id]
        except KeyError:
            # XXX The real utility is in the exception body; is that good
            # style?
            ch = self.channelFactory(id, self.outgoing)
            # the PacketDelegate defined above requires this buffer
            self.channels[id] = ch
        return ch

    def queue(self, key):
        """channel basic_deliver queue
        overrides AMQClient
            1) no need to be deferred
        """
        try:
            q = self.queues[key]
        except KeyError:
            q = TimeoutDeferredQueue()
            self.queues[key] = q
        return q

    def connectionMade(self):
        """
        authenticate and start the Node 
        """
        AMQClient.connectionMade(self)
        username = self.factory.username
        password = self.factory.password
        # authentication should happen automatically, and fail asap
        # XXX need to know how it can fail still
        d = self.authenticate(username, password)
        d.addCallback(self._auth_result)
        d.addErrback(self._auth_fail)

    def _auth_result(self, result):
        #self.factory.connectClient(self)
        return result

    def _auth_fail(self, reason):
        #print '_auth_fail', #XXX do something better here
        reason.printTraceback() 

    def processFrame(self, frame):
        ch = self.channel(frame.channel)
        if frame.payload.type == Frame.HEARTBEAT:
            self.lastHBReceived = time()
        else:
            ch.dispatch(frame, self.work)
        if self.heartbeatInterval > 0:
            self.reschedule_checkHB()

    @defer.inlineCallbacks
    def start(self, response, mechanism='AMQPLAIN', locale='en_US'):
        self.response = response
        self.mechanism = mechanism
        self.locale = locale

        # XXX To get rid of this, turn start into an event.
        yield self.started.wait()
        channel0 = self.channel(0)

        def handle_open_ok(result):
            self.connectionOpened()

        def handle_open_error(reason):
            reason.printTraceback()
            return reason

        d = channel0.connection_open(self.vhost)
        d.addCallback(handle_open_ok)
        d.addErrback(handle_open_error)

    def connectionOpened(self):
        """
        This event let's us know the entire AMQP start up sequence is
        complete, a connection into a vhost has opened, and the client 
        can is ready to be used.
        """
        self.factory.connectClient(self)
   

class AMQPClientFactory(protocol.ClientFactory):
    """
    The factory uses the connected protocol for as long as the connection
    lasts. This factory should not be connected with reactor.connectTCP
    more than once per instance.
    """
    protocol = AMQPProtocol
    delegate = AMQPEvents

    connected = 0

    def __init__(self, username='guest', password='guest', vhost='/'):
        """
        """
        self.username = username
        self.password = password
        self.vhost = vhost
        self.spec = spec.load(SPEC_PATH)

    def buildProtocol(self, addr):
        delegate = self.delegate() #XXX
        p = self.protocol(delegate, self.vhost, self.spec)
        p.factory = self
        return p

    def clientConnectionFailed(self, connector, reason):
        """
        """

    def clientConnectionLost(self, connector, reason):
        """
        """
        self.connector = connector
        #self.manager.stopService() # or deactivate?
        #self.nodeStop()
        #self.manager.connectionLost(reason)

    def connectClient(self, client):
        """
        Event that the amqp client calls when login to the broker succeeds.
        """
        self.connected = 1
        self.client = client
        self.nodeStart()
        # It is simpler to chain the life cycle states of the messaging and
        # the node manager. i.e. the manager can only start when the
        # messaging is up, and if the messaging goes down, the manager is
        # stopped OR the manager can be used to take down the messaging

    def shutdown_client(self):
        """amqp level Connection Class close command.
        The success of this triggers the TCP transport close, which will
        fire the NodeContainer clientConnectionLost event
        """
        def close_ok(result):
            """The result is not needed. The fact of success is sufficient.
            """
            return result

        def close_err(reason):
            reason.printTraceback()
            return reason

        ch0 = self.client.channel(0)
        d = ch0.connection_close()
        d.addCallbacks(close_ok, close_err)
        return d

    def bind_nchannel(self, nchannel):
        """
        or attach_nchannel ?
        similar to reactor connect or listen or spawn

        similar to binding/connecting a socket
         - a listener/consumer/service will bind to a known name
         - a client/producer may use an anonymous name

        So then, every entity has a unique local name, which maps 1to1 with
        amqp channel as; the name is an identifier for an amqp channel. an
        amqp channel is a context for 0, 1, or many consumers, which
        themselves have a consumer_tag name. consumer_tags have to be
        unique with in a connection, but each consumer in a channel is
        owned and managed by only one channel.

        If an entity is an anonymous client, it's name is uniquely
        anonymous; a queue and/or consumer may not even be created for that
        entity.
        If an entity is a service, the name it represent can only be used
        once in a Node (locally unique), but many Nodes could also host
        entities of that same name.

        NChannel is a data structure describing the messaging behavior
        NChannel is also an object that has functionality to...
         - effect the messaging configuration?
         - carry out the subsequent operation

        This can/will only be called when the messaging client is up.
        This will fail if self.client is set
        """
        chan = self.client.channel() # create a new amqp channel for nchannel to use
        # nchannel has to open it's channel
        return nchannel.createChannel(chan) #XXX what to do if this fails?

    def channel(self):
        """
        Allocate a new channel with the client, and return it for use in
        the Node.
        This only works when the client is active, so it will only be
        called by the Node when it's in an active state.
        """
 
    def deliverMessage(self, msg):
        """
        Implement this in a subclass
        """


