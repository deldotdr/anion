"""
AMQP Messaging Client Protocol 

These are extensions and fixes to the txamqp library
"""

import os
from time import time

from twisted.internet import defer
from twisted.internet import protocol
from twisted.python import log

from anion.pika_adapter import PikaAdapter
from anion.pika_adapter import TwistedConnection


class DeferredChannelAdapter(object):

    def __init__(self, channel, client):
        self.channel = channel
        self.client = client

    def exchange_declare(self, **kw):
        d = defer.Deferred()
        self.channel.exchange_declare(callback=d.callback, **kw)
        return d

    def exchange_delete(self, **kw):
        d = defer.Deferred()
        self.channel.exchange_delete(callback=d.callback, **kw)
        return d

    def exchange_bind(self, **kw):
        d = defer.Deferred()
        self.channel.exchange_bind(callback=d.callback, **kw)
        return d

    def exchange_unbind(self, **kw):
        d = defer.Deferred()
        self.channel.exchange_unbind(callback=d.callback, **kw)
        return d

    def queue_declare(self, **kw):
        d = defer.Deferred()
        self.channel.queue_declare(callback=d.callback, **kw)
        return d

    def queue_bind(self, **kw):
        d = defer.Deferred()
        self.channel.queue_bind(callback=d.callback, **kw)
        return d

    def queue_purge(self, **kw):
        d = defer.Deferred()
        self.channel.queue_purge(callback=d.callback, **kw)
        return d

    def queue_delete(self, **kw):
        d = defer.Deferred()
        self.channel.queue_delete(callback=d.callback, **kw)
        return d

    def queue_unbind(self, **kw):
        d = defer.Deferred()
        self.channel.queue_unbind(callback=d.callback, **kw)
        return d

    def basic_qos(self, **kw):
        d = defer.Deferred()
        self.channel.basic_qos(callback=d.callback, **kw)
        return d

    def basic_get(self, **kw):
        d = defer.Deferred()
        self.channel.basic_get(callback=d.callback, **kw)
        return d

    def basic_ack(self, **kw):
        return self.channel.basic_ack(**kw)

    def basic_reject(self, **kw):
        return self.channel.basic_reject(**kw)

    def basic_recover_async(self, **kw):
        return self.channel.basic_recover_async(**kw)

    def basic_recover(self, **kw):
        d = defer.Deferred()
        self.channel.basic_recover(callback=d.callback, **kw)
        return d

    def tx_select(self):
        d = defer.Deferred()
        self.channel.tx_select(callback=d.callback)
        return d

    def tx_commit(self):
        d = defer.Deferred()
        self.channel.tx_commit(callback=d.callback)
        return d

    def tx_rollback(self):
        d = defer.Deferred()
        self.channel.tx_rollback(callback=d.callback)
        return d

    def basic_consume(self, **kw):
        return self.channel.basic_consume(self.client.factory.deliverMessage, **kw)

    def basic_publish(self, *args, **kw):
        return self.channel.basic_publish(*args, **kw)

class AMQClient(PikaAdapter):
    """
    AMQP Client enhanced for interaction.
    """

    def connectionOpened(self, a):
        self.factory.connectClient(self)

    def channel(self):
        """
        Create a new channel and open it
        """
        d = defer.Deferred()
        self.pika_connection.channel(d.callback)
        def cb(chan, client):
            return DeferredChannelAdapter(chan, client)
        d.addCallback(lambda chan: DeferredChannelAdapter(chan, self))
        return d

class PikaClientFactory(protocol.ClientFactory):
    """
    """
    protocol = AMQClient

    def __init__(self, parameters=None):
        """
        - parameters: pika ConnectionParameters
        """
        self.parameters = parameters


    def buildProtocol(self, addr):
        pika_connection = TwistedConnection(self.parameters)
        proto = self.protocol(pika_connection)
        proto.factory = self
        return proto


class AMQPClientFactory(PikaClientFactory):
    """
    The factory uses the connected protocol for as long as the connection
    lasts. This factory should not be connected with reactor.connectTCP
    more than once per instance.
    """

    connected = 0

    def clientConnectionFailed(self, connector, reason):
        """
        """

    def clientConnectionLost(self, connector, reason):
        """
        """
        self.connector = connector
        #self.manager.stopService() # or deactivate?
        #self.nodeStop()

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
        self.client.close() # might trigger on_close_callback

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
        d = self.client.channel() # create a new amqp channel for nchannel to use
        d.addCallback(nchannel.createChannel)
        return d

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


