"""
AMQP Messaging Client Protocol 

These are extensions and fixes to the txamqp library
"""

import os
from time import time

from twisted.internet import defer
from twisted.internet import protocol
from twisted.python import log

from pika.adapters import twisted_connection



class AMQPClientFactory(twisted_connection.PikaClientFactory):
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
        log.msg(self.client)
        d = self.client.channel() # create a new amqp channel for nchannel to use
        log.msg(d)
        # nchannel has to open it's channel
        d.addCallback(nchannel.createChannel)
        #return nchannel.createChannel(chan) #XXX what to do if this fails?
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


