"""
"""
import os
import uuid

from zope.interface import implements

from twisted.internet import defer
from twisted.internet import reactor
from twisted.internet import protocol
from twisted.internet.interfaces import ILoggingContext
from twisted.application import service
from twisted.python import log

from pika.spec import BasicProperties

from anion import ianion
from anion import amqp

class NodeMessage(object):
    """
    A Node message (that is, a Message in the same logical space as a Node,
    which is above the messaging)

    """

    def __init__(self, body, nchannel, reply_to, correlation_id):
        """
        """
        self.body = body
        self.nchannel = nchannel
        self.reply_to = reply_to
        self.correlation_id = correlation_id

    def reply(self, msg):
        """
        """

    def ack(self, msg):
        """
        """


class NChannel(object):
    """
    mixture of request, transport, and amqp channel


    amqp configuration for a type of Channel
    """
    exchange = 'amq.direct'
    exchange_type = 'direct'

    def __init__(self, entity, name=None):
        """
        """
        self.entity = entity
        self.name = name
        self.pending_responses = {}

    @defer.inlineCallbacks
    def createChannel(self, chan):
        """
        chan is the actual amqp channel. The activation of an NChannel
        instance happens when the node allocates a amqp channel
        specifically for this self.entity
        This event might be revised...not sure if this is the best way.

        The successful handling of this event results in the activation of
        the entity.
        """
        self.chan = chan
        yield defer.maybeDeferred(self.configureChannel)
        defer.returnValue(self.entity.makeConnection(self))

    @defer.inlineCallbacks
    def configureChannel(self):
        """
        implement txamqp config
        do amqp configuration
        """
        #set up reply queue
        queue = yield self.chan.queue_declare(auto_delete=True, exclusive=True)
        consumer_tag = self.name + '.rpc'
        yield self.chan.queue_bind(exchange=self.exchange,
                                        routing_key=consumer_tag)
        self.chan.basic_consume(no_ack=True,
                                        consumer_tag=consumer_tag)

    def send(self, dest, msg, application_headers={},
                                content_type=None,
                                content_encoding=None,
                                message_type=None,
                                reply_to=None,
                                correlation_id=None,
                                message_id=None):
        """
        The NChannel send is the main fulcrum in this messaging system
        abstraction. The amqp message is constructed, and the publish
        arguments are determined and applied.
        """
        properties = {}
        # Only add properties if they are provided. Defaults aren't needed
        # (yet) and we don't a dict with None values.
        if application_headers:
            properties['application_headers'] = application_headers
        if content_type:
            properties['content_type'] = content_type
        if content_encoding:
            properties['content_encoding'] = content_encoding
        if message_type:
            properties['type'] = message_type
        if reply_to:
            properties['reply_to'] = reply_to
        if correlation_id:
            properties['correlation_id'] = correlation_id
        if message_id:
            properties['message_id'] = message_id
        # msg body is assumed to be properly encoded 
        self.chan.basic_publish(exchange=self.exchange, #todo  
                                routing_key=dest, #todo 
                                body=msg,
                                properties=BasicProperties(**properties),
                                immediate=False, #todo 
                                mandatory=False) #todo

    def rpc_send(self, dest, msg):
        """
        The NChannel should avoid creating a temporary reply queue for
        every rpc request. This suggests the need for NChannel types
        tailored for clients. A client NChannel will allocate a reply queue
        when it is created, and then the queue can be used for all
        interactions that take place during the life of the client NChannel
        """
        message_type = 'rpc-request' # Where should these be defined?
        reply_to = self.name + '.rpc' #XXX Needs robustification XXX 
        correlation_id = uuid.uuid4().hex
        response_deferred = defer.Deferred()
        self.pending_responses[correlation_id] = response_deferred
        # ^^ should create a more robust response thing   XXX   ^^

        self.send(dest, msg, reply_to=reply_to, 
                                    correlation_id=correlation_id,
                                    message_type=message_type)
        return response_deferred

    def receive(self, header_frame, body):
        """
        The main message received event for an Entity. The NChannel handles
        amqp details; amqp is not exposed or needed beyond the NChannel.
        The Entity doesn't know or care about amqp.
        this event then calls the entity instance receive

        The nchannel could do some sorting/routing here.
        The use of the message type property would make this easy (if all
        messages had a type / every interaction uses a certain messaging
        pattern.
        """
        props = header_frame
        #msg_type = props.get('type', None)
        msg_type = header_frame.type
        # stupid select routine:
        if msg_type == 'rpc-response':
            #correlation_id = props['correlation id']
            correlation_id = header_frame.correlation_id
            try:
                response_deferred = self.pending_responses.pop(correlation_id)
            except KeyError:
                # we aren't expecting this message, so it should be dropped
                # should it be returned to the broker?
                # should we save or report this?
                return
            response_deferred.callback(body) # XXX need to add
            # optional encoding layer that uses a Serialization registry
        else:
            self.entity.receive(body)

class Request(object):
    """
    A Node message (that is, a Message in the same logical space as a Node,
    which is above the messaging)

    This will implement INodeMessage 
    Special NodeMessages, extending the base NodeMessage if that makes
    sense to have a base NodeMessage, can be made for each different
    message pattern (Nchannel)... hopefully there wont need to be as many
    NodeMessage types as there will be NChannel types.

    This represents a request to an RPC NChannel. 
    This is a specific prototype implementation of a Node Message
    """

    def __init__(self, body, nchannel, reply_to, correlation_id):
        """
        XXX Realization!: ok, if the Node Messages delivered to the entity
        handlers bring the nchannel with them (Messages encapsulate a
        context for an interaction...) then the entity does not need to
        have an nchannel bound to it, which makes more sense right
        now...except for if the entity wants to initiate sending a
        message...then how does it do that?
        Answer: well, since it is simpler to limit a entity to one class of
        interaction (Protocol) then a client should be it's own entity
        (Client Protocol). An entity that wants to initiate an interaction
        (use a client) will need to be equipped with a client entity 
        """
        self.body = body
        self.nchannel = nchannel
        self.reply_to = reply_to
        self.correlation_id = correlation_id

    def reply(self, msg):
        """
        msg or data...
        """
        message_type = 'rpc-response' # response or reply? pick on and stick with it!
        self.nchannel.send(self.reply_to, msg,
                                    message_type=message_type,
                                    correlation_id=self.correlation_id)


class RPCChannel(NChannel):
    """
    An rpc message pattern.
    """

    def __init__(self, entity, name):
        """
        in this case, a name is required
        """
        self.entity = entity
        self.name = name
        self.logstr = self.entity.__class__.__name__ + ', ' + self.__class__.__name__

    @defer.inlineCallbacks
    def configureChannel(self):
        """
        This is called to configure the channel. 
        Returns a deferred that callsback when the configuration succeeds,
        or errs back if the configuration fails.

        XXX need to build in Error handling for deferred events like this!!
        set up consumer listening on our name

        this could optionally resolve a managed name, using a resolver
        function/service

        The exchange type and name should be managed in a level above this.

        The amqclient consumer queue could be replaced with a smart queue
        that can have an optional callback registered with it.
        That way, if there is a callback, the basic_deliver event directly
        propagates to the receive event of the entity; if there is no
        callback, the message is queued in the deferred queue.
        """
        yield self.chan.queue_declare(queue=self.name, exclusive=True, auto_delete=True)
        yield self.chan.queue_bind(queue=self.name, exchange='amq.direct',
                                                    routing_key=self.name)
        self.chan.basic_consume(queue=self.name, no_ack=True,
                                                    consumer_tag=self.name)

    def receive(self, header_frame, body):
        """
        simplest implementation: one request at a time
        """
        props = header_frame
        msg_type = props.type
        if msg_type == 'rpc-request':
            # should a Entity Request Message be constructed for this
            # interaction?
            reply_to = props.reply_to # XXX message format enforcement needed
            correlation_id = props.correlation_id
            request = Request(body, self, reply_to, correlation_id)
            self.entity.receive(request)

class Node(amqp.AMQPClientFactory):
    """
    Implement the container (or factory aspect) in amqp.AMQPClientFactory
    and the manager aspect here, as Node.
    """

    def __init__(self, username='guest', password='guest', vhost='/'):
        """
        The root application could be some entity container/node mapper
        (mapping endpoints to different implementations of messaging names)
        """
        amqp.AMQPClientFactory.__init__(self, None)
        self.entities = {} # the resources
        self.nchannels = {} # the protocol/transports (not exactly amqp chan)

    def nodeStart(self):
        """
        The activation of the node should happen when the amqp connection
        succeeds in connecting.
        """
        for name in self.entities:
            self.startEntity(name)

    def nodeStop(self):
        """
        """
        for name in self.entities:
            self.stopEntity(name)

    def connectionLost(self, reason):
        """
        When the broker connection closes (amqp conn)
        Fatal situation, all entities need to die without calling
        stopEntity. (because the channels were already closed!)
        Everything can be restarted if a new connection is made.

        What is the best way to clean up all the nchannels, memory wise?

        The nchannels could be notified, and go into their initial state.
        Client entities need the ability to exist before the node/nchan is
        ready...
        """

    def deliverMessage(self, chan, method_frame, header_frame, body):
        """
        get entity nchannel by name and invoke its receive event
        """
        name = method_frame.consumer_tag.split('.')[0]
        nchan = self.nchannels[name]
        nchan.receive(header_frame, body) # simplest first go

    def addEntity(self, name, entity, nChannel):
        """
        name is the messaging name to bind to
        entity is an object providing the IMessagingEntity interface
        nChannel is the class of NChannel to use
        
        More parameters can be attributed to the entity as it runs in the
        node:
         - identity
         - policy (what it can do in the node...)
        """
        if name in self.entities:
            raise KeyError("Entity named %s already exists" % (name,))
        self.entities[name] = [entity, nChannel]
        if self.connected:
            self.startEntity(name)

    def removeEntity(self, name):
        """
        """
        self.stopEntity(name)
        del self.entities[name]

    def startEntity(self, name):
        """
        or start entity? (getting close to spawn..)
        name must be unique
        nchannels are like the sockets for each entity.
        an entity can have only one nchan
        nchans should be deleted when a entity is stopped 
        if the amqclient uses the consumer_tag to select a nchan from the
        manager, consumers map directly to nchans
        but an amqp channel can have many consumers, so consumer_tags are
        useful for organizing named endpoints within the context of a
        amqp channel, 
        so, either the manager has to keep track of each entities named
        endpoints (consumer_tags),...

        """
        if name in self.nchannels:
            return

        def start_ok(result):
            """
            store this entities nchan in our nchannels dict
            """
            self.nchannels[name] = nchan
            return True

        def start_fail(reason):
            """entity nchan not added to nchan dict
            """
            reason.printTraceback()
            return reason
        # starting results in a new channel/connector/consumer... object
        # the channel connector object thing represents state within the
        # amqp client.
        # The creation of the channel connector is a lower level node
        # container method.
        # The container is passed some config parameters (name, ...) and a
        # callback function (the channel connector object itself..). The
        # actual amqp channels and consumer names remain in the client.
        # The entity can send messages with the channel connector, and
        # maybe potentially effect its messaging config/usage after it is
        # started in the Node (or even turn itself off/remove itself from
        # the ndode)
        entity, nChannel = self.entities[name]
        nchan = nChannel(entity, name)
        d = self.bind_nchannel(nchan) #this is a deferred operation!
        # should node.bind_nchannel return  deferred, or return something,
        # as reactor does, and then notification of failure can happen
        # through an even chain (and not directly via callbacks here)?
        d.addCallbacks(start_ok, start_fail)

    def stopEntity(self, name):
        """
        XXX amqp note:
        calling channel_close on an already closed channel raises a channel
        error
        """
        if name not in self.entities:
            raise KeyError("Unrecognized entity name: %s" % (name,))
        # close channel, cancel consumer, unbind name, (free resources)
        if not self.nchannels.has_key(name):
            return
        nchan = self.nchannels[name]
        d = nchan.chan.channel_close() # Now, when the channel tells us it
        # closed, del this nchan. Oh! this gives us a clue on who should
        # have the nchan and the chan!

        def close_ok(result):
            del self.nchannels[name]

        d.addCallback(close_ok)
        return d



def test():
    node = Node()
    reactor.connectTCP('localhost', 5672, node)
    return node



if __name__ == '__main__':
    test()
    reactor.run()






