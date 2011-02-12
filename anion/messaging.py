"""
Use txrabbitmq as a library.
Expose the txrabbitmq twisted service over different network protocols.
"""

import os
import uuid
from time import time

try:
    import json
except ImportError:
    import simplejson as json

from zope.interface import implements

from twisted.internet import defer
from twisted.internet import reactor
from twisted.internet import protocol
from twisted.internet.interfaces import ILoggingContext
from twisted.application import service

from txamqp import spec
from txamqp.content import Content
from txamqp.client import TwistedDelegate
from txamqp.protocol import AMQClient, Frame
from txamqp.queue import TimeoutDeferredQueue

import anion
from anion import ianion
SPEC_PATH = os.path.join(anion.__path__[0], 'amqp0-8.xml')


class Entity(object):
    """
    Messaging Entity (or Process)

    The receive method is to be implemented by the application.
    process is like render. process doesn't always have to be like a
    request response. 

    This could eventually be like a gen analog. This first try will be like
    a server. 
    """

    def makeConnection(self, nchannel):
        """
        event that occurs when Entity is bound into messaging
        """
        self.nchannel = nchannel # need a better name
        self.connectionMade()

    def connectionMade(self):
        """
        implement if you want something to happen when the entity is ready
        """

    def receive(self, msg):
        """
        Main event to handle a received message (like render).

        Messages are delivered here because they are addressed to the name
        this handler is bound to.
        """

class NodeDelegate(TwistedDelegate):
    """
    AMQP methods the broker calls on the client
    """

    def close(self, reason):
        """
        We can do things here when the connection closes
        """
        #print "CLOSE"
        return TwistedDelegate.close(self, reason)

    def basic_return(self, chan, msg):
        self.client.basic_return_queue.put(msg)

    def basic_deliver(self, chan, msg):
        """
        the delegate delegates...
        forward to node container deliver
        """
        self.client.factory.deliverMessage(msg) # factory can translate amqp msg
        #to node msg object. msg.consumer_tag is used to route to corrent
        #entity

class MessagingProtocol(AMQClient):
    """Extends txamqp client Protocol
    Fixes behavior of some methods implemented in original txamqp protocol.
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
        # It's possible an authentication rejection could be a callBack,
        # and not an errback. need to understand that
        #print '_auth_result', result #XXX do something better here
        self.factory.connectClient(self)

    def _auth_fail(self, reason):
        """
        handle broker authorization failure
        """
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
    def bind_peer(self, name, peer):
        """
        get this to happen somehow, and then give the final deferred
        callback an event to fire 
        """
        chan = self.channel()
        yield chan.channel_open()
        yield chan.queue_declare(queue=name, exclusive=True, auto_delete=True)
        yield chan.queue_bind(queue=name, exchange='amq.direct',
                routing_key=name)
        yield chan.basic_consume(queue=name, no_ack=True, consumer_tag=name)
        queue = self.queue(name)
        d = queue.get()
        def queue_eater(msg, queue):
            """temp/place holder 
            """
            peer.receive(msg)
            d = queue.get()
            d.addCallback(queue_eater, queue)
            return d
        d.addCallback(queue_eater, queue)

class NChannel(object):
    """
    mixture of request, transport, and amqp channel

    NChannel might get the point across the best.

    amqp configuration for a style of messaging


    an interface that basic_deliver can call
    might want to use the IConsumer interface

    an object that holds an entity's send capability

    an amqp channel can have multiple consumers, so an entity can have
    multiple consumers, and consumers shouldn't be shared between entities.
    This way, a running entity corresponds to an amqp channel this is open
    for the duration of the entitys life. an entity only ever uses one
    channel.
    If it binds multiple consumers, it must be able to deal with the
    different deliveries, which will all be given to the same entity receive
    handler.

    Does an NChannel maintain a communication context like a connection
    between two endpoints, or between an endpoint and some 'space' (exchange)?
    """

    implements(ILoggingContext)

    def __init__(self, entity, name=None):
        """
        different patterns may or may not require a given name

        this is like the Transport object
        and chan is like the socket 

        instead of queueing write events with the reactor, as Transport
        does rather than writing directly to the socket, here we can write
        directly to chan because we know chan will never block (contrast
        this with how Transport can not write directly to its socket
        because the socket could block). 
        """
        self.entity = entity
        self.name = name
        self.pending_responses = {}
        self.logstr = self.entity.__class__.__name__ + ', ' + self.__class__.__name__

    def logPrefix(self):
        return self.logstr

    @defer.inlineCallbacks
    def createChannel(self, chan):
        """
        chan is the actual amqp channel. The activation of an NChannel
        instance happens when the node allocates a amqp channel
        specifically for this self.entity
        This event might be revised...not sure if this is the best way.

        The successful handling of this event results in the activation of
        the entity.
        The caller of this event doesn't care if this about the Deferred
        returned here. inlineCallbacks are used to simplify the
        configuration steps. 
        Failures raised as exceptions here need to be captured and the
        shutdown procedure should be initiated by calling the (?) event
        """
        self.chan = chan
        yield chan.channel_open()
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
        yield self.chan.queue_bind(exchange='amq.direct',
                                        routing_key=consumer_tag)
        yield self.chan.basic_consume(no_ack=True,
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

        This is a generic send that can be re implemented for different
        kinds of NChannels...
        All implementations will utilize the basic_publish amqp channel
        class method to 'send' the payload into the system. basic_publish
        is not a deferred method (It shouldn't be, anyways) -- there is
        no response required from the broker, although the broker can call
        us back if delivery to a queue or a consumer is not immediately
        possible if we ask it (configure it with immediate/mandatory
        flags). A successful send to the message broker is all that we need
        to consider; the message system guarantees delivery to the
        destination (assuming the destination is there). Any problem
        sending will produce a failure/exception, and not an error
        response.
        txamqp happens to return a deferred for basic_publish

        dest:
         - simple name
         - (exchange, routing_key,) This could serve as an official name
        How should delivery policy be set here? (how should immediate and
        mandatory be configured/managed/specified?)
        """
        properties = {}
        # Only add properties if they are provided. Defaults aren't needed
        # (yet) and we don't a dict with None values.
        if application_headers:
            properties['application headers'] = application_headers
        if content_type:
            properties['content type'] = content_type
        if content_encoding:
            properties['content encoding'] = content_encoding
        if message_type:
            properties['type'] = message_type
        if reply_to:
            properties['reply to'] = reply_to
        if correlation_id:
            properties['correlation id'] = correlation_id
        if message_id:
            properties['message id'] = message_id
        # msg body is assumed to be properly encoded 
        content = Content(body=msg, properties=properties)
        self.chan.basic_publish(exchange='amq.direct', #todo  
                                content=content,
                                routing_key=dest, #todo 
                                immediate=True, #todo 
                                mandatory=True) #todo

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

    def receive(self, msg):
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
        props = msg.content.properties
        msg_type = props['type']
        # stupid select routine:
        if msg_type == 'rpc-response':
            correlation_id = props['correlation id']
            try:
                response_deferred = self.pending_responses.pop(correlation_id)
            except KeyError:
                # we aren't expecting this message, so it should be dropped
                # should it be returned to the broker?
                # should we save or report this?
                return
            response_deferred.callback(msg.content.body) # XXX need to add
            # optional encoding layer that uses a Serialization registry
        else:
            self.entity.receive(msg)

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
        yield self.chan.basic_consume(queue=self.name, no_ack=True,
                                                    consumer_tag=self.name)

    def receive(self, msg):
        """
        simplest implementation: one request at a time
        """
        props = msg.content.properties
        msg_type = props['type']
        if msg_type == 'rpc-request':
            # should a Entity Request Message be constructed for this
            # interaction?
            reply_to = props['reply to'] # XXX message format enforcement needed
            correlation_id = props['correlation id']
            request = Request(msg.content.body, self, reply_to, correlation_id)
            self.entity.receive(request)

class NodeManager(service.Service):
    """
    or just Node?
    interface for binding into message system, independant of amqp
    connection

    If this is a service, it can be adapted to be a Node Factory
    """
    implements(ianion.INodeManager)

    def __init__(self):
        """
        should the Node object be passed in here?
        or should it be given as an event?
        """
        self.entities = {} # the resources
        self.nchannels = {} # the protocol/transports (not exactly amqp chan)
        self.node = None #Gets set by node when this is passed to its __init__

    def startService(self):
        """
        """
        service.Service.startService(self)
        for name in self.entities:
            self.startEntity(name)

    def stopService(self):
        """
        """
        service.Service.stopService(self)
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


    def deliverMessage(self, name, msg):
        """
        get entity nchannel by name and invoke its receive event
        """
        nchan = self.nchannels[name]
        nchan.receive(msg) # simplest first go

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
        if self.running:
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
        d = self.node.bind_nchannel(nchan) #this is a deferred operation!
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


class NodeContainer(protocol.ClientFactory):
    """
    or MessagingNodeContainer?
    This version of NodeContainer takes a manager instance (the actual
    node) as an init arg.

    It might be possible to automatically create a manager, but that would
    be too many things for this class to do

    The factory uses the connected protocol for as long as the connection
    lasts. This factory should not be connected with reactor.connectTCP
    more than once per instance.
    """
    protocol = MessagingProtocol
    delegate = NodeDelegate

    def __init__(self, manager, username='guest', password='guest', vhost='/'):
        """
        """
        manager.node = self # might be cleaner to just pass NodeContainer
        #instance to NodeManager, treating NodeContainer like reactor...
        self.manager = manager
        self.username = username
        self.password = password
        self.vhost = vhost
        self.spec = spec.load(SPEC_PATH)

    def buildProtocol(self, addr):
        delegate = self.delegate() #XXX
        p = self.protocol(delegate, self.vhost, self.spec)
        p.factory = self
        return p

    def connectClient(self, client):
        """
        Event that the amqp client calls when login to the broker succeeds.
        """
        self.client = client
        self.manager.startService() # or activate?
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

        ch0 = self.client.channel(0)
        d = ch0.connection_close()
        d.addCallbacks(close_ok, close_err)
        return d

    def clientConnectionLost(self, connector, reason):
        """
        """
        self.connector = connector
        #self.manager.stopService() # or deactivate?
        self.manager.connectionLost(reason)
        #reason.printDetailedTraceback() # dont print if Clean close

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
 
    def deliverMessage(self, msg):
        """
        msg is an txamqp message object
        consumer_tag should be a name in node manager (what about entities
        that have multiple consumers?
        delegate to manager deliverMessage
        """
        #XXX FIXME!!!!
        name = msg.consumer_tag.split('.')[0]
        #msg = msg translation might happen here..
        self.manager.deliverMessage(name, msg)

def how_it_should_all_work():
    # Don't actually try to call this function ;-)
    # Here, the manager is a dependency of the NodeContainer factory
    # The factory has a relationship with the manager
    node = NodeManager()
    node_container = NodeContainer(node) #Factory
    reactor.connectTCP(host, port, node_container)
    
    ## OR ##
    node = NodeManager()
    reactor.connectTCP(host, port, INodeContainer(node)) #OR
    reactor.connectTCP(host, port, INodeFactory(node))
    # This lets you focus on the NodeManager interface without worrying
    # about the relationship between the Node Manager and the Node
    # Container (Factory). The Node Container notifies the Manager when it
    # the connection starts and stops. The connection start event is the
    # point where they system _can_ start running (the messaging network is
    # up). The connection lost event will bring the Manager into an
    # inactive state. A connection lost event can result from a normal
    # shutdown initiated by the manager (hmm, or by the container?); OR a
    # connection lost event can result from an amqp error, in which case
    # the manager goes into a failure mode. Each entity can be restarted
    # when/if the messaging comes back on. This means all connection state
    # or interactions will be fresh, but the business state within any
    # entity is unaffected; the manager deals with entity instances, not
    # classes, it doesn't have anything to do with the creation of an
    # entity, it has to do with the operation of an entity in the Node
    # environment. The creation process (configuration, instantiation,
    # assembly, etc.) of your application services are handled via typical
    # means (twistd application, tac file or plugin)


    # Entities can be added to run in the node using the node manager 
    # any time after the manager is created
    rservice = RabbitmqctlService()
    rentity = MessagingEntity(rservice)
    node.addEntity(rentity, 'rabbitmqctl')
    # The entity will start immediately if the manager is active,
    # otherwise, it will start when the manager conneciton is active

    # Entities can be removed from the node
    node.removeEntity('rabbitmqctl')



def test():
    node_manager = NodeManager()
    node_container = NodeContainer(node_manager)
    reactor.connectTCP('localhost', 5672, node_container)
    return node_manager



if __name__ == '__main__':
    test()
    reactor.run()






