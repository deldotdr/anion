"""
The core interfaces.

The build up of the messaging abstractions are the most important.

The design of these interfaces attempts to be as unassuming as possible as
to what is communicating and how. In the OTP model, process don't
communicate, processes provide a context from which messages are sent, or
into which messages are received.

"""

from zope.interface import Interface


class INodeMessagingSystem(Interface):
    """
    These are the basic operations on the messaging system. An
    implementation will probably need an active txamqp client.
    This interface also contains event handlers that the underlying amqp
    client can call during operation.
    An instance of an INodeMessagingSystem provider holds the context
    necessary to make a amqp broker connection. 

    Things you can do:
    start, shutdown, restart 
    allocate amqp client resources
    attach NChannels (NChannels are Node channels. They are different but
    related to amqp connection channels). 

    This is a low level interface that application developers will probably
    not need to worry about usually.
    """

    def shutdown_client():
        """
        Initiate amqp connection close, which should conclude in the tcp
        transport close, and result in the clientConnectionLost event being
        called.
        """

    def bind_nchannel(nchannel):
        """
        nchannel represents a messaging pattern; therefore it facilitates
        configuring an amqp channel (and maybe queues in the broker), and
        maybe starting one or more consumers.
        nchannel is also a receiving point for delivered messages --
        messages coming out of amqp land and into Node land do so though an
        NChannel.
        
        The act of binding (or attaching..) means an amqp channel will be
        created for use by the provided nchannel. The NChannel instance
        uses the amqp channel to apply the amqp configuration required by
        its message pattern type. This configuration is describable using
        pure amqp Queue, Exchange, and Basic Class methods (and their
        associated parameters). Implementors can therefore provide their 
        own NChannel types without needing to know anything beyond the
        NChannel interface, and the amqp specification.
        """

    def connectClient(client):
        """
        This is an event the amqp client calls, passing itself in.
        Before this event, the Node is not in an operational state.
        """

    def clientConnectionLost(connector, reason):
        """
        This is an event notifying the Node that the amqp client connection
        closed for some reason (it might have been asked to close, or it
        might have failed...). 
        The connector is a Twisted specified object. It can be used to
        reconnect the system. If reconnection succeeds, the connectClient
        event will be called, and the system can start back up.
        """

    def deliverMessage(msg):
        """
        An event that the amqp client calls when a message is delivered
        from the broker. msg is an amqp basic Content class object.
        All messages for a node instance enter through this one point.
        The context of the message is extracted from the headers here, and
        a decision is made on which NChannel to pass the msg on to.
        
        The idea of a canonical piece of delivery context should be
        extracted before firing this event, such that the signature was
        something to the effect of:
        def deliverMessage(NChannelID, msg)

        """


class INodeManager(Interface):
    """
    This isn't as core as INodeMessagingSystem, but the implementation so
    far makes most sense with an INodeMessagingSystem, INodeManager pair.

    INodeManager is a higher level interface that application developers
    and deployers will use. 
    """

    def addEntity(name, entity, nChannel):
        """
        name is the messaging name to bind to
        entity is an object providing the IMessagingEntity interface
        nChannel is the class of NChannel to use
        
        More parameters can be attributed to the entity as it runs in the
        node:
         - identity
         - policy (what it can do in the node...)
        """

    def removeEntity(name):
        """
        """

    def startEntity(name):
        """
        """

    def stopEntity(name):
        """
        """

    def deliverMessage(name, msg):
        """
        """

    def connectionLost(reason):
        """
        This is a maybe...
        A messageSystemFailure or something might be better.
        """

class IMessagingEntity(Interface):
    """
    Alternative interface name: INodeEntity
    The word Entity could be replaced with a better word for this thing.

    Application developers use these to adapt their generic services to
    the Node Messaging System transport.

    IMessagingEntity is similar to twisted.web IResource.
    """

    def makeConnection(nchannel):
        """
        An event that establishes an operational state of the entity.
        Application developers shouldn't use this. They can use
        connectionMade for notification of activation.

        Tentative name to be rethought; current name is an artifact of the
        prototype
        """

    def connectionMade():
        """
        An event the application developer can implement to do something as
        soon as the entity is able to use the nodes messaging system.

        Tentative name to be rethought; current name is an artifact of the
        prototype
        """

    def receive(msg):
        """
        The main event for an Entity (similar to Resource.render)

        msg will be some type of Node message.
        """

class INChannel(Interface):
    """
    mixture of request, transport, and amqp channel

    NChannel might get the point across the best.

    amqp configuration for a style of messaging

    an object that holds an entity's send capability

    an amqp channel can have multiple consumers, so an entity can have
    multiple consumers, and consumers shouldn't be shared between entities.
    This way, a running entity corresponds to an amqp channel this is open
    for the duration of the entitys life. an entity only ever uses one
    channel.
    If it binds multiple consumers, it must be able to deal with the
    different deliveries, which will all be given to the same entity receive
    handler.

    """

    def createChannel(chan):
        """
        Event for that gives the NChannel the amqp channel allocated for it
        by the amqp client. With the chan, NChannel applies its amqp
        configuration with the configureChannel event.
        """

    def configureChannel():
        """
        Implementors use amqp Queue, Exchange, ans Basic class methods to
        set their chan up the way the want it.
        Aside from dealing with deferreds, no knowledge of the amqp client
        should be necessary here, nor should it be necessary to even import
        the client lib in the module of the NChannel implementation.
        """

    def send(dest, msg, application_headers={},
                                content_type=None,
                                content_encoding=None,
                                message_type=None,
                                reply_to=None,
                                correlation_id=None,
                                message_id=None):
        """
        The NChannel send is a main fulcrum in this messaging system
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

    def receive(msg):
        """
        msg is the amqp Basic Content class message.

        This could be renamed 'process' or something, to indicate the
        message is being processed and is on the way to being received,
        ultimately by Entity receive

        The NChannel handles amqp details; amqp is not exposed or needed
        beyond the NChannel.  The Entity doesn't know or care about amqp.
        This event then calls the entity instance receive

        The nchannel could do some sorting/routing here.
        The use of the message type property would make this easy (if all
        messages had a type / every interaction uses a certain messaging
        """

class INodeMessage(Interface):
    """
    A Node message is a message in the same logical space as a Node,
    which is above the amqp messaging.

    attributes:
     - body
     - nchannel
     - reply to
     - correlation id
     - .....

    Should methods like 'reply' and 'ack' be part of the interface?
    It might be more appropriate to specify different NChannel interfaces
    for different messaging patterns individually. 
    Each type of NChannel is essentially a protocol flavor, right above
    amqp.

    send might be a baseline method, it would be the same as NChannel send.
    It isn't clear if all IMessagingEntity implementations should have
    their nchannel as an attribute.
    """


class ISerialize(Interface):
    """
    This interface name should be discussed, but here is what the thing is
    for and where it fits in:

    A generic interface for encoding and decoding object types.
    Implementations can decide if they support arbitrary types or a
    constrained set of types or even a specific set of defined types. 

    The main thing is, the content sent by an Entity must be encoded and
    the content delivered to an Entity must be decoded. There can be
    flexibility in how things get encoded/decoded and who is responsible,
    (the simplest thing to do with any quick hack is to encode and decode
    right in the Entity implementation -- the hard coded method.
    The next, more sophisticated, strategy is to let the Entity encode and
    decode simple dict structures with JSON or msgpack automatically.
    Then, more sophisticated yet (and, therefore, requiring more careful
    design though), your Entity implementation can include a set of defined
    types (call them message objects; or forget messages and call the
    Entity a Protocol). 

    these attributes might be included in a serialization registry interface:
     - content_type
     - content_encoding

    The python encodings.codecs.Codec should be used as a design reference.
    """

    def encode(obj):
        """
        obj is potentially any python object type that makes sense to
        encode.
        return a byte string 
        """

    def decode(data):
        """
        data is an encoded byte string that the implementation will know
        how to unserialize into an object.
        return the object.
        """

class ISerializations(Interface):
    """
    """

