"""
Messaging Entities are used to adapt your business service into something
that can send and/or receive messages.

One of the simplest things you can do with an Entity, is make an RPC
Service. The key thing is that your service has it's own interface, and is
defined independent of anion. The less dependent of anion you are, the
better because you're flexible and it's easy to maintain your code.

You could create an version of entity that knows the service.Service
interface (or something similar)...something the Node can send
notifications to; likewise, the some entities might want to interact with
of their Node environment, so they would want an interface to the Node.

Out of the box, an Entity that is added to a node gets connected to the
message system, with a specified messaging pattern.

----
refact2r

These aren't channels, these are message receivers and constructors.
If a entity constructs a new message, it is either responding to a request
through an request response channel, or it is creating a new channel
through the node interface. 

"""
import json

from twisted.internet import defer
from twisted.python import log

from zope.interface import implements

from anion import ianion

class Entity(object):
    """
    Messaging Entity (or Process)

    The receive method is to be implemented by the application.
    process is like render. process doesn't always have to be like a
    request response. 

    This could eventually be like a gen analog. This first try will be like
    a server. 
    """

    implements(ianion.IMessagingEntity)

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

class RPCEntityFromService(Entity):
    """
    Going to need a way to enforce the downstream service interface
    """

    def __init__(self, service):
        self.service = service

    def receive(self, request):
        cmd_dict = json.loads(request.body)

        d = self._call_cmd(cmd_dict)
        d.addCallback(self._send_response, request)
        d.addErrback(self._handle_error, request)

    def _call_cmd(self, cmd_dict):
        meth = getattr(self.service, cmd_dict['name'])
        args = cmd_dict['required']
        kwargs = dict([(str(k), v) for k, v in cmd_dict['optional'].items()])
        return defer.maybeDeferred(meth, *args, **kwargs)

    def _send_response(self, result, request):
        msg = json.dumps(result)
        request.reply(msg)

    def _handle_error(self, reason, request):
        """return some kind of failure message to the client, since this is
        an rpc response.
        """
        reason.printTraceback()


class RPCClientEntityFromInterface(Entity):
    """
    Provides IRabbitMQControlService interface.

    In this version of the client, the methods are automatically generated
    from the interface. It's harder to see how the client works (you have
    to refer to the interface), but you gain robustness by directly using
    the interface.
    """

    def __init__(self, service_name, iface):
        self.service_name = service_name # The name of the service this is a client for
        namesAndDesc = iface.namesAndDescriptions()
        for name, command in namesAndDesc:
            info = command.getSignatureInfo()
            doc = command.getDoc()
            setattr(self, name, _Command(self, name, info, doc))

    def callRemote(self, cmd_dict):
        """
        Send the command to the remote service.
        Returns a Deferred that fires with the response.
        """
        data = json.dumps(cmd_dict) #XXX handle encoding errors
        d = self.nchannel.rpc_send(self.service_name, data)

        def decode_response(data):
            return json.loads(data)
        d.addCallback(decode_response)
        d.addErrback(defer.logError) # the caller handles errors
        return d

class _Command(object):
    """
    Command method generated from interface.
    Serialize using json
    """

    def __init__(self, client, name, siginfo, doc):
        self.client = client
        self.name = name
        self.positional = siginfo['positional']
        self.required = siginfo['required']
        self.optional = siginfo['optional']
        self.__doc__ = doc

    def __call__(self, *args, **kwargs):
        command_dict = self._commad_dict_from_call(*args, **kwargs)
        return self.client.callRemote(command_dict)

    def _commad_dict_from_call(self, *args, **kwargs):
        if not len(args) == len(self.required):
            raise TypeError('%s() takes at least %d arguments (%d given)' %
                    (self.name, len(self.required), len(args)))
        cmd_dict = {}
        cmd_dict['name'] = self.name
        cmd_dict['required'] = args
        cmd_dict['optional'] = {}
        for k, v in self.optional.iteritems():
            cmd_dict['optional'][k] = kwargs.get(k, v)
        if not (len(cmd_dict['required']) + len(cmd_dict['optional'])) == len(self.positional):
            raise TypeError('%s() takes %d arguments (%d given)' %
                    (self.name, len(self.positional), len(cmd_dict)))
        return cmd_dict

