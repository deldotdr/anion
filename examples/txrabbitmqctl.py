"""
This example requires the txrabbitmq python package to be available.
A rabbitmq broker must be running on your local machine (that is the
purpose of utility demonstrated here).
Ideally, the broker process owner should be you, otherwise you might have
to run this example as root.
"""

import os

try:
    import json
except ImportError:
    import simplejson as json

from twisted.internet import defer
from twisted.internet import reactor
from twisted.internet import protocol

from txrabbitmq import irabbitmqctl
from txrabbitmq.service import RabbitMQControlService

from anion import entity
from anion import messaging


##################################################################
## Client version 1
## Implement each method from the irabbitmqctl.IRabbitMQControlService
## interface. 

class RabbitMQControlClientVerbose(entity.Entity):
    """
    This is the verbose implementation of the Control Client. All the
    irabbitmqctl.IRabbitMQControlService methods are explicitly
    implemented. Compare this to the alternate implementation below, which
    generates the methods from the interface definition, automatically.
    """

    def __init__(self, remote_name):
        self.remote_name = remote_name # The name of the service this is a client for

    def callRemote(self, cmd_dict):
        """
        The Commands bound to this client use this.
        """
        data = json.dumps(cmd_dict) #XXX handle encoding errors
        d = self.nchannel.rpc_send(self.remote_name, data)

        def decode_response(data):
            return json.loads(data)
        d.addCallback(decode_response)
        d.addErrback(defer.logError) # the caller handles errors
        return d
     
    def add_user(self, username, password):
        """add new user with given password"""
        return self.callRemote({
                'required': (username, password), 
                'optional': {}, 
                'name': 'add_user'
                })

    def delete_user(self, username):
        """delete user"""
        return self.callRemote({
                'required': (username,), 
                'optional': {}, 
                'name': 'delete_user'
                })

    def change_password(self, username, password):
        """change user password"""
        return self.callRemote({
                'required': (username, password), 
                'optional': {}, 
                'name': 'change_password'
                })

    def list_users(self):
        """list all users"""
        return self.callRemote({
                'required': (), 
                'optional': {}, 
                'name': 'list_users'
                })

    def add_vhost(self, vhostpath):
        """add new vhost"""
        return self.callRemote({
                'required': (vhostpath,), 
                'optional': {}, 
                'name': 'add_vhost'
                })

    def delete_vhost(self, vhostpath):
        """delete vhost"""
        return self.callRemote({
                'required': (vhostpath,), 
                'optional': {}, 
                'name': 'delete_vhost'
                })

    def list_vhosts(self):
        """list all vhosts"""
        return self.callRemote({
                'required': (), 
                'optional': {}, 
                'name': 'list_vhosts'
                })

    def set_permissions(self, username, config_regex, write_regex, read_regex, vhostpath=None):
        """set permission of a user to broker resources"""
        return self.callRemote({
                'required': (username, config_regex, write_regex, read_regex), 
                'optional': {'vhostpath': vhostpath}, 
                'name': 'set_permissions'
                })

    def clear_permissions(self, username, vhostpath=None): 
        """clear user permissions"""
        return self.callRemote({
                'required': (username,), 
                'optional': {'vhostpath': vhostpath},
                'name': 'clear_permissions'
                })

    def list_vhost_permissions(self, vhostpath=None): 
        """list all users permissions"""
        return self.callRemote({
                'required': (), 
                'optional': {'vhostpath': vhostpath}, 
                'name': 'list_vhost_permissions'
                })

    def list_user_permissions(self, username=None): 
        """list all users permissions"""
        return self.callRemote({
                'required': (), 
                'optional': {'username': username}, 
                'name': 'list_user_permissions'
                })

    def list_queues(self, vhostpath=None, queueinfoitem=None):
        """list all queues
        @todo need a list of queueinfoitems 
        """
        return self.callRemote({
                'required': (), 
                'optional': {'vhostpath': vhostpath, 'queueinfoitem': queueinfoitem}, 
                'name': 'list_queues'
                })

    def list_exchanges(self, vhostpath=None, exchangeinfoitem=None):
        """list all exchanges
        @todo need a list of exchangeinfoitems 
        """
        return self.callRemote({
                'required': (), 
                'optional': {'vhostpath': vhostpath, 'exchangeinfoitem': exchangeinfoitem}, 
                'name': 'list_exchanges'
                })

    def list_bindings(self, vhostpath=None):
        """list all bindings"""
        return self.callRemote({
                'required': (), 
                'optional': {'vhostpath': vhostpath}, 
                'name': 'list_bindings'
                })

    def list_connections(self, connectioninfoitem=None):
        """list all connections
        @todo need a list of connectioninfoitems
        """
        return self.callRemote({
                'required': (), 
                'optional': {'connectioninfoitem': connectioninfoitem},
                'name': 'list_connections'
                })


##############################################################################
## Client version 2
## Generate commands from the interface irabbitmqctl.IRabbitMQControlService

class RabbitMQControlClientFromInterface(entity.Entity):
    """
    Provides IRabbitMQControlService interface.

    In this version of the client, the methods are automatically generated
    from the interface. It's harder to see how the client works (you have
    to refer to the interface), but you gain robustness by directly using
    the interface.
    """

    def __init__(self, service_name):
        self.service_name = service_name # The name of the service this is a client for
        namesAndDesc = irabbitmqctl.IRabbitMQControlService.namesAndDescriptions()
        for name, command in namesAndDesc:
            info = command.getSignatureInfo()
            doc = command.getDoc()
            setattr(self, name, Command(self, name, info, doc))

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
 
class Command(object):
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
 


################################################################################
## Service

class RabbitMQControlEntityFromService(entity.Entity):

    def __init__(self, service):
        """
        service is an instance of RabbitMQControlService that talks
        directly to the RabbitMQ broker.
        """
        self.service = service

    def receive(self, request):
        cmd_dict = json.loads(request.body)

        d = self._call_cmd(cmd_dict)
        d.addCallback(self._send_response, request)
        d.addErrback(self._handle_error, request)

    def _call_cmd(self, cmd_dict):
        meth = getattr(self.service, cmd_dict['name'])
        args = [str(e) for e in cmd_dict['required']]
        #kwargs = dict([(str(k), v) for k, v in cmd_dict['optional'].items()])
        kwargs = {}
        for k, v in cmd_dict['optional'].items():
            if isinstance(v, unicode):
                v = str(v) # Damn you JSON!! (and Python :-/)
            kwargs[str(k)] = v
        return meth(*args, **kwargs)

    def _send_response(self, result, request):
        msg = json.dumps(result)
        request.reply(msg)

    def _handle_error(self, reason, request):
        """return some kind of failure message to the client, since this is
        an rpc response.
        """
        reason.printTraceback()


def make_rabbitmqctl_service():
    """
    Boiler plate construction of txrabbitmq Service.
    Usually Twisted Service libraries provide a makeService method.
    """
    from twotp import Process, readCookie, buildNodeName
    cookie = readCookie()
    nodeName = buildNodeName('txrabbitmq')
    process = Process(nodeName, cookie)
    service = RabbitMQControlService(process, 'rabbit')
    return service

###############################################################################
## Example usage

def setup_service_and_client():
    # Start the Node
    node_manager = messaging.NodeManager()
    node_container = messaging.NodeContainer(node_manager)
    reactor.connectTCP('localhost', 5672, node_container)

    # Configure the service Entity, and add it to the node
    rservice = make_rabbitmqctl_service()
    rentity = RabbitMQControlEntityFromService(rservice)
    node_manager.addEntity('rabbitmqctl', rentity, messaging.RPCChannel)

    # Configure the client Entity, and add it to the node
    client = RabbitMQControlClientFromInterface('rabbitmqctl')
    node_manager.addEntity('anonymous', client, messaging.NChannel)
    return node_manager, client

def setup_service_and_client():
    # Start the Node
    node = messaging.Node()
    reactor.connectTCP('localhost', 5672, node)

    # Configure the service Entity, and add it to the node
    rservice = make_rabbitmqctl_service()
    rentity = RabbitMQControlEntityFromService(rservice)
    node.addEntity('rabbitmqctl', rentity, messaging.RPCChannel)

    # Configure the client Entity, and add it to the node
    client = RabbitMQControlClientFromInterface('rabbitmqctl')
    node.addEntity('anonymous', client, messaging.NChannel)
    return node, client

def start_client():
    node = messaging.Node()
    reactor.connectTCP('localhost', 5672, node)
    client = RabbitMQControlClientFromInterface('rabbitmqctl')
    node.addEntity('anonymous', client, messaging.NChannel)
    return node, client

def test():
    """
    Note:
    Starting the reactor after using the client is not supported yet.
    Run this test from the ayps shell:
    ><> import txrabbitmqctl
    ><> node, client = txrabbitmqctl.test()
    ><> client.list_queues()
    """
    node, client = setup_service_and_client()
    return node, client
    



