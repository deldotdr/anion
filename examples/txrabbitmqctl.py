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

from anion import messaging

class RabbitMQContro():
    """
    Functionality of 'rabbitmqctl' exposed as a Twisted Service.
    """

    def add_user(username, password):
        """add new user with given password"""

    def delete_user(username):
        """delete user"""

    def change_password(username, password):
        """change user password"""

    def list_users():
        """list all users"""

    def add_vhost(vhostpath):
        """add new vhost"""

    def delete_vhost(vhostpath):
        """delete vhost"""

    def list_vhosts():
        """list all vhosts"""

    def set_permissions(username, config_regex, write_regex, read_regex, vhostpath=None):
        """set permission of a user to broker resources"""

    def clear_permissions(username, vhostpath=None): 
        """clear user permissions"""

    def list_vhost_permissions(vhostpath=None): 
        """list all users permissions"""

    def list_user_permissions(username=None): 
        """list all users permissions"""

    def list_queues(vhostpath=None, queueinfoitem=None):
        """list all queues"""

    def list_exchanges(vhostpath=None, exchangeinfoitem=None):
        """list all exchanges"""

    def list_bindings(vhostpath=None):
        """list all bindings"""

    def list_connections(connectioninfoitem=None):
        """list all connections"""


#########################################################################
## An encoding for the RabbitMQControlService commands
## This is essentially a little protocol. 
## The irabbitmqctl.IRabbitMQControlService interface specifies the names
## and call signatures for every rabbitmqctl method.
##
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
        if not len(args) == len(self.required):
            raise TypeError('%s() takes at least %d arguments (%d given)' %
                    (self.name, len(self.required), len(args)))
        cmd_msg = {}
        cmd_msg['required'] = args
        cmd_msg['optional'] = {}
        for k, v in self.optional.iteritems():
            cmd_msg['optional'][k] = kwargs.get(k, v)
        if not (len(cmd_msg['required']) + len(cmd_msg['optional'])) == len(self.positional):
            raise TypeError('%s() takes %d arguments (%d given)' %
                    (self.name, len(self.positional), len(cmd_msg)))
        cmd_msg['name'] = self.name
        return self.client.callRemote(cmd_msg)


##############################################################################
## Client

class RabbitMQControlClient(messaging.Entity):
    """
    Provides IRabbitMQControlService interface.

    In this version of the client, the methods are automatically generated
    from the interface. It's harder to see how the client works (you have
    to refer to the interface), but you gain robustness by directly using
    the interface.
    """

    def __init__(self, remote_name):
        self.remote_name = remote_name # The name of the service this is a client for
        namesAndDesc = irabbitmqctl.IRabbitMQControlService.namesAndDescriptions()
        for name, command in namesAndDesc:
            info = command.getSignatureInfo()
            doc = command.getDoc()
            setattr(self, name, Command(self, name, info, doc))

    def callRemote(self, cmd_msg):
        """
        The Commands bound to this client use this.
        """
        data = json.dumps(cmd_msg) #XXX handle encoding errors
        return self.nchannel.rpc_send(self.remote_name, data)
     

################################################################################
## Service

class RabbitMQControlEntityFromService(messaging.Entity):

    def __init__(self, service):
        """
        service is an instance of RabbitMQControlService that talks
        directly to the RabbitMQ broker.
        """
        self.service = service

    def receive(self, request):
        service_command = json.loads(request.body)

        meth = getattr(self.service, service_command['name'])
        args = service_command['required']
        kwargs = dict([(str(k), v) for k, v in service_command['optional'].items()])
        d = meth(*args, **kwargs)
        d.addCallback(self._send_response, request)
        d.addErrback(self._handle_error, request)

    def _send_response(self, result, request):
        msg = json.dumps(result)
        request.reply(msg)

    def _handle_error(self, reason, request):
        """return some kind of failure message to the client, since this is
        an rpc response.
        """
        reason.printTraceback()


def make_rabbitmqctl_service():
    from twotp import Process, readCookie, buildNodeName
    cookie = readCookie()
    nodeName = buildNodeName('txrabbitmq')
    process = Process(nodeName, cookie)
    service = RabbitMQControlService(process, 'rabbit')
    return service


def test_node():
    node = messaging.NodeManager()
    node_container = messaging.NodeContainer(node)
    reactor.connectTCP('localhost', 5672, node_container)

    rserv = make_rabbitmqctl_service()
    rent = RabbitMQControlEntityFromService(rserv)
    node.addEntity('rabbitmqctl', rent, messaging.RPCChannel)
    cl = RabbitMQControlClient('rabbitmqctl')
    node.addEntity('cl', cl, messaging.NChannel)
    return node, cl


if __name__ == '__main__':
    test_node()
    reactor.run()






