
from twisted.application import internet
from twisted.application import service
from twisted.internet import reactor
from twisted.internet import defer

from zope.interface import Interface

from txredis.protocol import Redis

from anion import entity
from anion import messaging

class IStore(Interface):

    def put(key, val):
        """
        """

    def get(key):
        """
        """

    def delete(key):
        """
        """


class Store(object):

    def __init__(self):
        self.store = {}

    def put(self, key, val):
        self.store[key] = val
        return True

    def get(self, key):
        return self.store.get(key, '')

    def delete(self, key):
        return self.store.pop(key, '')

class StoreFromRedisClient(object):
    """ClientFactory
    If the redis api is available via a factory, then it can be treated as
    a TCP Service. That means it can be started and stopped by the
    application.
    The client factory always has to be given to the reactor, so when the
    reactor is run, the client factory will be started.
    If it is a tcp service, it can set the application service collection
    as its parent; the starting of the application will then cause the
    redis client to go active (assuming it is configured correctly).
    The redis tcp service can then be adapted to be an Entity, which is
    controlled by a Node.
    The Node starts an Entity when the node is connected, which also
    happens when the reactor is started.
    An Entity that adapts a service will need to be aware of the state of
    its service...without knowing how it is actually implemented.

    What if the redis client service is not running, but a Node activates
    the Entity adapting the redis client service?
    Then the Entity can notify the Node that it is in a failure state,
    because the service is not running. 
    It seems that the business service (redis client service) should be
    verified as working before the Node starts the Entity, or before the
    Entity is given to the Node for it to start. 
    That means, there could be an Entity type that first ensures its
    service is running, before it lets the Node start itself.
    The simpler thing is to not assume too much about how the entity
    business handling code works; and if an Entity is given to a Node to be
    started, it should be started as soon as possible.
    """

    def __init__(self, client):
        self.client = client

    def get(self, key):
        return self.client.hget('foo', key)

    def put(self, key, value):
        return self.client.hset('foo', key, value)

    def delete(self, key):
        return self.client.hdelete('foo', key)

def testr():
    """
    redis is a backend service that can be re-presented over different
    application interfaces. 
    The redis client itself can be thought of as a TCP Client Service.
    """


def test():
    node = messaging.Node()
    reactor.connectTCP('localhost', 5672, node)
    serv = Store()
    ent = entity.RPCEntityFromService(serv)
    node.addEntity('store', ent, messaging.RPCChannel)
    cl = entity.RPCClientEntityFromInterface('store', IStore)
    node.addEntity('anon', cl, messaging.NChannel)
    return node, cl, serv 

