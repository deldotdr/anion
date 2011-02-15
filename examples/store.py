
from twisted.internet import reactor
from twisted.internet import defer

from zope.interface import Interface

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


def test():
    node = messaging.Node()
    reactor.connectTCP('localhost', 5672, node)
    serv = Store()
    ent = entity.RPCEntityFromService(serv)
    node.addEntity('store', ent, messaging.RPCChannel)
    cl = entity.RPCClientEntityFromInterface('store', IStore)
    node.addEntity('anon', cl, messaging.NChannel)
    return node, cl, serv 

