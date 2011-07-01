
import sys
import time

from twisted.internet import reactor
from twisted.internet import defer
from twisted.internet import task
from twisted.python import log
log.startLogging(sys.stdout)

from anion import entity
from anion import messaging



class Counter(entity.Entity):

    def __init__(self):
        """
        start counter
        """
        self.received = 0
        self.last = ''
        self.start_time = time.time()

    def status(self):
        d_time = time.time() - self.start_time
        rate = self.received / d_time
        print '%s msgs received - %s msgs/sec - last %s' % (str(self.received), str(rate), self.last,)

    def connectionMade(self):
        loop = task.LoopingCall(self.status)
        loop.start(1)


    def receive(self, msg):
        """
        """
        self.received += 1
        self.last = msg

class CChannel(messaging.NChannel):

    @defer.inlineCallbacks
    def configureChannel(self):
        """
        This has something to do with defining a name, too.
        """
        log.msg('sssaddssaad')
        yield self.chan.queue_declare(queue='test')
        yield self.chan.queue_bind(queue='test', exchange='amq.direct', routing_key=self.name)
        #yield self.chan.basic_consume(no_ack=True, consumer_tag=self.name+'.foo')
        self.chan.basic_consume(no_ack=True, consumer_tag=self.name+'.foo')

class PrintLog(entity.Entity):

    def receive(self, msg):
        print msg.content.body

class LogChannel(messaging.NChannel):

    @defer.inlineCallbacks
    def configureChannel(self):
        """
        This has something to do with defining a name, too.
        """
        #yield self.chan.queue_delete(queue='testlog', if_empty=False)
        #yield self.chan.queue_declare(queue='testlog', passive=True)#, auto_delete=True)
        yield self.chan.queue_declare(queue='testlog', auto_delete=True)
        yield self.chan.queue_bind(queue='testlog', exchange='amq.rabbitmq.log', routing_key='#')
        yield self.chan.basic_consume(no_ack=True, consumer_tag=self.name+'.foo')

def main():
    #node = messaging.Node(vhost='/punch')
    #reactor.connectTCP('amoeba.ucsd.edu', 5672, node)
    node = messaging.Node()
    #node._faster = True
    reactor.connectTCP('localhost', 5672, node)
    counter = Counter()
    #node.addEntity('foo', counter, CChannel)
    reactor.callLater(0.4, node.addEntity,'foo', counter, CChannel)
    return node

def watchlog():
    node = messaging.Node()
    reactor.connectTCP('rabbitmq.oceanobservatories.org', 5672, node)
    printer = PrintLog()
    node.addEntity('foo', printer, LogChannel)
    return node

if __name__ == '__main__':
    main()
    #watchlog()
    reactor.run()
