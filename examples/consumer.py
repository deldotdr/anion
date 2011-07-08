
import sys
import time

from twisted.internet import reactor
from twisted.internet import defer
from twisted.internet import task
from twisted.python import log

from anion import entity
from anion import messaging

class MessageCountSampler(object):

    def __init__(self, *args):
        self.entities = args
        self.total_recvd = 0
        self.start_time = time.time()
        self.last_time = time.time()

    def status(self):
        _total = 0
        for e in self.entities:
            _total += e.received
        _diff = _total - self.total_recvd
        self.total_recvd = _total
        _now_time = time.time()
        d_time = _now_time - self.last_time
        inst_rate = _diff / d_time
        avg_rate = self.total_recvd / (_now_time - self.start_time)
        self.last_time = _now_time
        print "%s consumers - %s msgs rxd - inst: %s msg/sec " % (str(len(self.entities)), str(self.total_recvd), str(inst_rate),)



class Counter(entity.Entity):

    def __init__(self):
        """
        start counter
        """
        self.received = 0

    def connectionMade(self):
        self.received = 0

    def receive(self, msg):
        """
        """
        self.received += 1

class TestChannel(messaging.NChannel):

    @defer.inlineCallbacks
    def configureChannel(self):
        yield self.chan.queue_declare(queue='test')
        yield self.chan.queue_bind(queue='test', exchange='amq.direct', routing_key='test')
        self.chan.basic_consume(no_ack=True, consumer_tag=self.name+'.foo')


def main():
    node = messaging.Node()
    reactor.connectTCP('localhost', 5672, node)
    counter1 = Counter()
    counter2 = Counter()
    node.addEntity('foo', counter1, TestChannel)
    node.addEntity('foo2', counter2, TestChannel)
    sampler = MessageCountSampler(counter1, counter2)
    loop = task.LoopingCall(sampler.status)
    loop.start(1)
    return node

if __name__ == '__main__':
    log.startLogging(sys.stdout)
    main()
    reactor.run()
