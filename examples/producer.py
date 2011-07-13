
import sys
import time

from twisted.internet import reactor
from twisted.internet import defer
from twisted.internet import task
from twisted.python import usage
from twisted.python import log

from anion import entity
from anion import messaging

class MessageCountSampler(object):

    def __init__(self, *args):
        self.entities = args
        self.total_count = 0
        self.start_time = time.time()
        self.last_time = time.time()

    def sample(self):
        _total = 0
        for e in self.entities:
            _total += e.get_count()
        _diff = _total - self.total_count
        self.total_count = _total
        _now_time = time.time()
        d_time = _now_time - self.last_time
        inst_rate = _diff / d_time
        self.last_time = _now_time
        print "count: %s msgs - rate: %s msg/sec" % (str(self.total_count), str(inst_rate),)



class Producer(entity.Entity):

    def __init__(self, to, content, max_msgs):
        self.to = to
        self.content = content
        self.max_msgs = max_msgs
        self.total_sent = 0

    def get_count(self):
        return self.total_sent

    def connectionMade(self):
        task.cooperate(self.msg_iterator())

    def msg_iterator(self):
        for i in xrange(self.max_msgs):
            self.total_sent += 1
            self.nchannel.send(self.to, self.content)
            yield None


class TestChannel(messaging.NChannel):

    @defer.inlineCallbacks
    def configureChannel(self):
        yield self.chan.queue_declare(queue='test')
        yield self.chan.queue_bind(queue='test', exchange='amq.direct', routing_key='test')



def main():
    node = messaging.Node()
    reactor.connectTCP('localhost', 5672, node)
    producer1 = Producer('test', 'blah'*30, 400000)
    node.addEntity('test', producer1, TestChannel)
    sampler = MessageCountSampler(producer1)
    loop = task.LoopingCall(sampler.sample)
    loop.start(1)

def main2():
    node = messaging.Node()
    producer1 = Producer('test', 'blah'*30, 400000)
    node.addEntity('test', producer1, TestChannel)

    amoeba_endpoint = endpoints.TCP4ClientEndpoint(reactor, 'amoeba.ucsd.edu', 'amqp')
    amoeba_endpoint.connect(node)

class Options(usage.Options):
    optParameters = [
            ['host', None, 'localhost', 'Main broker'],
            ['max_msgs', None, 100000, 'Number of messages to publish'],
            ['size_msgs', None, 30, 'Size of message'],
            ]

if __name__ == '__main__':
    main()
    reactor.run()
