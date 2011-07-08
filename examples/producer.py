
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
        self.total_sent = 0
        self.start_time = time.time()
        self.last_time = time.time()

    def status(self):
        _total = 0
        for e in self.entities:
            _total += e.total_sent
        _diff = _total - self.total_sent
        self.total_sent = _total
        _now_time = time.time()
        d_time = _now_time - self.last_time
        inst_rate = _diff / d_time
        avg_rate = self.total_sent / (_now_time - self.start_time)
        self.last_time = _now_time
        print "%s msgs txd - inst: %s msg/sec" % (str(self.total_sent), str(inst_rate),)



class Producer(entity.Entity):

    def __init__(self, to, content, max_msgs):
        self.to = to
        self.content = content
        self.max_msgs = max_msgs
        self.total_sent = 0

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
    loop = task.LoopingCall(sampler.status)
    loop.start(1)
    

if __name__ == '__main__':
    main()
    reactor.run()
