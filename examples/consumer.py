
import sys
import time

from twisted.internet import reactor
from twisted.internet import endpoints
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
        return "count: %s msgs - rate: %s msg/sec " % (str(self.total_count), str(inst_rate),)


class Counter(entity.Entity):

    def __init__(self):
        self.count = 0

    def get_count(self):
        return self.count

    def receive(self, msg):
        self.count += 1

class TestChannel(messaging.NChannel):

    @defer.inlineCallbacks
    def configureChannel(self):
        yield self.chan.queue_declare(queue='test')
        yield self.chan.queue_bind(queue='test', exchange='amq.direct', routing_key='test')
        self.chan.basic_consume(no_ack=True, consumer_tag=self.name+'.foo')

class ControlEntity(entity.Entity):

    def __init__(self, loop):
        """
        control the loop
        """

class DataPublish(entity.Entity):

    def __init__(self, data_name, data_source, sample_rate=1):
        self.loop = task.LoopingCall(self._write)
        self.data_name = data_name
        self.data_source = data_source
        self.sample_rate = sample_rate

    def connectionMade(self):
        self.loop.start(self.sample_rate)

    def _write(self):
        self.nchannel.send(self.data_name, self.data_source())

class PubChannel(messaging.NChannel):
    exchange = 'amq.topic'


class DataSubscriberPrinter(entity.Entity):

    def receive(self, msg):
        print msg

class SubChannel(messaging.NChannel):
    exchange = 'amq.topic'

    @defer.inlineCallbacks
    def configureChannel(self):
        yield self.chan.queue_declare(queue='testdata', auto_delete=True)
        yield self.chan.queue_bind(queue='testdata',
                exchange=self.exchange, routing_key='consumer_rate')
        self.chan.basic_consume(no_ack=True, consumer_tag=self.name)


def main():
    node = messaging.Node()
    reactor.connectTCP('ghostfish.local', 5672, node)
    counter1 = Counter()
    counter2 = Counter()
    node.addEntity('foo', counter1, TestChannel)
    node.addEntity('foo2', counter2, TestChannel)

    data_node = messaging.Node()
    reactor.connectTCP('localhost', 5672, data_node)
    sampler = MessageCountSampler(counter1, counter2)
    data_publisher = DataPublish('consumer_rate', sampler.sample)
    data_node.addEntity('xpub', data_publisher, PubChannel)
    return node

def main2():
    node = messaging.Node()
    counter1 = Counter()
    node.addEntity('foo', counter1, TestChannel)
    amoeba_endpoint = endpoints.TCP4ClientEndpoint(reactor, 'amoeba.ucsd.edu', 'amqp')
    amoeba_endpoint.connect(node)

    data_node = messaging.Node()
    sampler = MessageCountSampler(counter1, counter2)
    data_publisher = DataPublish('consumer_rate', sampler.sample)
    data_node.addEntity('xpub', data_publisher, PubChannel)
    local_endpoint = endpoints.TCP4ClientEndpoint(reactor, 'localhost', 'amqp')
    local_endpoint.connect(data_node)

def main3():
    node = messaging.Node()
    data_sub_printer = DataSubscriberPrinter()
    node.addEntity('datasub', data_sub_printer, SubChannel)
    local_endpoint = endpoints.TCP4ClientEndpoint(reactor, 'localhost', 'amqp')
    local_endpoint.connect(node)


def want():
    Process('consumer.main2')
    Process('consumer.main3')



class ConsumerOptions(usage.Options):
    optParameters = [
            ['data_host', None, 'localhost', 'Data pubsub broker'],
            ]

    optFlags = [['monitor', 'm', 'Turn on monitor publisher']]

class Options(usage.Options):
    optParameters = [
            ['host', None, 'localhost', 'Main broker'],
            ]
    optFlags = [['monitor', 'm', 'Turn on monitor publisher']]
    subCommands = [['consumer', 'consumer', ConsumerOptions, 'Consumer performance test']]

def run():
    """
    This version does not print the msgs per second to stdout.
    It optionally publishes that to a pubsub channel
    """
    config = Options()
    config.parseOptions()

    if not config['monitor']:
        node = messaging.Node()
        counter1 = Counter()
        node.addEntity('foo', counter1, TestChannel)
        amoeba_endpoint = endpoints.TCP4ClientEndpoint(reactor, config['host'], 'amqp')
        amoeba_endpoint.connect(node)

        data_node = messaging.Node()
        sampler = MessageCountSampler(counter1)
        data_publisher = DataPublish('consumer_rate', sampler.sample)
        data_node.addEntity('xpub', data_publisher, PubChannel)
        local_endpoint = endpoints.TCP4ClientEndpoint(reactor, 'localhost', 'amqp')
        local_endpoint.connect(data_node)
    else:
        node = messaging.Node()
        data_sub_printer = DataSubscriberPrinter()
        node.addEntity('datasub', data_sub_printer, SubChannel)
        local_endpoint = endpoints.TCP4ClientEndpoint(reactor, 'localhost', 'amqp')
        local_endpoint.connect(node)




if __name__ == '__main__':
    #log.startLogging(sys.stdout)
    #main()
    run()
    reactor.run()
