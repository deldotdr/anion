"""
Simple control node that can start and stop instances of the producer and
consumer programs.


This interface needs to deal with start-able process types and running
process instances in a distributable manner.

The local concern for starting a process:
    ---- general functionality (not dependent on location) ----
    executable name
    args
    ProcessProtocol - protocol for parsing stdout and/or format for
        writing to stdin. This is related to the format of what can be sent
        or received in/out of this process. 

    ---- specific to deployment platform ----
    --- (These options will be configured at deployment) ---
    uid
    gid
    env (context)
    path 


"""

from zope.interface import Interface, implements

from twisted.application import service
from twisted.protocols import basic
from twisted.internet import error
from twisted.internet import reactor
from twisted.internet import protocol
from twisted.internet import endpoints
from twisted.python import log

from anion import entity
from anion import messaging


class INodeControl(Interface):
    """
    This interface needs to deal with start-able process types and running
    process instances in a distributable manner.
    """

    def start_process(name, number=1):
        """
        return process instance ids
        """

    def stop_process(inst_id):
        """
        """

    def list_processes():
        """
        """

    def list_process_types():
        """
        """

class DummyTransport:

    disconnecting = 0

transport = DummyTransport()

class LineLogger(basic.LineReceiver):

    tag = None
    delimiter = '\n'

    def lineReceived(self, line):
        log.msg('[%s] %s' % (self.tag, line))


class LoggingProtocol(protocol.ProcessProtocol):

    service = None
    inst_id = None
    empty = 1

    def connectionMade(self):
        self.output = LineLogger()
        self.output.tag = self.inst_id
        self.output.makeConnection(transport)


    def outReceived(self, data):
        self.output.dataReceived(data)
        self.empty = data[-1] == '\n'

    errReceived = outReceived


    def processEnded(self, reason):
        if not self.empty:
            self.output.dataReceived('\n')
        self.service.connectionLost(self.inst_id)


class NodeControl(service.Service):
    """
    """

    implements(INodeControl)

    def __init__(self, process_types, reactor):
        """
        process_types is a dict of things you need to start a process
        """
        self.process_types = process_types
        self._reactor = reactor
        self.processes = {}
        self.protocols = {}
        self.timestarted = {}
        self._instance_id = 0 # use id pool here

    def start_process(self, name, number=1):
        """
        Initiates that startup procedure for a process. Does not mean
        process will start instantly.

        Simple first draft interface to starting a process from a
        predefined list
        """
        self._instance_id += 1
        inst_id = self._instance_id
        self.processes[inst_id] = self.process_types[name]
        if self.running:
            self._start_process(inst_id)

    def stop_process(self, inst_id):
        """
        Initiates the shutdown procedure for a process.
        """
        if inst_id not in self.processes:
            raise KeyError('Bad process instance id: %s' % (inst_id,))

        proto = self.protocols.get(inst_id, None)
        if proto is not None:
            proc = proto.transport
            try:
                proc.signalProcess('TERM')
            except error.ProcessExitedAlready:
                pass
            else:
                pass #murder

    def kill_process(self, proc):
        """
        Forceably ensure processes are stopped.
        """
        try:
            proc.signalProcess('KILL')
        except error.ProcessExitedAlready:
            pass


    def list_processes(self):
        """
        """
        return self.processes

    def list_process_types(self):
        """
        """
        return self.process_types

    def _start_process(self, inst_id):
        if inst_id in self.protocols:
            return
        args, uid, gid, env = self.processes[inst_id]

        proto = LoggingProtocol()
        proto.service = self
        proto.inst_id = inst_id
        self.protocols[inst_id] = proto
        self.timestarted[inst_id] = self._reactor.seconds()
        self._reactor.spawnProcess(proto, args[0], args, uid=uid, gid=gid,
                env=env)

    def connectionLost(self, inst_id):
        del self.protocols[inst_id]
        # implement failover strategy here

    def startService(self):
        service.Service.startService(self)
        for inst_id in self.processes:
            self._start_process(inst_id)

    def stopService(self):
        service.Service.stopService(self)

        for inst_id in self.processes:
            self.stop_process(inst_id)

def clientFactory(name='control'):
    node = messaging.Node()
    client = entity.RPCClientEntityFromInterface(name, INodeControl)
    node.addEntity('foo', client, messaging.NChannel)
    local_endpoint = endpoints.TCP4ClientEndpoint(reactor, 'localhost', 'amqp')
    local_endpoint.connect(node)
    return client

if __name__ == "__main__":
    import sys
    from ayps import ayps
    process_types = {
            'consumer': (['python', 'consumer.py'], None, None, None,),
            'producer': (['python', 'producer.py'], None, None, None,),
            }

    test = service.MultiService()

    nodeControl = NodeControl(process_types, reactor)
    nodeControl.setServiceParent(test)
    namespace = locals()
    #aypsShell = ayps.Controller(namespace)
    aypsShell.setServiceParent(test)
    log.startLogging(sys.stdout)
    #nodeControl.start_process('producer')

    nodeControlEntity = entity.RPCEntityFromService(nodeControl)
    node = messaging.Node()
    node.addEntity('control', nodeControlEntity, messaging.RPCChannel)
    local_endpoint = endpoints.TCP4ClientEndpoint(reactor, 'localhost', 'amqp')
    local_endpoint.connect(node)


    test.startService()
    reactor.run()


"""
python process node configuration 

the configuration of the messaging node (or nodes) should be abstracted so
that no knowledge of brokers is necessary in developing an application.

The management and configuration of the nodes available to a system is a
service in itself (just as the tcp middleware / nic card interfaces


Event emitting interface for generic event publishing.
Use push producer and consumer interfaces.

There are different interfaces for different interactions with the Service.
A query interface is an RPC interaction.
A notification interface is an event publishing interaction.
A discovery or system interface is a consuming/subscribing interaction
(listens for broadcast messages).


Name
The supervisor is started with a provided name (inst-n; epu-name-n).
The root supervisor for a vm can be started as a regular python process so
that it can give error codes when it exits.
The epu launcher only ever needs to launch and maintain this one supervisor
"""


