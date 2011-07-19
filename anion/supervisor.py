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
from anion import util


class ISupervisor(Interface):
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



class ProcessMonitor(service.Service):
    """
    ProcessMonitor runs processes, monitors their progress, and restarts
    them when they die.

    The ProcessMonitor will not attempt to restart a process that appears to
    die instantly -- with each "instant" death (less than 1 second, by
    default), it will delay approximately twice as long before restarting
    it.  A successful run will reset the counter.

    The primary interface is L{addProcess} and L{removeProcess}. When the
    service is running (that is, when the application it is attached to is
    running), adding a process automatically starts it.

    Each process has a name. This name string must uniquely identify the
    process.  In particular, attempting to add two processes with the same
    name will result in a C{KeyError}.

    @type threshold: C{float}
    @ivar threshold: How long a process has to live before the death is
        considered instant, in seconds.  The default value is 1 second.

    @type killTime: C{float}
    @ivar killTime: How long a process being killed has to get its affairs
        in order before it gets killed with an unmaskable signal.  The
        default value is 5 seconds.

    @type minRestartDelay: C{float}
    @ivar minRestartDelay: The minimum time (in seconds) to wait before
        attempting to restart a process.  Default 1s.

    @type maxRestartDelay: C{float}
    @ivar maxRestartDelay: The maximum time (in seconds) to wait before
        attempting to restart a process.  Default 3600s (1h).

    @type _reactor: L{IReactorProcess} provider
    @ivar _reactor: A provider of L{IReactorProcess} and L{IReactorTime}
        which will be used to spawn processes and register delayed calls.

    @note Adapted from twisted.runner.procmon

    """
    threshold = 1
    killTime = 5
    minRestartDelay = 1
    maxRestartDelay = 3600

    processProtocol = LoggingProtocol # assuming only one kind of process


    def __init__(self, reactor=reactor):
        self._reactor = reactor

        self.processes = {}
        self.protocols = {}
        self.delay = {}
        self.timeStarted = {}
        self.murder = {}
        self.restart = {}

    def __getstate__(self):
        dct = service.Service.__getstate__(self)
        del dct['_reactor']
        dct['protocols'] = {}
        dct['delay'] = {}
        dct['timeStarted'] = {}
        dct['murder'] = {}
        dct['restart'] = {}
        return dct


    def addProcess(self, name, args, uid=None, gid=None, env={}, path=None):
        """
        Add a new monitored process and start it immediately if the
        L{ProcessMonitor} service is running.

        Note that args are passed to the system call, not to the shell. If
        running the shell is desired, the common idiom is to use
        C{ProcessMonitor.addProcess("name", ['/bin/sh', '-c', shell_script])}

        @param name: A name for this process.  This value must be
            unique across all processes added to this monitor.
        @type name: C{str}
        @param args: The argv sequence for the process to launch.
        @param uid: The user ID to use to run the process.  If C{None},
            the current UID is used.
        @type uid: C{int}
        @param gid: The group ID to use to run the process.  If C{None},
            the current GID is used.
        @type uid: C{int}
        @param env: The environment to give to the launched process. See
            L{IReactorProcess.spawnProcess}'s C{env} parameter.
        @type env: C{dict}
        @raises: C{KeyError} if a process with the given name already
            exists
        """
        if name in self.processes:
            raise KeyError("remove %s first" % (name,))
        self.processes[name] = args, uid, gid, env, path
        self.delay[name] = self.minRestartDelay
        if self.running:
            self.startProcess(name)


    def removeProcess(self, name):
        """
        Stop the named process and remove it from the list of monitored
        processes.

        @type name: C{str}
        @param name: A string that uniquely identifies the process.
        """
        self.stopProcess(name)
        del self.processes[name]


    def startService(self):
        """
        Start all monitored processes.
        """
        service.Service.startService(self)
        for name in self.processes:
            self.startProcess(name)


    def stopService(self):
        """
        Stop all monitored processes and cancel all scheduled process restarts.
        """
        service.Service.stopService(self)

        # Cancel any outstanding restarts
        for name, delayedCall in self.restart.items():
            if delayedCall.active():
                delayedCall.cancel()

        for name in self.processes:
            self.stopProcess(name)


    def connectionLost(self, name):
        """
        Called when a monitored processes exits. If
        L{ProcessMonitor.running} is C{True} (ie the service is started), the
        process will be restarted.
        If the process had been running for more than
        L{ProcessMonitor.threshold} seconds it will be restarted immediately.
        If the process had been running for less than
        L{ProcessMonitor.threshold} seconds, the restart will be delayed and
        each time the process dies before the configured threshold, the restart
        delay will be doubled - up to a maximum delay of maxRestartDelay sec.

        @type name: C{str}
        @param name: A string that uniquely identifies the process
            which exited.
        """
        # Cancel the scheduled _forceStopProcess function if the process
        # dies naturally
        if name in self.murder:
            if self.murder[name].active():
                self.murder[name].cancel()
            del self.murder[name]

        del self.protocols[name]

        if self._reactor.seconds() - self.timeStarted[name] < self.threshold:
            # The process died too fast - backoff
            nextDelay = self.delay[name]
            self.delay[name] = min(self.delay[name] * 2, self.maxRestartDelay)

        else:
            # Process had been running for a significant amount of time
            # restart immediately
            nextDelay = 0
            self.delay[name] = self.minRestartDelay

        # Schedule a process restart if the service is running
        if self.running and name in self.processes:
            self.restart[name] = self._reactor.callLater(nextDelay,
                                                         self.startProcess,
                                                         name)


    def startProcess(self, name):
        """
        @param name: The name of the process to be started
        """
        # If a protocol instance already exists, it means the process is
        # already running
        if name in self.protocols:
            return

        args, uid, gid, env, path = self.processes[name]

        proto = self.processProtocol()
        proto.service = self
        proto.name = name
        self.protocols[name] = proto
        self.timeStarted[name] = self._reactor.seconds()
        self._reactor.spawnProcess(proto, args[0], args, uid=uid,
                                          gid=gid, env=env, path=path)


    def _forceStopProcess(self, proc):
        """
        @param proc: An L{IProcessTransport} provider
        """
        try:
            proc.signalProcess('KILL')
        except error.ProcessExitedAlready:
            pass


    def stopProcess(self, name):
        """
        @param name: The name of the process to be stopped
        """
        if name not in self.processes:
            raise KeyError('Unrecognized process name: %s' % (name,))

        proto = self.protocols.get(name, None)
        if proto is not None:
            proc = proto.transport
            try:
                proc.signalProcess('TERM')
            except error.ProcessExitedAlready:
                pass
            else:
                self.murder[name] = self._reactor.callLater(
                                            self.killTime,
                                            self._forceStopProcess, proc)


    def restartAll(self):
        """
        Restart all processes. This is useful for third party management
        services to allow a user to restart servers because of an outside change
        in circumstances -- for example, a new version of a library is
        installed.
        """
        for name in self.processes:
            self.stopProcess(name)


    def __repr__(self):
        l = []
        for name, proc in self.processes.items():
            uidgid = ''
            if proc[1] is not None:
                uidgid = str(proc[1])
            if proc[2] is not None:
                uidgid += ':'+str(proc[2])

            if uidgid:
                uidgid = '(' + uidgid + ')'
            l.append('%r%s: %r' % (name, uidgid, proc[0]))
        return ('<' + self.__class__.__name__ + ' '
                + ' '.join(l)
                + '>')

class Supervisor(ProcessMonitor):
    """
    A Supervisor for starting and monitoring operating system processes.
    The twisted procmon is a watchdog that keeps processes alive.
    This generalizes that and also provides a way to start more than one of
    the same process type. 
    There is also the idea of a table of process types that can be started
    by a single abstract name with optional arguments. 
    """

    implements(ISupervisor)

    def __init__(self, process_types, reactor=reactor):
        """
        process_types is a dict of things you need to start a process
        """
        ProcessMonitor.__init__(self, reactor=reactor)
        self.process_types = process_types
        self._id_pool = util.IDPool()

    def list_process_types(self):
        return self.process_types.get_process_types()

    def start_process(self, name, number=1):
        """
        Initiates that startup procedure for a process. Does not mean
        process will start instantly.

        Simple first draft interface to starting a process from a
        predefined list
        """
        inst_id = self._id_pool.get_id()
        process_type = self.process_types.get_process_type(name)
        self.addProcess(inst_id, process_type['args'],
                                 uid=process_type['uid'],
                                 gid=process_type['gid'],
                                 path=process_type['path'],
                                 env=process_type['env'])

        return inst_id

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


from twisted.python import modules

class StartableTypeFinder(object):
    """
    There are a few different ways to keep track of processes that can be
    started by name in a certain namespace.

    A Package/App will have a name (like Twisted or Anion) in a certain
    namespace (Pypi is one namespace; a directory of packages in a file
    system is another namespace).

    A package supporting distributed processes might have a namespace of
    it's own. It might want to run a python module or some non-python
    program.

    Look for all modules in the current directory
    mods = modules.PythonPath('.') 
    mods.iterModules()

    When the set of modules is iterated over, a new set of structures need
    for starting with the process monitor are created.

    The first version just maps the name of the modules within packages
    with out the py extension. Assume that is the only way to find things
    to run.

    """

    def __init__(self, pack_root='.'):
        """
        Assume this the cwd is where we look
        """
        self.pack_root = pack_root
        self.process_types = self._load_available_types()

    def _load_available_types(self):
        packs_path = modules.PythonPath(self.pack_root)
        packs = packs_path.iterModules()
        _available_types = {}
        for pack in packs:
            mods = pack.iterModules()
            for mod in mods:
                mod_file = mod.filePath
                _available_types[mod.name] = {
                                            'file': mod_file.basename(),
                                            'path': mod_file.dirname(),
                                            }
        return _available_types


    def list_process_types(self):
        return self.process_types.keys()

    def get_process_type(self, name):
        if name in self.process_types:
            file = self.process_types[name]['file']
            path = self.process_types[name]['path']
            return {'executable':'python',
                    'args': ['python', file], 
                    'env': None,
                    'path': path,
                    'uid': None, 
                    'gid': None
                    }




