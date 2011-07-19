"""
Support for creating a service which runs a process monitor.
Adapted from twisted.runner.procmontap
"""

from twisted.python import usage
from anion.supervisor import Supervisor, StartableTypeFinder


class Options(usage.Options):

    synopsis = "[anion options] commandline"

    optParameters = [["threshold", "t", 1, "How long a process has to live "
                      "before the death is considered instant, in seconds.",
                      float],
                     ["killtime", "k", 5, "How long a process being killed "
                      "has to get its affairs in order before it gets killed "
                      "with an unmaskable signal.",
                      float],
                     ["minrestartdelay", "m", 1, "The minimum time (in "
                      "seconds) to wait before attempting to restart a "
                      "process", float],
                     ["maxrestartdelay", "M", 3600, "The maximum time (in "
                      "seconds) to wait before attempting to restart a "
                      "process", float]]

    optFlags = []

    zsh_actions = {}


    longdesc = """\
Anion supervisor runs a generic Supervisor/process monitor that can
discover python modules in the cwd. The supervisor can be remotely
commanded to run any of the modules in their own python process.

"""

    def parseArgs(self, *args):
        """
        Grab the command line that is going to be started and monitored
        """
        self['args'] = args


    def postOptions(self):
        """
        Check for dependencies.
        """
        if len(self["args"]) < 1:
            raise usage.UsageError("Please specify a process commandline")



def makeService(config):
    types = StartableTypeFinder()
    s = Supervisor(types)

    s.threshold = config["threshold"]
    s.killTime = config["killtime"]
    s.minRestartDelay = config["minrestartdelay"]
    s.maxRestartDelay = config["maxrestartdelay"]
    s.start_process(config["args"][0])
    return s
