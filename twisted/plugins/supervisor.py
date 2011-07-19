
from twisted.application.service import ServiceMaker

RootSupervisor = ServiceMaker(
    "Root Supervisor",
    "anion.supervisortap",
    ("A process watchdog / supervisor"),
    "anion")
