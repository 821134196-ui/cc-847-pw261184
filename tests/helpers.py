"""Shared test helpers."""

from __future__ import annotations

import os
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from minidb import Store, initialise  # noqa: E402
from minidb.faults import FaultPlan, Injector  # noqa: E402


class TempDirMixin(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="minidb-test-")
        self.dbdir = os.path.join(self.tmp, "db")
        initialise(self.dbdir)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)


class CrashSimulator:
    """Run a function in a child process that is killed by a fault plan.

    Returns the child's exit status.  ``os._exit(90)`` means the injector
    fired; 0 means the function finished without hitting its fault.
    """

    def __init__(self, dbdir: str, plan: FaultPlan, capacity: int = 2):
        self.dbdir = dbdir
        self.plan = plan
        self.capacity = capacity

    def run(self, body: str, timeout: float = 30) -> int:
        import subprocess

        script = f"""
import os, sys
sys.path.insert(0, {os.path.dirname(os.path.dirname(os.path.abspath(__file__)))!r})
from minidb import Store
from minidb.faults import FaultPlan, Injector
plan = FaultPlan(**{self.plan.as_dict()!r})
inj = Injector(plan, exit_process=True)
store = Store({self.dbdir!r}, capacity={self.capacity}, injector=inj)
{body}
store.close()
"""
        proc = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            timeout=timeout,
        )
        return proc.returncode

    def run_script(self, script_path: str, timeout: float = 30) -> int:
        import subprocess

        proc = subprocess.run(
            [
                sys.executable, "-m", "minidb", "run", self.dbdir, script_path,
                "--capacity", str(self.capacity), "--fault", self.plan.to_json(),
                "--in-process",
            ],
            capture_output=True,
            timeout=timeout,
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        )
        return proc.returncode


def open_store(dbdir: str, capacity: int = 8) -> Store:
    return Store(dbdir, capacity=capacity)


def rows_as_dict(store: Store) -> dict[int, tuple[int, str]]:
    return {key: (value, tag) for _p, _s, key, value, tag in store.scan()}
