"""Helpers for tests that drive real coordinator and node processes."""

from __future__ import annotations

import json
import os
import queue
import signal
import subprocess
import sys
import threading

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PYTHON = sys.executable


def _spawn(args):
    env = dict(os.environ)
    env["PYTHONPATH"] = REPO + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    env["PYTHONUNBUFFERED"] = "1"
    proc = subprocess.Popen(
        [PYTHON] + args,
        cwd=REPO,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return proc


def _drain(proc, timeout=15.0):
    """Wait for the process, closing both pipes so no handles leak."""
    try:
        return proc.wait(timeout=timeout)
    finally:
        for stream in (proc.stdout, proc.stderr):
            if stream is not None:
                stream.close()


def _read_address(proc, timeout=15.0):
    """Read the first stdout line, which every runner prints as JSON."""
    result = queue.Queue()

    def reader():
        line = proc.stdout.readline()
        result.put(line)

    thread = threading.Thread(target=reader, daemon=True)
    thread.start()
    try:
        line = result.get(timeout=timeout)
    except queue.Empty:
        proc.kill()
        _drain(proc)
        raise RuntimeError("the process did not announce its address")
    if not line:
        error = proc.stderr.read()
        _drain(proc)
        raise RuntimeError("the process died: %s" % error)
    return json.loads(line)["address"]


class NodeProcess:
    def __init__(self, ident, targets, db_path, port=0):
        args = [os.path.join(REPO, "tools", "run_node.py"), "--id", ident, "--db", db_path]
        for value in targets:
            args += ["--y", value]
        args += ["--port", str(port)]
        self.ident = ident
        self.db_path = db_path
        self.port = port
        self.proc = _spawn(args)
        self.address = "http://" + _read_address(self.proc).split("://")[-1]

    def kill(self):
        """Hard kill: no cleanup, no graceful shutdown."""
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGKILL)
            _drain(self.proc)

    def stop(self):
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.kill()
        _drain(self.proc)


class CoordinatorProcess:
    def __init__(self, db_path, nodes, port=0):
        args = [os.path.join(REPO, "tools", "run_coordinator.py"), "--db", db_path]
        for ident, url in nodes.items():
            args += ["--node", "%s=%s" % (ident, url)]
        args += ["--port", str(port)]
        self.db_path = db_path
        self.nodes = dict(nodes)
        self.port = port
        self.proc = _spawn(args)
        self.address = "http://" + _read_address(self.proc).split("://")[-1]

    def kill(self):
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGKILL)
            _drain(self.proc)

    def stop(self):
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.kill()
        _drain(self.proc)
