#!/usr/bin/python3
import os
import sys
import shlex
import subprocess
import re
import signal
import errno
import psutil
import datetime
from util.constants import LOG, FILE_CHECK_PID_EXISTS, FILE_KILL_SERVER_ON_PORT, CommandLineStr, ErrorCode


def run_command(command,
                error_msg="",
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=None,
                printable=True):
    """
    General purpose wrapper for running a subprocess
    """
    p = subprocess.Popen(shlex.split(command),
                         stdout=stdout,
                         stderr=stderr,
                         cwd=cwd)

    while p.poll() is None:
        if printable:
            if stdout == subprocess.PIPE:
                out = p.stdout.readline()
                if out:
                    LOG.info(out.decode("utf-8").rstrip("\n"))

    rc = p.poll()
    return rc, p.stdout, p.stderr


def print_output(filename):
    """ Print out contents of a file """
    with open(filename) as file:
        lines = file.readlines()
        for line in lines:
            LOG.info(line.strip())


def format_time(timestamp):
    return datetime.datetime.fromtimestamp(timestamp).strftime(
        "%Y-%m-%d %H:%M:%S")


def kill_pids_on_port(port):
    for proc in psutil.process_iter():
        try:
            for conns in proc.connections(kind="inet"):
                if conns.laddr.port == port:
                    LOG.info(
                        "Killing existing server instance listening on port {} [PID={}], created at {}"
                        .format(port, proc.pid,
                                format_time(proc.create_time())))
                    proc.send_signal(signal.SIGKILL)
        except psutil.ZombieProcess:
            LOG.info("Killing zombie process [PID={}]".format(proc.pid))
            proc.parent().send_signal(signal.SIGCHLD)


def get_pids_on_port(port):
    """Get the list of PIDs (if any) listening on the target port"""

    pids = []
    for proc in psutil.process_iter():
        try:
            for conns in proc.connections(kind="inet"):
                if conns.laddr.port == port:
                    pids.append(proc.pid)
        except psutil.ZombieProcess:
            # ignore the zombie process
            continue
    return pids


def check_pid_exists(pid):
    """ Checks to see if the pid exists """
    return psutil.pid_exists(pid)


def run_check_pid_exists(pid):
    cmd = "sudo python3 {SCRIPT} {PID}".format(SCRIPT=FILE_CHECK_PID_EXISTS,
                                               PID=pid)
    rc, stdout, _ = run_command(cmd,
                                error_msg="",
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                cwd=None,
                                printable=False)
    if rc != ErrorCode.SUCCESS:
        LOG.error(
            "Error occured in run_check_pid_exists for [PID={}]".format(pid))
        return False
    res_str = stdout.readline().decode("utf-8").rstrip("\n")
    return res_str == CommandLineStr.TRUE


def run_kill_pids_on_port(port):
    cmd = "sudo python3 {SCRIPT} {PORT}".format(
        SCRIPT=FILE_KILL_SERVER_ON_PORT, PORT=port)
    rc, _, _ = run_command(cmd)
    return rc
