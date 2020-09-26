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
from util.constants import LOG
from util import constants


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


def run_as_root(command, printable=True):
    """
    General purpose wrapper for running a subprocess as root user
    """
    sudo_command = "sudo {}".format(command)
    return run_command(sudo_command,
                       error_msg="",
                       stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE,
                       cwd=None,
                       printable=printable)


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
    """Kill all the PIDs (if any) listening on the target port"""

    if os.getuid() != 0:
        raise Exception("Cannot call this function unless running as root!")

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

    if os.getuid() != 0:
        raise Exception("Cannot call this function unless running as root!")

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

    if os.getuid() != 0:
        raise Exception("Cannot call this function unless running as root!")

    return psutil.pid_exists(pid)


def run_check_pids(pid):
    """ 
    Fork a subprocess with sudo privilege to check if the given pid exists,
    because psutil requires sudo privilege.
    """

    cmd = "python3 {SCRIPT} {PID}".format(SCRIPT=constants.FILE_CHECK_PIDS,
                                          PID=pid)
    rc, stdout, _ = run_as_root(cmd, printable=False)

    if rc != constants.ErrorCode.SUCCESS:
        LOG.error(
            "Error occured in run_check_pid_exists for [PID={}]".format(pid))
        return False

    res_str = stdout.readline().decode("utf-8").rstrip("\n")
    return res_str == constants.CommandLineStr.TRUE


def run_kill_server(port):
    """ 
    Fork a subprocess with sudo privilege to kill all the processes listening 
    to the given port, because psutil requires sudo privilege.
    """

    cmd = "python3 {SCRIPT} {PORT}".format(SCRIPT=constants.FILE_KILL_SERVER,
                                           PORT=port)
    rc, _, _ = run_as_root(cmd)

    if rc != constants.ErrorCode.SUCCESS:
        raise Exception(
            "Error occured in run_check_pid_exists for [PORT={}]".format(port))
