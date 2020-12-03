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
from util import constants
from util.constants import LOG
from util.mem_metrics import MemoryInfo
from collections import namedtuple


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


def print_file(filename):
    """ Print out contents of a file """
    try:
        with open(filename) as file:
            lines = file.readlines()
            for line in lines:
                LOG.info(line.strip())
    except FileNotFoundError:
        LOG.error("file not exists: '{}'".format(filename))


def print_pipe(p):
    """ Print out the memory buffer of subprocess pipes """
    try:
        stdout, stderr = p.communicate()
        if stdout:
            for line in stdout.decode("utf-8").rstrip("\n").split("\n"):
                LOG.info(line)
        if stderr:
            for line in stdout.decode("utf-8").rstrip("\n").split("\n"):
                LOG.error(line)
    except ValueError:
        # This is a dirty workaround
        LOG.error("Error in subprocess communicate")
        LOG.error(
            "Known issue in CPython https://bugs.python.org/issue35182. Please upgrade the Python version."
        )


def format_time(timestamp):
    return datetime.datetime.fromtimestamp(timestamp).strftime(
        "%Y-%m-%d %H:%M:%S")


def print_or_log(msg, logger=None):
    if logger:
        logger.info(msg)
    else:
        print(msg)


def kill_pids_on_port(port, logger=None):
    """Kill all the PIDs (if any) listening on the target port"""

    if os.getuid() != 0:
        print_or_log("not root user, uid = {}".format(os.getuid()), logger)
        raise Exception("Cannot call this function unless running as root!")

    # get the command of lsof based on the os platform
    lsof_path = constants.LSOF_PATH_MACOS if sys.platform.startswith(
        constants.OS_FAMILY_DARWIN) else constants.LSOF_PATH_LINUX

    cmd = "{LSOF_PATH} -i:{PORT} | grep 'LISTEN' | awk '{{ print $2 }}'".format(
        LSOF_PATH=lsof_path, PORT=port)

    rc, stdout = subprocess.getstatusoutput(cmd)
    if rc != constants.ErrorCode.SUCCESS:
        raise Exception(
            "Error in running 'lsof' to get processes listening to PORT={PORT}, [RC={RC}]"
            .format(PORT=port, RC=rc))

    for pid_str in stdout.split("\n"):
        try:
            pid = int(pid_str.strip())
            cmd = "kill -9 {}".format(pid)
            rc, _, _ = run_command(cmd, printable=False)
            if rc != constants.ErrorCode.SUCCESS:
                raise Exception("Error in killing PID={PID}, [RC={RC}]".format(
                    PID=pid, RC=rc))
        except ValueError:
            continue


def check_pid_exists(pid):
    """ Checks to see if the pid exists """

    if os.getuid() != 0:
        raise Exception("Cannot call this function unless running as root!")

    return psutil.pid_exists(pid)


def collect_mem_info(pid):
    """
    Collect the memory info of the process if the pid exists.

    Precondition:
    Looks like collecting the mem info for the process belongs to the same user
    does not require escalated privilege.
    """
    if not psutil.pid_exists(pid):
        return None
    p = psutil.Process(pid)
    return p.memory_info()


def update_mem_info(pid, interval, mem_info_dict):
    """
    Update the mem_info dict by appending the memory info of the given pid at
    the current time in seconds.
    """
    curr = len(mem_info_dict) * interval
    mem_info_dict[curr] = run_collect_mem_info(pid)


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
            "Error occured in run_kill_server for [PORT={}]".format(port))


def run_collect_mem_info(pid):
    """ 
    Fork a subprocess with sudo privilege to collect the memory info for the
    given pid.
    """

    cmd = "python3 {SCRIPT} {PID}".format(
        SCRIPT=constants.FILE_COLLECT_MEM_INFO, PID=pid)

    rc, stdout, _ = run_as_root(cmd, printable=False)

    if rc != constants.ErrorCode.SUCCESS:
        LOG.error(
            "Error occured in run_collect_mem_info for [PID={}]".format(pid))
        return False

    res_str = stdout.readline().decode("utf-8").rstrip("\n")
    rss, vms = res_str.split(constants.MEM_INFO_SPLITTER)
    rss = int(rss) if rss else None
    vms = int(vms) if vms else None
    mem_info = MemoryInfo(rss, vms)
    return mem_info
