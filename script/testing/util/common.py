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
from util.constants import LOG, LSOF_PATH_MACOS, LSOF_PATH_LINUX, ErrorCode
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
    LOG.info("in run_command | command = " + command)
    p = subprocess.Popen(shlex.split(command),
                         stdout=stdout,
                         stderr=stderr,
                         cwd=cwd)

    LOG.info("in run_command | subproces created, pid = {}".format(p.pid))
    while p.poll() is None:
        if printable:
            if stdout == subprocess.PIPE:
                out = p.stdout.readline()
                if out:
                    LOG.info(out.decode("utf-8").rstrip("\n"))

    rc = p.poll()
    LOG.info("in run_command | rc = {}".format(rc))
    return rc, p.stdout, p.stderr


def run_as_root(command, printable=True):
    """
    General purpose wrapper for running a subprocess as root user
    """
    sudo_command = "sudo {}".format(command)
    LOG.info("sudo command: '{}'".format(sudo_command))
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


def print_pipe(stdout=None, stderr=None):
    """ Print out the memory buffer of subprocess pipes """
    if stdout:
        for line in stdout.split("\n"):
            LOG.info(line.decode("utf-8").rstrip("\n"))
    if stderr:
        for line in stderr.split("\n"):
            LOG.error(line.decode("utf-8").rstrip("\n"))


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

    print("in kill_pids_on_port: user id = {}".format(os.getuid()))
    if os.getuid() != 0:
        print_or_log("not root user, uid = {}".format(os.getuid()), logger)
        raise Exception("Cannot call this function unless running as root!")

    # get the command of lsof based on the os platform
    lsof_path = LSOF_PATH_MACOS if sys.platform.startswith(
        "darwin") else LSOF_PATH_LINUX

    cmd = "{LSOF_PATH} -i:{PORT} | grep 'LISTEN' | awk '{print $2}'".format(
        LSOF_PATH=lsof_path, PORT=port)

    rc, stdout, stderr = run_as_root(cmd, printable=False)
    if rc != ErrorCode.SUCCESS:
        raise Exception(
            "Error in running 'lsof' to get processes listening to PORT={PORT}, [RC={RC}]"
            .format(PORT=port, RC=rc))

    pids = [
        int(pid_str.decode("utf-8").rstrip("\n"))
        for pid_str in stdout.split("\n")
    ]
    for pid in pids:
        cmd = "kill -9 {}".format(pid)
        rc, _, _ = run_as_root(cmd, printable=False)
        if rc != ErrorCode.SUCCESS:
            raise Exception("Error in killing PID={PID}, [RC={RC}]".format(
                PID=pid, RC=rc))

    # # psutil failed attempt: AccessDenied
    # count = 0
    # for proc in psutil.process_iter():
    #     print("iter {}".format(count))
    #     count += 1
    #     try:
    #         for conns in proc.connections(kind="inet"):
    #             if conns.laddr.port == port:
    #                 print_or_log(
    #                     "Killing existing server instance listening on port {} [PID={}], created at {}"
    #                     .format(port, proc.pid,
    #                             format_time(proc.create_time())), logger)
    #                 proc.send_signal(signal.SIGKILL)
    #     except psutil.ZombieProcess:
    #         print_or_log("Killing zombie process [PID={}]".format(proc.pid),
    #                      logger)
    #         proc.parent().send_signal(signal.SIGCHLD)
    #     except Exception as e:
    #         # get more generic debug info
    #         print_or_log(e)
    #         raise

    # # psutil failed attempt: subprocess rc = -9
    # try:
    #     for conns in psutil.net_connections(kind="inet"):
    #         if conns.laddr.port == port:
    #             proc = psutil.Process(pid=conns.pid)
    #             if proc is None:
    #                 continue
    #             print_or_log(
    #                 "Killing existing server instance listening on port {} [PID={}], created at {}"
    #                 .format(port, proc.pid,
    #                         format_time(proc.create_time())), logger)
    #             proc.send_signal(signal.SIGKILL)
    # except psutil.ZombieProcess:
    #     print_or_log("Killing zombie process [PID={}]".format(proc.pid),
    #                  logger)
    #     proc.parent().send_signal(signal.SIGCHLD)
    # except Exception as e:
    #     # get more generic debug info
    #     print_or_log(e)
    #     raise


def get_pids_on_port(port):
    """Get the list of PIDs (if any) listening on the target port"""

    print("in get_pids_on_port: user id = {}".format(os.getuid()))
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
            "Error occured in run_kill_server for [PORT={}]".format(port))
