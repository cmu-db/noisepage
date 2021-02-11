import datetime
import os
import shlex
import subprocess
import sys

import psutil

from . import constants
from .constants import LOG
from .mem_metrics import MemoryInfo


def run_command(command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=None,
                printable=True,
                silent_start=False):
    """
    Run the specified command in a subprocess.

    Parameters
    ----------
    command : str
        The command to run.
    stdout : Optional[IO[AnyStr]]
        File descriptor for stdout.
    stderr : Optional[IO[AnyStr]]
        File descriptor for stderr.
    cwd : Optional[AnyPath]
        The working directory to run the command in.
    printable : bool
        True if the output of the command should be printed.
    silent_start : bool
        True if the command should be logged before it is executed.

    Returns
    -------
    rc : int
        The return code of the subprocess.
    stdout : Optional[IO[AnyStr]]
        The stdout of the subprocess.
    stderr : Optional[IO[AnyStr]]
        The stderr of the subprocess.
    """
    if not silent_start:
        LOG.info(f'Running: {command}')

    p = subprocess.Popen(shlex.split(command),
                         stdout=stdout, stderr=stderr, cwd=cwd)

    while p.poll() is None:
        if printable:
            if stdout == subprocess.PIPE:
                out = p.stdout.readline()
                if out:
                    LOG.info(out.decode("utf-8").rstrip("\n"))

    rc = p.poll()
    return rc, p.stdout, p.stderr


def expect_command(command,
                   stdout=subprocess.PIPE,
                   stderr=subprocess.PIPE,
                   cwd=None,
                   printable=True,
                   silent_start=False):
    """
    Run the specified command in a subprocess.
    Exit if the command errors, otherwise just continue.

    Parameters
    ----------
    command : str
        The command to run.
    stdout : Optional[IO[AnyStr]]
        File descriptor for stdout.
    stderr : Optional[IO[AnyStr]]
        File descriptor for stderr.
    cwd : Optional[AnyPath]
        The working directory to run the command in.
    printable : bool
        True if the output of the command should be printed.
    silent_start : bool
        True if the command should be logged before it is executed.

    Returns
    -------
    None on success. Exits the entire process on error.
    """

    rc, stdout, stderr = \
        run_command(command, stdout, stderr, cwd, printable, silent_start)

    if rc != constants.ErrorCode.SUCCESS:
        LOG.info(stdout.read())
        LOG.error(stderr.read())
        sys.exit(rc)


def run_as_root(command, printable=True, silent_start=False):
    """
    Run the specified command in a subprocess as root.

    Parameters
    ----------
    command : str
        The command to run.
    printable : bool
        True if the output of the command should be printed.
    silent_start : bool
        True if the command should be logged before it is executed.

    Returns
    -------
    rc : int
        The return code of the subprocess.
    stdout : Optional[IO[AnyStr]]
        The stdout of the subprocess.
    stderr : Optional[IO[AnyStr]]
        The stderr of the subprocess.
    """
    sudo_command = "sudo {}".format(command)
    return run_command(sudo_command,
                       stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE,
                       cwd=None,
                       printable=printable,
                       silent_start=silent_start)


def print_file(filename):
    """
    Log the contents of the entire file.

    Parameters
    ----------
    filename : str
        The file to be printed.

    Raises
    ------
    FileNotFoundError if the file doesn't exist.
    """
    try:
        with open(filename) as file:
            for line in file:
                LOG.info(line.strip())
    except FileNotFoundError:
        LOG.error(f"Specified file doesn't exist: {filename}")


def print_pipe(p):
    """
    Log out the subprocess stdout and stderr pipes.

    Parameters
    ----------
    p : Popen[Any]
        The subprocess whose stdout and stderr pipes are to be printed.

    Raises
    ------
    ValueError if some obscure condition occurs.
    """

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
        # TODO(WAN): I don't know how they hit this and whether it is relevant to us.
        # This is a dirty workaround
        LOG.error("Error in subprocess communicate.")
        LOG.error(
            "Known issue in CPython https://bugs.python.org/issue35182. Please upgrade the Python version."
        )


def format_time(timestamp):
    """
    Format the specified timestamp.
    TODO(WAN): Nobody is using this, so I have no clue why it exists.

    Parameters
    ----------
    timestamp
        A POSIX timestamp.

    Returns
    -------
    The formatted timestamp.
    """
    return datetime.datetime.fromtimestamp(timestamp).strftime(
        "%Y-%m-%d %H:%M:%S")


def kill_pids_on_port(port, logger=None):
    """
    Kill all of the processes that are listening on the target port.

    Parameters
    ----------
    port : int
        The port whose listening processes will be killed.
    logger : Optional[Logger]
        The logger to print to instead of stdout.

    Raises
    ------
    RuntimeError if there are any problems with killing PIDs.
    """
    def print_or_log(msg, logger=None):
        if logger:
            logger.info(msg)
        else:
            print(msg)

    if os.getuid() != 0:
        print_or_log("not root user, uid = {}".format(os.getuid()), logger)
        raise Exception("Cannot call this function unless running as root!")

    lsof_path = constants.LSOF_PATH_LINUX
    cmd = f"{lsof_path} -i:{port} | grep 'LISTEN' | awk '{{ print $2 }}'"

    rc, stdout = subprocess.getstatusoutput(cmd)
    if rc != constants.ErrorCode.SUCCESS:
        raise RuntimeError(f"Error running lsof for [PORT={port}], [RC={rc}].")

    for pid_str in stdout.split("\n"):
        try:
            pid = int(pid_str.strip())
            cmd = "kill -9 {}".format(pid)
            print_or_log(f"Running: {cmd}", logger)
            rc, _, _ = run_command(cmd, printable=False)
            if rc != constants.ErrorCode.SUCCESS:
                raise RuntimeError(f"Error in killing PID={pid}, [RC={rc}]")
        except ValueError:
            continue


def check_pid_exists(pid):
    """
    This function must be called while running as root.
    Check if the specified PID exists.

    Parameters
    ----------
    pid : int
        The PID of a process.

    Returns
    -------
    True if the PID exists and false otherwise.
    """
    if os.getuid() != 0:
        raise Exception("Cannot call this function unless running as root!")

    return psutil.pid_exists(pid)


def collect_mem_info(pid):
    """
    Collect the memory information of the provided process.

    Parameters
    ----------
    pid : int
        The PID of a process.
        Either the process must have been spawned by the same user,
        or the caller of this function must have sudo access.

    Returns
    -------
    The memory info of the specified process.
    """
    if not psutil.pid_exists(pid):
        raise Exception("Either PID does not exist or you need sudo.")
    p = psutil.Process(pid)
    return p.memory_info()


def update_mem_info(pid, interval, mem_info_dict):
    """
    Update the provided memory information dictionary at some weird key
    determined by the interval parameter, with the value being the MemoryInfo
    of the provided PID.

    Parameters
    ----------
    pid : int
        The PID to collect memory information for.
    interval : int
        TODO(WAN): ????
    mem_info_dict : dict
        The memory information dictionary to update.
    """
    curr = len(mem_info_dict) * interval
    mem_info_dict[curr] = run_collect_mem_info(pid)


def run_check_pids(pid):
    """
    Fork a subprocess that runs check_pids.py to see if the PID exists.

    Parameters
    ----------
    pid : int
        The PID to check.

    Returns
    -------
    True if the PID exists. False otherwise, including if an error occurred.
    """
    cmd = f"python3 {constants.FILE_CHECK_PIDS} {pid}"
    rc, stdout, _ = run_as_root(cmd, printable=False, silent_start=True)

    if rc != constants.ErrorCode.SUCCESS:
        LOG.error(f"Error in run_check_pid_exists: [PID={pid}]")
        return False

    res_str = stdout.readline().decode("utf-8").rstrip("\n")
    return res_str == constants.CommandLineStr.TRUE


def run_kill_server(port):
    """
    Fork a subprocess that runs kill_server.py to kill all processes
    on the given port.

    Parameters
    ----------
    port : int
        The port whose occupiers will all be killed.

    Returns
    -------
    None on success.

    Raises
    ------
    Exception if any error occurs.
    """
    cmd = f"python3 {constants.FILE_KILL_SERVER} {port}"
    rc, _, _ = run_as_root(cmd)
    if rc != constants.ErrorCode.SUCCESS:
        raise Exception(f"Error in run_kill_server: [PORT={port}]")


def run_collect_mem_info(pid):
    """
    Fork a subprocess that runs collect_mem_info.py to collect memory
    information on the specified PID. The PID is assumed to have been
    started by the same user.

    TODO(WAN):  This used to have sudo privilege.
                But it appears to work just fine locally without sudo privilege,
                and I imagine it should be fine as long as Python script is
                started by the same user (using htop logic).
                The sudo privilege seems unnecessary and is rather scary,
                so I have removed sudo for now.

    Parameters
    ----------
    pid : int
        The PID of the process to collect memory information for.

    Returns
    -------
    mem_info : MemoryInfo
        The memory information for the specified process.
        If an error occurs, MemoryInfo(None, None) is returned.
    """

    """ 
    Fork a subprocess with sudo privilege to collect the memory info for the
    given pid.
    """

    cmd = f"python3 {constants.FILE_COLLECT_MEM_INFO} {pid}"

    rc, stdout, _ = run_command(cmd, printable=False, silent_start=True)

    if rc != constants.ErrorCode.SUCCESS:
        LOG.error("Error with run_collect_mem_info: [PID={}]".format(pid))
        return MemoryInfo(None, None)

    res_str = stdout.readline().decode("utf-8").rstrip("\n")
    rss, vms = res_str.split(constants.MEM_INFO_SPLITTER)
    rss = int(rss) if rss else None
    vms = int(vms) if vms else None
    mem_info = MemoryInfo(rss, vms)
    return mem_info
