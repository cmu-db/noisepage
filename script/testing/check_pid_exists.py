#!/usr/local/bin/python3
import sys
from util.constants import LOG, CommandLineStr, ErrorCode
from util.common import check_pid_exists


def usage():
    LOG.info("python3 check_pid_exists.py [PID] ([PID_2] [PID_3]...])")
    exit(ErrorCode.ERROR)


def sanitize_args():
    if len(sys.argv) < 2:
        LOG.error("You need to provide at least 1 process id")
        usage()

    pids = []
    for arg in sys.argv[1:]:
        try:
            pids.append(int(arg))
        except ValueError:
            LOG.error("Invalid port value '{}'".format(arg))
            usage()
    return pids


def main():
    pids = sanitize_args()
    res = []
    for pid in pids:
        if check_pid_exists(pid):
            print(CommandLineStr.TRUE)
        else:
            print(CommandLineStr.FALSE)


if __name__ == "__main__":
    main()