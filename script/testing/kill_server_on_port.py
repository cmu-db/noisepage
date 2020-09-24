#!/usr/local/bin/python3
import sys
from util.constants import LOG, ErrorCode
from util.common import kill_pids_on_port


def usage():
    LOG.error("python3 kill_server_on_port.py [PORT] ([PORT_2] [PORT_3]...])")
    exit(ErrorCode.ERROR)


def sanitize_args():
    if len(sys.argv) < 2:
        LOG.error("You need to provide at least 1 port number")
        usage()

    ports = []
    for arg in sys.argv[1:]:
        try:
            ports.append(int(arg))
        except ValueError:
            LOG.error("Invalid port value '{}'".format(arg))
            usage()
    return ports


def main():
    ports = sanitize_args()
    for port in ports:
        LOG.info(
            "****** start killing processes on port {} ******".format(port))
        kill_pids_on_port(port)
        LOG.info(
            "****** finish killing processes on port {} ******".format(port))


if __name__ == "__main__":
    main()