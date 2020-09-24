#!/usr/bin/env python3
import sys
import argparse
from util.constants import LOG
from util.common import kill_pids_on_port


def main():
    aparser = argparse.ArgumentParser(
        description="Kill any processes listening on these ports!")
    aparser.add_argument("ports", type=int, nargs="+", help="Ports to check")
    args = vars(aparser.parse_args())

    for port in args.get("ports", []):
        LOG.info(
            "****** start killing processes on port {} ******".format(port))
        kill_pids_on_port(port)
        LOG.info(
            "****** finish killing processes on port {} ******".format(port))


if __name__ == "__main__":
    main()
