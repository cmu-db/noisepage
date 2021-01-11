#!/usr/bin/env python3
import argparse

from util.common import check_pid_exists
from util.constants import CommandLineStr

if __name__ == "__main__":
    aparser = argparse.ArgumentParser(
        description="Check if the given PIDs exist.")
    aparser.add_argument("pids", type=int, nargs="+", help="PIDs to check.")
    args = vars(aparser.parse_args())

    for pid in args.get("pids", []):
        if check_pid_exists(pid):
            print(CommandLineStr.TRUE)
        else:
            print(CommandLineStr.FALSE)
