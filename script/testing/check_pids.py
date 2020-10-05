#!/usr/local/bin/python3
import os
import sys
import argparse
from util.constants import CommandLineStr, ErrorCode
from util.common import check_pid_exists

if __name__ == "__main__":
    aparser = argparse.ArgumentParser(
        description="Check if the given pids exist.")
    aparser.add_argument("pids", type=int, nargs="+", help="Pids to check")
    args = vars(aparser.parse_args())

    for pid in args.get("pids", []):
        if check_pid_exists(pid):
            print(CommandLineStr.TRUE)
        else:
            print(CommandLineStr.FALSE)