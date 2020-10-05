#!/usr/bin/env python3
import os
import sys
import argparse
from util.common import kill_pids_on_port

if __name__ == "__main__":
    aparser = argparse.ArgumentParser(
        description="Kill any processes listening on these ports!")
    aparser.add_argument("ports", type=int, nargs="+", help="Ports to check")
    args = vars(aparser.parse_args())

    for port in args.get("ports", []):
        print("****** start killing processes on port {} ******".format(port))
        kill_pids_on_port(port)
        print("****** finish killing processes on port {} ******".format(port))
