#!/usr/bin/env python3
import argparse

from util.common import collect_mem_info
from util.constants import MEM_INFO_SPLITTER

if __name__ == "__main__":
    aparser = argparse.ArgumentParser(
        description="Collect the memory info of the given process IDs.")
    aparser.add_argument("pids", type=int, nargs="+", help="PIDs to collect.")
    args = vars(aparser.parse_args())

    for pid in args.get("pids", []):
        mem_info = collect_mem_info(pid)
        rss = mem_info.rss if mem_info else ""
        vms = mem_info.vms if mem_info else ""
        res = f"{rss}{MEM_INFO_SPLITTER}{vms}"
        print(res)
