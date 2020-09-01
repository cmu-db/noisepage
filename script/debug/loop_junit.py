#!/usr/bin/env python3

import os
import sys
import shlex
import subprocess

DIR_CURR = os.path.dirname(os.path.realpath(__file__))
DIR_SCRIPT = os.path.dirname(DIR_CURR)
DIR_TESTING = os.path.join(DIR_SCRIPT, "testing")
DIR_JUNIT = os.path.join(DIR_TESTING, "junit")
FILE_RUN_JUNIT = os.path.join(DIR_JUNIT, "run_junit.py")
LOG_FILE = os.path.join(DIR_CURR, "log_loop_junit.log")
CMD = "python3 {} --build-type=debug --query-mode=simple".format(
    FILE_RUN_JUNIT)

if __name__ == "__main__":
    count = 1
    while True:
        p = subprocess.Popen(shlex.split(CMD),
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        print("{}:\t{}".format(count, p.returncode))
        if p.returncode != 0:
            with open(LOG_FILE, "wb+") as f:
                f.write(stdout)
                f.write(stderr)
            exit(1)
        count += 1