#!/usr/bin/python3
import os
import sys
import shlex
import subprocess


def run_command(command, error_msg=""):
    p = subprocess.Popen(shlex.split(command),
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    while True:
        output = p.stdout.readline()
        if output == "" and p.poll() is not None:
            break
        if output:
            print output.strip()
    rc = p.poll()
    return rc