#!/usr/bin/python3
import os
import sys
import shlex
import subprocess


def run_command(command,
                error_msg="",
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=None):
    p = subprocess.Popen(shlex.split(command),
                         stdout=stdout,
                         stderr=stderr,
                         cwd=cwd)

    while p.poll() is None:
        if stdout == subprocess.PIPE:
            out = p.stdout.readline()
            if out:
                print(out.decode("utf-8").rstrip("\n"))

    rc = p.poll()
    return rc, p.stdout, p.stderr