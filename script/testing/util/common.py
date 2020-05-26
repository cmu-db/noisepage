#!/usr/bin/python3
import os
import sys
import shlex
import subprocess
from xml.etree import ElementTree as et
from xml.dom import minidom


def run_command(command,
                error_msg="",
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=None):
    """
    General purpose wrapper for running a subprocess
    """
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


def write_file(filepath, content):
    """
    General purpose wrapper for writing the file with the given content string
    """
    with open(filepath, "w") as f:
        f.write(content)


def xml_prettify(element):
    """
    Return a pretty-printed XML string for the element.
    """
    raw_str = et.tostring(element, "utf-8")
    parsed = minidom.parseString(raw_str)
    return parsed.toprettyxml(indent="  ")