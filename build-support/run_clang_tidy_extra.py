#!/usr/bin/env python

"""
A helper class, to suppress execution of clang-tidy.

In clang-tidy-6.0, if the clang-tidy configuration file suppresses ALL checks,
(e.g. via a .clang-tidy file), clang-tidy will print usage information and
exit with a return code of 0. Harmless but verbose. In later versions of
clang-tidy the return code becomes 1, making this a bigger problem.

This helper addresses the problem by suppressing execution according to
the configuration in this file.
"""

import re
import os


class CheckConfig(object):
    """ Check paths against the built-in config """

    def __init__(self, apply_git_filter=True):
        # debug prints
        self.debug = False
        self.apply_git_filter = apply_git_filter
        self._init_config()
        return

    def _init_config(self):
        """ Any path matching one of the ignore_pats regular expressions,
            denotes that we do NOT want to run clang-tidy on that item.
        """
        self.ignore_pats = ["/third_party/"]

        if self.apply_git_filter:
            """ Takes the relative complement of tracked files & modified files.
                The result is a list of all elements that were NOT modified.
                We tell clang-tidy to skip these files to speed up builds.
                
                Git is told to look relative to the root path (:/)
                We tell git to only diff into the past (...) 
                We also tell git to only diff new or modified files (filter=AM)
            """
            with os.popen('(git ls-files --full-name :/;'
                          ' git diff origin/master... --name-only --diff-filter=AM)' 
                          '| sort | uniq -u') as diff:
                for line in diff:
                    self.ignore_pats.append("/%s" % line.strip())

    def should_skip(self, path):
        """ Should execution of clang-tidy be skipped?
            path - to check, against the configuration.
                   Typically the full path.
            returns - False if we want to run clang-tidy
                      True of we want to skip execution on this item
        """
        for pat in self.ignore_pats:
            if pat in path:
                if self.debug:
                    print("match pat: {}, {} => don't run".format(pat, path))
                return True
        return False
