#!/usr/bin/env python3

import os
import sys
import argparse
import traceback
import git
import glob
from git import Repo

base_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, base_path)

from junit import constants
from junit.test_junit import TestJUnit
from util.constants import LOG

def is_git_repo(path):
    try:
        _ = git.Repo(path).git_dir
        return True
    except git.exc.InvalidGitRepositoryError:
        return False

def download_git_repo():
    trace_path = os.path.join(os.getcwd(), constants.TESTFILES_REPO)
    if not os.path.isdir(trace_path):
        os.mkdir(trace_path)
    if not is_git_repo(trace_path):
        repo = Repo.clone_from(constants.TESTFILES_REPO_URL, trace_path)

if __name__ == "__main__":

    aparser = argparse.ArgumentParser(description="junit runner")

    aparser.add_argument("--db-host", help="DB Hostname")
    aparser.add_argument("--db-port", type=int, help="DB Port")
    aparser.add_argument("--db-output-file", help="DB output log file")
    aparser.add_argument("--test-output-file", help="Test output log file")
    aparser.add_argument("--build-type",
                         default="debug",
                         choices=["debug", "release", "relwithdebinfo"],
                         help="Build type (default: %(default)s)")
    aparser.add_argument("--query-mode",
                         choices=["simple", "extended"],
                         help="Query protocol mode")
    aparser.add_argument("--prepare-threshold",
                         type=int,
                         help="Threshold under the 'extened' query mode")

    args = vars(aparser.parse_args())
    
    
    # HACK: We manually download the Git repo for the testfiles.
    # This needs to be switched to using true Git submodules
    download_git_repo()
    
    all_exit_codes = []
    
    # Step 1: Run the regular JUnit tests. 
    exit_code = 0
    try:
        runner = TestJUnit(args)
        runner.test_command = constants.JUNIT_TEST_CMD_JUNIT
        exit_code = runner.run()
    except:
        LOG.error("Exception trying to run '%s'" % constants.JUNIT_TEST_CMD_JUNIT)
        LOG.error("================ Python Error Output ==================")
        traceback.print_exc(file=sys.stdout)
        exit_code = 1
    finally:
        all_exit_codes.append(exit_code)
    
        
    # Step 2: Run the trace test for each file that we find
    # Each directory represents another set of SQL traces to test.
    noise_trace_dir = os.path.join(os.getcwd(), constants.TESTFILES_REPO, constants.TESTFILES_REPO_TRACE_DIR)
    for test_type in os.listdir(noise_trace_dir):
        type_dir = os.path.join(noise_trace_dir, test_type)
        if not os.path.isdir(type_dir): continue

        # Look for all of the .test files in the each directory
        for file in glob.glob(os.path.join(type_dir, "*" + constants.TESTFILES_PREFIX)):
            os.environ["NOISEPAGE_TRACE_FILE"] = os.path.join(type_dir, file)
            LOG.info(os.environ["NOISEPAGE_TRACE_FILE"])
            
            exit_code = 0
            try:
                runner = TestJUnit(args)
                runner.test_command = constants.JUNIT_TEST_CMD_TRACE
                exit_code = runner.run()
            except KeyboardInterrupt:
                exit_code = 1
                raise
            except:
                LOG.error("Exception trying to run '%s'" % constants.JUNIT_TEST_CMD_TRACE)
                LOG.error("================ Python Error Output ==================")
                traceback.print_exc(file=sys.stdout)
                exit_code = 1
            finally:
                all_exit_codes.append(exit_code)
            LOG.info("="*80)
        ## FOR (files)
    ## FOR (dirs)
    
    # Compute final exit code. If any test failed, then the entire program has to fail
    final_code = 0
    for c in all_exit_codes:
        final_code = final_code or c
    sys.exit(final_code)
# MAIN
