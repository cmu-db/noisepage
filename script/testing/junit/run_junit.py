#!/usr/bin/python3

import os
import sys
import argparse
import traceback
import git
from git import Repo
noise_path = os.getcwd()+"/noisepage-testfiles"
os.mkdir(noise_path)
repo = Repo.clone_from("https://github.com/dniu16/noisepage-testfiles.git", noise_path)
# invoke git clone with run_command
# if not repo.bare:
#     print('Repo at {} successfully loaded.'.format(repo))
#     tree = repo.heads.master.commit.tree
#     print(tree.blobs)
#     for blob in tree.blobs:
#         print(blob.name)
# else:
#     print("hahaha")
# git.Git(os.getcwd()).clone("https://github.com/cmu-db/noisepage-testfiles.git")
base_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, base_path)
# TODO: turn on junit xml report (within xml), merge junit xml files https://gist.github.com/cgoldberg/4320815
from junit.test_junit import TestJUnit

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
    exit_code = 0
    noise_trace_dir = os.getcwd() + "/noisepage-testfiles/sql_trace/"
    for test_type in os.listdir(noise_trace_dir):
        type_dir = noise_trace_dir + test_type
        if os.path.isdir(type_dir):
            for file in os.listdir(type_dir):
                if "output" in file:
                    path = type_dir + "/" + file
                    os.environ["path"] = path
                    print(path)
                    try:
                        junit = TestJUnit(args)
                        exit_code = junit.run()
                    except:
                        print("Exception trying to run junit tests")
                        print("================ Python Error Output ==================")
                        traceback.print_exc(file=sys.stdout)
                        exit_code = 1
    sys.exit(exit_code)




#     try:
#         junit = TestJUnit(args)
#         exit_code = junit.run()
#     except:
#         print("Exception trying to run junit tests")
#         print("================ Python Error Output ==================")
#         traceback.print_exc(file=sys.stdout)
#         exit_code = 1
#
#     sys.exit(exit_code)
