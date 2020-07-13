import os
import sys
import argparse
import traceback
import git
from git import Repo

def is_git_repo(path):
    try:
        _ = git.Repo(path).git_dir
        return True
    except git.exc.InvalidGitRepositoryError:
        return False

def download_git_repo():
    noise_path = os.getcwd()+"/noisepage-testfiles"
    if not os.path.isdir(noise_path):
        os.mkdir(noise_path)
    if not is_git_repo(noise_path):
        repo = Repo.clone_from("https://github.com/dniu16/noisepage-testfiles.git", noise_path)

base_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, base_path)
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

    download_git_repo()
    args = vars(aparser.parse_args())
    exit_code = 0
    code = []
    noise_trace_dir = os.getcwd() + "/noisepage-testfiles/sql_trace/"
    for test_type in os.listdir(noise_trace_dir):
        type_dir = noise_trace_dir + test_type
        if os.path.isdir(type_dir):
            for file in os.listdir(type_dir):
                if "output" in file:
                    path = type_dir + "/" + file
                    os.environ["path"] = path
                    try:
                        print(os.environ["path"])
                        junit = TestJUnit(args)
                        exit_code = junit.run()
                        code.append(exit_code)

                    except:
                        print("Exception trying to run junit tests")
                        print("================ Python Error Output ==================")
                        traceback.print_exc(file=sys.stdout)
                        exit_code = 1
    final_code = 0
    for c in code:
        final_code = final_code or c
    print("Final exit code: " + str(final_code))
    sys.exit(exit_code)