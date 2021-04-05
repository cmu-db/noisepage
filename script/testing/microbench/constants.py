import os

# Local Data Directory
# Instead of checking with Jenkins, you can build a local repository of results
LOCAL_REPO_DIR = os.path.realpath("local")

# Jenkins URL
JENKINS_URL = "http://jenkins.db.cs.cmu.edu:8080"
# Jenkins project used to compare 30 day average
JENKINS_REF_PROJECT = "terrier-nightly"

# How many historical values are "required" before enforcing the threshold check
MIN_REF_VALUES = 20

# Default failure threshold
# The regression threshold determines how much the benchmark is allowed to get
# slower from the previous runs before it counts as a failure if we are
# using historical data (i.e., if min_ref_values are available).
# You really should not be messing with this value without asking somebody else first.
DEFAULT_FAILURE_THRESHOLD = 10

# The number of threads to use for multi-threaded benchmarks.
# This parameter will be passed in as an environment variable to each benchmark.
BENCHMARK_THREADS = 4

# The path to the logfile for the benchmarks.
BENCHMARK_LOGFILE_PATH = "/tmp/noisepage-benchmark.log"

# Where to find the benchmarks to execute
BENCHMARK_PATH = os.path.realpath("../../build/benchmark/")

# if fewer than min_ref_values are available
LAX_TOLERANCE = 50

# minimum run time for the benchmark, seconds
MIN_TIME = 10
