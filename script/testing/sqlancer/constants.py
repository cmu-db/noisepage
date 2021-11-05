import os

from ..util.constants import DIR_TMP

# git settings for sqlancer.
SQLANCER_GIT_URL = "https://github.com/dniu16/sqlancer.git"
SQLANCER_GIT_LOCAL_PATH = os.path.join(DIR_TMP, "sqlancer")
SQLANCER_GIT_CLEAN_COMMAND = "rm -rf {}".format(SQLANCER_GIT_LOCAL_PATH)
SQLANCER_GIT_CLONE_COMMAND = "git clone {} {}".format(SQLANCER_GIT_URL,
                                                 SQLANCER_GIT_LOCAL_PATH)
# command to run sqlancer
SQLANCER_TEST_CMD_ALL = "java -jar sqlancer-1.1.0.jar --num-threads 1 --num-tries 10 noisepage --oracle NoREC"

# directory to run sqlancer in
SQLANCER_TEST_CMD_CWD = os.path.join(SQLANCER_GIT_LOCAL_PATH, "target")
SQLANCER_OUTPUT_FILE = os.path.join(SQLANCER_GIT_LOCAL_PATH, "res.txt")

# command to build sqlancer
SQLANCER_BUILD_COMMAND = "mvn package -DskipTests"
