#!/usr/bin/env python3

import sys
import traceback

from script.testing.sqlancer.test_sqlancer import TestSqlancer
from ..util.constants import LOG, ErrorCode
from .constants import (SQLANCER_TEST_CMD_ALL, SQLANCER_TEST_CMD_CWD)
from .test_case_sqlancer import TestCaseSqlancer
from .utils import parse_command_line_args


def run_test(t_server, test_command, test_command_cwd):
    test_case = TestCaseSqlancer(args, test_command=test_command, test_command_cwd=test_command_cwd)
    ecode = ErrorCode.ERROR
    try:
        ecode = t_server.run([test_case])
    except KeyboardInterrupt:
        LOG.error("KeyboardInterrupt received. Terminating.")
        raise
    except Exception as err:
        LOG.error(f'Exception trying to run {test_command}')
        LOG.error(err)
        LOG.error("================ Python Error Output ==================")
        traceback.print_exc(file=sys.stdout)
    return ecode


if __name__ == "__main__":
    args = parse_command_line_args()
    errcode = 0
    try:
        test_server = TestSqlancer(args)
        errcode = run_test(test_server, SQLANCER_TEST_CMD_ALL, SQLANCER_TEST_CMD_CWD)
    except:
        LOG.error("Exception trying to run Sqlancer tests.")
        traceback.print_exc(file=sys.stdout)

    # Compute the final exit code.
    LOG.info("Final Status => {}".format("FAIL" if errcode else "SUCCESS"))
    sys.exit(errcode)
