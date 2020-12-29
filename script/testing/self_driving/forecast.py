#!/usr/bin/env python3
"""
The forecast scripts generates metrics traces needed for workload forecasting.
"""

from oltpbench.test_case_oltp import TestCaseOLTPBench
from oltpbench.run_oltpbench import TestOLTPBench
from oltpbench.constants import OLTPBENCH_GIT_LOCAL_PATH
from util.constants import (LOG, ErrorCode, DEFAULT_DB_HOST, DEFAULT_DB_PORT)
from util.common import run_command
from util.db_server import NoisePageServer
from self_driving.constants import (
    DEFAULT_TPCC_TIME_SEC,
    DEFAULT_TPCC_WEIGHTS,
    DEFAULT_QUERY_TRACE_FILE,
    DEFAULT_OLTP_SERVER_ARGS,
    DEFAULT_OLTP_TEST_CASE,
    DEFAULT_WORKLOAD_PATTERN)

from pathlib import Path
from xml.etree import ElementTree
from typing import Dict, List, Optional, Tuple


def config_forecast_data(xml_config_file: str, rate_pattern: List[int]) -> None:
    """
     Modify a OLTP config file to follow a certain pattern in its duration.

    :param xml_config_file:
    :param rate_pattern:
    :return:
    """
    xml = ElementTree.parse(xml_config_file)
    root = xml.getroot()
    works = root.find("works")
    works.clear()

    # Set the work pattern
    for rate in rate_pattern:
        work = ElementTree.Element("work")

        # NOTE: rate has to be before weights... This is what the OLTP expects
        elems = [
            ("time", str(DEFAULT_TPCC_TIME_SEC)),
            ("rate", str(rate)),
            ("weights", DEFAULT_TPCC_WEIGHTS)
        ]

        for name, text in elems:
            elem = ElementTree.Element(name)
            elem.text = text
            work.append(elem)

        works.append(work)

    # Write back result
    xml.write(xml_config_file)


def gen_oltp_trace(
        tpcc_weight: str, tpcc_rates: List[int], pattern_iter: int) -> bool:
    """
    Generates the trace by running OLTP TPCC benchmark on the built database
    :param tpcc_weight:  Weight for the TPCC workload
    :param tpcc_rates: Arrival rates for each phase in a pattern
    :param pattern_iter:  Number of patterns
    :return: True when data generation succeeds
    """
    # Remove the old query_trace/query_text.csv
    Path(DEFAULT_QUERY_TRACE_FILE).unlink(missing_ok=True)

    # Server is running when this returns
    oltp_server = TestOLTPBench(DEFAULT_OLTP_SERVER_ARGS)
    db_server = oltp_server.db_instance
    db_server.run_db()

    # Download the OLTP repo and build it
    oltp_server.run_pre_suite()

    # Load the workload pattern - based on the tpcc.json in
    # testing/oltpbench/config
    test_case_config = DEFAULT_OLTP_TEST_CASE
    test_case_config["weights"] = tpcc_weight
    test_case = TestCaseOLTPBench(test_case_config)

    # Prep the test case build the result dir
    test_case.run_pre_test()

    rates = tpcc_rates * pattern_iter
    config_forecast_data(test_case.xml_config, rates)

    # Turn on query trace metrics tracing
    db_server.execute("SET query_trace_metrics_enable='true'", expect_result=False)

    # Run the actual test
    ret_val, _, stderr = run_command(test_case.test_command,
                                     test_case.test_error_msg,
                                     cwd=test_case.test_command_cwd)
    if ret_val != ErrorCode.SUCCESS:
        LOG.error(stderr)
        return False

    # Clean up, disconnect the DB
    db_server.stop_db()
    db_server.delete_wal()

    if not Path(DEFAULT_QUERY_TRACE_FILE).exists():
        LOG.error(
            f"Missing {DEFAULT_QUERY_TRACE_FILE} at CWD after running OLTP TPCC")
        return False

    return True
