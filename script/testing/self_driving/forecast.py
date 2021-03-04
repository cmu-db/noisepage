"""
The forecast script generates metrics traces needed for workload forecasting.
"""

from pathlib import Path
from typing import List
from xml.etree import ElementTree

from ..oltpbench.test_case_oltp import TestCaseOLTPBench
from ..oltpbench.test_oltpbench import TestOLTPBench
from ..util.common import run_command
from ..util.constants import LOG, ErrorCode
from .constants import (DEFAULT_OLTP_SERVER_ARGS, DEFAULT_OLTP_TEST_CASE,
                        DEFAULT_QUERY_TRACE_FILE, DEFAULT_TPCC_TIME_SEC,
                        DEFAULT_TPCC_WEIGHTS, DEFAULT_PIPELINE_METRICS_FILE,
                        DEFAULT_PIPELINE_METRICS_SAMPLE_RATE)


def config_forecast_data(xml_config_file: str, rate_pattern: List[int]) -> None:
    """
    Modify an OLTPBench config file to follow a certain pattern in its duration.

    Parameters
    ----------
    xml_config_file : str
        The file to be modified.
    rate_pattern : List[int]
        The pattern to be used.
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
        tpcc_weight: str, tpcc_rates: List[int], pattern_iter: int, record_pipeline_metrics: bool) -> bool:
    """
    Generate the trace by running OLTPBench's TPCC benchmark on the built DBMS.

    Parameters
    ----------
    tpcc_weight : str
        The weight for the TPCC workload.
    tpcc_rates : List[int]
        The arrival rates for each phase in a pattern.
    pattern_iter : int
        The number of patterns.
    record_pipeline_metrics : bool
        Record the pipeline metrics instead of query traces

    Returns
    -------
    True on success.
    """
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

    if record_pipeline_metrics:
        # Turn on pipeline metrics recording
        db_server.execute("SET pipeline_metrics_enable='true'", expect_result=False)
        db_server.execute("SET pipeline_metrics_sample_rate={}".format(DEFAULT_PIPELINE_METRICS_SAMPLE_RATE),
                          expect_result=False)
        result_file = DEFAULT_PIPELINE_METRICS_FILE
    else:
        # Turn on query trace metrics tracing
        db_server.execute("SET query_trace_metrics_enable='true'", expect_result=False)
        result_file = DEFAULT_QUERY_TRACE_FILE
    # Remove the old result file
    Path(result_file).unlink(missing_ok=True)

    # Run the actual test
    ret_val, _, stderr = run_command(test_case.test_command,
                                     cwd=test_case.test_command_cwd)
    if ret_val != ErrorCode.SUCCESS:
        LOG.error(stderr)
        return False

    # Clean up, disconnect the DB
    db_server.stop_db()
    db_server.delete_wal()

    if not Path(result_file).exists():
        LOG.error(
            f"Missing {result_file} at CWD after running OLTP TPCC")
        return False

    return True
