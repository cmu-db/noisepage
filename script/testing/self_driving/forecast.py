"""
The forecast script generates metrics traces needed for workload forecasting.
"""

from pathlib import Path
from typing import Callable, List
from xml.etree import ElementTree

from ..oltpbench.test_case_oltp import TestCaseOLTPBench
from ..oltpbench.test_oltpbench import TestOLTPBench
from ..util.common import run_command
from ..util.constants import LOG, ErrorCode
from . import constants
import psycopg2
import time


def _config_forecast_data(xml_config_file: str, rate_pattern: List[int]) -> None:
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

        # NOTE: rate has to be before weights... This is what OLTPBench expects
        elems = [
            ("time", str(constants.DEFAULT_TPCC_TIME_SEC)),
            ("rate", str(rate)),
            ("weights", constants.DEFAULT_TPCC_WEIGHTS)
        ]

        for name, text in elems:
            elem = ElementTree.Element(name)
            elem.text = text
            work.append(elem)

        works.append(work)

    # Write back result
    xml.write(xml_config_file)


def get_config_tpcc(weights=constants.DEFAULT_TPCC_WEIGHTS,
                    terminals=constants.DEFAULT_TPCC_TERMINALS,
                    scale_factor=constants.DEFAULT_TPCC_SCALE_FACTOR,
                    query_mode=constants.DEFAULT_TPCC_QUERY_MODE) -> dict:
    """
    Get a configuration for running TPC-C.

    Parameters
    ----------
    weights : str
        The weight for the txn types in TPC-C.
        NewOrder, Payment, OrderStatus, Delivery, StockLevel.
    terminals : int
        The number of terminals to invoke in TPC-C.
    scale_factor : int
        The number of warehouses in TPC-C.
    query_mode : str
        Either "simple" or "extended"

    Returns
    -------
    config : dict
        A dictionary with all the relevant TPC-C parameters configured, usable as input to TestCaseOLTPBench.
    """
    config = {}
    config['benchmark'] = "tpcc"
    config['weights'] = weights
    config['terminals'] = terminals
    config['scale_factor'] = scale_factor
    config['query_mode'] = query_mode
    return config


def fn_pipeline_metrics_with_counters(oltpbench: TestOLTPBench, test_case: TestCaseOLTPBench) -> None:
    """Enable pipeline metrics."""
    db_server = oltpbench.db_instance
    db_server.execute("SET pipeline_metrics_enable='true'", expect_result=False, quiet=False)
    db_server.execute("SET pipeline_metrics_sample_rate={}".format(constants.DEFAULT_PIPELINE_METRICS_SAMPLE_RATE),
                      expect_result=False, quiet=False)
    db_server.execute("SET counters_enable='true'", expect_result=False, quiet=False)

def fn_query_trace_metrics(oltpbench: TestOLTPBench, test_case: TestCaseOLTPBench) -> None:
    """Enable query trace metrics."""
    db_server = oltpbench.db_instance
    db_server.execute("SET query_trace_metrics_enable='true'", expect_result=False, quiet=False)


def fn_enable_pilot(oltpbench: TestOLTPBench, test_case: TestCaseOLTPBench) -> None:
    """Enable pilot planning. Analyze must have been run!"""
    db_server = oltpbench.db_instance
    db_server.execute("SET pilot_planning=true", expect_result=False, quiet=False)


def fn_enable_pilot_and_wait_for_index(oltpbench: TestOLTPBench, test_case: TestCaseOLTPBench) -> None:
    """
    TODO(WAN): This is a hack around the buggy interaction between the Pilot and the network layer requiring
     everything to happen on the same connection. Specifically, you cannot form new psql connections while the
     pilot is running. Therefore, once you run SET pilot_planning=true, you cannot use db_server.execute()
     any more -- you need to keep the same connection around and use that instead.
    """

    db_server = oltpbench.db_instance
    conn = psycopg2.connect(port=db_server.db_port, host=db_server.db_host, user=constants.DEFAULT_DB_USER)
    conn.set_session(autocommit=True)
    index_created = False
    with conn.cursor() as cursor:
        sql = "SET pilot_planning=true"
        LOG.info(f'Executing SQL on [host={db_server.db_host},port={db_server.db_port},user={constants.DEFAULT_DB_USER}]: {sql}')
        cursor.execute(sql)
    while not index_created:
        sql = f"SELECT * from noisepage_applied_actions where action_text like 'create index automated_%'"
        LOG.info(f'Executing SQL on [host={db_server.db_host},port={db_server.db_port},user={constants.DEFAULT_DB_USER}]: {sql}')
        with conn.cursor() as cursor:
            cursor.execute(sql)
            rows = cursor.fetchall()
            if len(rows) == 0:
                sleep_s = 30
                LOG.info(f"Waiting for {sleep_s} seconds.")
                time.sleep(sleep_s)
            else:
                LOG.info(f"Found result: {rows}")
                index_created = True


def fn_test_results_file(result_file_path: str) -> bool:
    """Test if the specified results file exists (DEFAULT_PIPELINE_METRICS_FILE or DEFAULT_QUERY_TRACE_FILE."""
    if not Path(result_file_path).exists():
        LOG.error(f"Missing {result_file_path} at CWD.")
        return False
    return True


def make_fn_test_case_run_pre_test(create: bool, load: bool) -> Callable[[TestOLTPBench, TestCaseOLTPBench], None]:
    """
    Return a function that will run TestCastOLTPBench.pre_test(create=create, load=load).

    Parameters
    ----------
    create : bool
        True if OLTPBench create should be run.
    load : bool
        True if OLTPBench load should be run.

    Returns
    -------
    A function as described above.
    """

    def fn_pretest(oltpbench: TestOLTPBench, test_case: TestCaseOLTPBench) -> None:
        test_case.run_pre_test(create, load)

    return fn_pretest


def make_fn_remove_results_file(result_file_path: str) -> Callable[[TestOLTPBench, TestCaseOLTPBench], None]:
    """
    Return a function that will remove the specified result file.

    Parameters
    ----------
    result_file_path : str
        The file that should be removed.

    Returns
    -------
    A function as described above.
    """

    def fn_remove(oltpbench: TestOLTPBench, test_case: TestCaseOLTPBench) -> None:
        Path(result_file_path).unlink(missing_ok=True)

    return fn_remove


def make_fn_config_forecast_data_tpcc(tpcc_rates: List[int], pattern_iter: int) -> \
        Callable[[TestOLTPBench, TestCaseOLTPBench], None]:
    """
    Return a function that will configure the forecast data for TPC-C.

    Parameters
    ----------
    tpcc_rates : List[int]
        The arrival rates for each phase in a pattern.
    pattern_iter : int
        The number of patterns.

    Returns
    -------
    A function as described above.
    """

    def fn_config(oltpbench: TestOLTPBench, test_case: TestCaseOLTPBench) -> None:
        rates = tpcc_rates * pattern_iter
        _config_forecast_data(test_case.xml_config, rates)

    return fn_config


def make_fn_wait_until_exists_index_like(idx_name: str) -> Callable[[TestOLTPBench, TestCaseOLTPBench], None]:
    """Wait until the index with the specified name exists. SQL injection blablabla..."""

    def fn_index(oltpbench: TestOLTPBench, test_case: TestCaseOLTPBench) -> None:
        dbms = oltpbench.db_instance
        index_found = False
        LOG.info(f"Waiting until the following index exists: {idx_name}")
        while not index_found:
            result = dbms.execute(f"SELECT relname FROM pg_class WHERE relname LIKE '{idx_name}'", quiet=False,
                                  expect_result=True)
            if len(result) == 0:
                sleep_s = 30
                LOG.info(f"Waiting for {sleep_s} seconds.")
                time.sleep(sleep_s)
            else:
                LOG.info(f"Found indexes: {result}")
                index_found = True

    return fn_index


def fn_tpcc_analyze(oltpbench: TestOLTPBench, test_case: TestCaseOLTPBench) -> None:
    """Run ANALYZE on all the TPC-C tables."""
    db_server = oltpbench.db_instance
    db_server.execute("analyze order_line", expect_result=False, quiet=False)
    db_server.execute("analyze new_order", expect_result=False, quiet=False)
    db_server.execute("analyze stock", expect_result=False, quiet=False)
    db_server.execute("analyze oorder", expect_result=False, quiet=False)
    db_server.execute("analyze history", expect_result=False, quiet=False)
    db_server.execute("analyze customer", expect_result=False, quiet=False)
    db_server.execute("analyze district", expect_result=False, quiet=False)
    db_server.execute("analyze item", expect_result=False, quiet=False)
    db_server.execute("analyze warehouse", expect_result=False, quiet=False)


def fn_tpcc_drop_indexes_secondary(oltpbench: TestOLTPBench, test_case: TestCaseOLTPBench) -> None:
    """Drop the secondary indexes for TPCC."""
    # Keep this in sync with OLTPBench.
    server = oltpbench.db_instance
    server.execute('DROP INDEX IDX_CUSTOMER_NAME', expect_result=False, quiet=False)
    server.execute('DROP INDEX IDX_ORDER', expect_result=False, quiet=False)


def fn_final_true(ignored: str) -> bool:
    return True


def run_oltpbench(server_args: dict,
                  benchmark_config: dict,
                  result_file_path: str,
                  fns_pre_run: List[Callable[[TestOLTPBench, TestCaseOLTPBench], None]],
                  should_execute: bool,
                  fns_post_run: List[Callable[[TestOLTPBench, TestCaseOLTPBench], None]],
                  fn_final: Callable[[str], bool]):
    # Download and build OLTPBench.
    oltpbench = TestOLTPBench({'server_args': server_args})
    oltpbench.run_pre_suite()
    # Start the DBMS.
    db_server = oltpbench.db_instance
    db_server.run_db()

    try:
        # Instantiate the TestCaseOLTPBench object, but do not actually do anything with it.
        test_case = TestCaseOLTPBench(benchmark_config)

        for fn in fns_pre_run:
            fn(oltpbench, test_case)

        if should_execute:
            # Run the actual test.
            ret_val, _, stderr = run_command(test_case.test_command,
                                             cwd=test_case.test_command_cwd)
            if ret_val != ErrorCode.SUCCESS:
                LOG.error(stderr)
                return False

        for fn in fns_post_run:
            fn(oltpbench, test_case)
    finally:
        # Stop the DBMS and delete the WAL.
        exit_code = db_server.stop_db()
        db_server.delete_wal()

    return exit_code, fn_final(result_file_path)
