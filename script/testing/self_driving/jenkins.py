import sys

from ..util.common import LOG
from . import constants, forecast


def _generate_data():
    # 1. Create and load TPC-C via OLTPBench.
    # 2. Drop secondary TPC-C indexes.
    # 3. Execute OLTPBench.
    exit_code, result_ok = forecast.run_oltpbench(
        server_args={},
        benchmark_config=forecast.get_config_tpcc(terminals=1),
        result_file_path=constants.DEFAULT_QUERY_TRACE_FILE,
        fns_pre_run=[forecast.make_fn_test_case_run_pre_test(create=True, load=True),
                     forecast.make_fn_config_forecast_data_tpcc(constants.DEFAULT_WORKLOAD_PATTERN,
                                                                constants.DEFAULT_ITER_NUM),
                     forecast.fn_query_trace_metrics,
                     forecast.make_fn_remove_results_file(constants.DEFAULT_QUERY_TRACE_FILE),
                     forecast.fn_tpcc_drop_indexes_secondary],
        should_execute=True,
        fns_post_run=[],
        fn_final=forecast.fn_test_results_file
    )
    return exit_code, result_ok


def _pilot_planning():
    # Start NoisePage with Pilot and ModelServer. Create and load OLTPBench, skip the actual execute phase, enable pilot planning.
    exit_code, result_ok = forecast.run_oltpbench(
        server_args={
            'use_pilot_thread': True,
            'model_server_enable': True,
            'messenger_enable': True,
            'model_save_path': './ou_runner_trained_model/ou_model_map.pickle',
        },
        benchmark_config=forecast.get_config_tpcc(terminals=1),
        result_file_path=constants.DEFAULT_QUERY_TRACE_FILE,
        fns_pre_run=[forecast.make_fn_test_case_run_pre_test(create=True, load=True),
                     forecast.fn_tpcc_drop_indexes_secondary],
        should_execute=False,
        fns_post_run=[forecast.fn_enable_pilot,
                      forecast.make_fn_wait_until_exists_index_like('automated_index_%')],
        fn_final=forecast.fn_final_true
    )
    return exit_code, result_ok


if __name__ == "__main__":
    exit_code, result_ok = _generate_data()
    if exit_code != 0:
        sys.exit(exit_code)

    # TODO(WAN):    Unfortunately, the ModelServer and Pilot currently do not cleanly shutdown because of
    #               the future.Wait(). Therefore we ignore the exit codes. If the index failed to be created,
    #               it would still timeout in Jenkins waiting for automated_index_foo to be created.
    _, _ = _pilot_planning()
    LOG.warn("We are ignoring the DBMS exit code!")
    sys.exit(0)
