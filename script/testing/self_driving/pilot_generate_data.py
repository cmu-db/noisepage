import sys

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


if __name__ == "__main__":
    exit_code, result_ok = _generate_data()
    sys.exit(exit_code)
