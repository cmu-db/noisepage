import sys

from . import constants, forecast


def _pilot_planning():
    # Start NoisePage with Pilot and ModelServer. Create and load OLTPBench, skip the actual execute phase, enable pilot planning.
    exit_code, result_ok = forecast.run_oltpbench(
        server_args={
            'use_pilot_thread': True,
            'model_server_enable': True,
            'messenger_enable': True,
            'startup_ddl_path': 'startup.sql',
            'ou_model_save_path': './trained_model_ou/model_ou.pickle',
            'interference_model_save_path': './trained_model_interference/model_interference.pickle',
        },
        benchmark_config=forecast.get_config_tpcc(terminals=1),
        result_file_path=constants.DEFAULT_QUERY_TRACE_FILE,
        fns_pre_run=[forecast.make_fn_test_case_run_pre_test(create=True, load=True),
                     forecast.fn_tpcc_drop_indexes_secondary],
        should_execute=False,
        fns_post_run=[forecast.fn_tpcc_analyze,
                      forecast.fn_enable_pilot_and_wait_for_index],
        fn_final=forecast.fn_final_true
    )
    return exit_code, result_ok


if __name__ == "__main__":
    exit_code, _ = _pilot_planning()
    sys.exit(exit_code)
