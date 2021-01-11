from prettytable import PrettyTable

from ..reporting.report_result import report_microbenchmark_result
from ..util.constants import LOG
from .google_benchmark.gbench_run_result import GBenchRunResult

FIELD_NAMES = ['status', 'iterations', 'throughput', 'ref_throughput', 'tolerance',
               'change', 'coef_var', 'reference_type', 'num_results', 'suite', 'test']
LARGE_FLOAT_FIELD_NAMES = ['throughput', 'ref_throughput']
PERCENT_FLOAT_FIELD_NAMES = ['coef_var', 'change']


def send_results(config, artifact_processor):
    """ Iterate over the microbenchmark results to generate a comparison
    against the histroical artifacts and send that comparrison to the 
    performance storage service """
    ret_code = 0
    for bench_name in sorted(config.benchmarks):
        filename = "{}.json".format(bench_name)
        gbench_run_results = GBenchRunResult.from_benchmark_file(filename)

        for key in sorted(gbench_run_results.benchmarks.keys()):
            result = gbench_run_results.benchmarks.get(key)
            LOG.debug("%s Result:\n%s", bench_name, result)

            comparison = artifact_processor.get_comparison_for_publish_result(bench_name, result, config.lax_tolerance)
            try:
                report_microbenchmark_result(
                    config.publish_results_env, result.timestamp, config, comparison)
            except Exception as err:
                LOG.error("Error reporting results to performance storage service")
                LOG.error(err)
                ret_code = 1

    return ret_code


def table_dump(config, artifact_processor):
    """ Create a human readable table for the output comparison between the 
    current test results and the historical results (if there are any)
    """
    text_table = PrettyTable()
    text_table.field_names = FIELD_NAMES
    text_table.align['suite'] = 'l'
    text_table.align['test'] = 'l'
    for bench_name in sorted(config.benchmarks):
        filename = "{}.json".format(bench_name)
        gbench_run_results = GBenchRunResult.from_benchmark_file(filename)

        for key in sorted(gbench_run_results.benchmarks.keys()):
            result = gbench_run_results.benchmarks.get(key)
            LOG.debug("%s Result:\n%s", bench_name, result)

            comparison = artifact_processor.get_comparison(
                bench_name, result, config.lax_tolerance)
            if comparison.get('pass') == 'FAIL':
                ret = 1

            row_values = list(map(lambda field_name: formatFields(
                field_name, comparison.get(field_name, 0)), text_table.field_names))
            text_table.add_row(row_values)
    print("")
    print(text_table)
    print("")


def formatFields(field_name, field_value):
    if field_name in LARGE_FLOAT_FIELD_NAMES:
        return "{:1.4e}".format(field_value)
    if field_name in PERCENT_FLOAT_FIELD_NAMES:
        return "{:.0f}".format(field_value)
    return field_value
