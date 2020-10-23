import os
import subprocess
import shutil
import json

from microbench.google_benchmark.gbench2junit import GBenchToJUnit
from microbench.constants import LOCAL_REPO_DIR
from util.constants import LOG


class MicroBenchmarksRunner(object):
    """ A runner for microbenchmark tests. It will run the microbenchmarks
    based on a config object passed to it """

    def __init__(self, config):
        self.config = config
        self.last_build = '000'
        return

    def run_benchmarks(self, enable_perf):
        """ Return 0 if all benchmarks succeed, otherwise return the error code
            code from the last benchmark to fail
        """
        if not len(self.config.benchmarks):
            LOG.error("Invlid benchmarks were specified to execute. \
                Try not specifying a benchmark and it will execute all.")
            return 0

        ret_val = 0
        benchmark_fail_count = 0

        # iterate over all benchmarks and run them
        for benchmark_count, bench_name in enumerate(self.config.benchmarks):
            LOG.info("Running '{}' with {} threads [{}/{}]".format(bench_name,
                                                                   self.config.num_threads,
                                                                   benchmark_count,
                                                                   len(self.config.benchmarks)))
            benchmark_ret_val = self.run_single_benchmark(bench_name, enable_perf)
            if benchmark_ret_val:
                ret_val = benchmark_ret_val
                benchmark_fail_count += 1

        LOG.info("{PASSED}/{TOTAL} benchmarks passed".format(PASSED=len(self.config.benchmarks) -
                                                             benchmark_fail_count, TOTAL=len(self.config.benchmarks)))

        return ret_val

    def run_single_benchmark(self, bench_name, enable_perf):
        """ Execute a single benchmark. The results will be stored in a JSON
        file and an XML file. """
        output_file = "{}.json".format(bench_name)
        cmd = self._build_benchmark_cmd(bench_name, output_file, enable_perf)

        # Environment Variables
        os.environ["TERRIER_BENCHMARK_THREADS"] = str(self.config.num_threads)  # has to be a str
        os.environ["TERRIER_BENCHMARK_LOGFILE_PATH"] = self.config.logfile_path

        ret_val, err = self._execute_benchmark(cmd)

        if ret_val == 0:
            convert_result_xml(bench_name, output_file)
        else:
            LOG.error("Unexpected failure of {BENCHMARK} [ret_val={RET_CODE}]".format(
                BENCHMARK=bench_name, RET_CODE=ret_val))
            LOG.error(err)

        # return the process exit code
        return ret_val

    def _build_benchmark_cmd(self, bench_name, output_file, enable_perf):
        """ Given the arguments passed in this will construct the command
        necessary to execute the microbenchmark test """
        benchmark_path = os.path.join(self.config.benchmark_path, bench_name)
        cmd = "{BENCHMARK} --benchmark_min_time={MIN_TIME} " + \
              " --benchmark_format=json" + \
              " --benchmark_out={OUTPUT_FILE}"
        cmd = cmd.format(BENCHMARK=benchmark_path,
                         MIN_TIME=self.config.min_time, OUTPUT_FILE=output_file)

        if enable_perf:
            if not is_package_installed('perf'):
                raise Exception("Missing perf binary. Please install package.")
            perf_cmd = generate_perf_command(bench_name)
            cmd = "{PERF_CMD} {CMD}".format(PERF_CMD=perf_cmd, CMD=cmd)

        if not is_package_installed('numactl', '--show') and not self.config.is_local:
            raise Exception("Missing numactl binary. Please install package")
            # pass # if you are running locally but want to access Jenkins artifacts comment the above line
        numa_cmd = generate_numa_command()
        cmd = "{NUMA_CMD} {CMD}".format(NUMA_CMD=numa_cmd, CMD=cmd)
        return cmd

    def _execute_benchmark(self, cmd):
        """ Execute the microbenchmark command provided """
        LOG.debug("Executing command [num_threads={}]: {}".format(self.config.num_threads, cmd))
        try:
            output = subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
            pretty_format_json = json.dumps(json.loads(output.decode('utf8').replace("'", '"')), indent=4)
            LOG.debug("OUTPUT: {}".format(pretty_format_json))
            return 0, None
        except subprocess.CalledProcessError as err:
            return err.returncode, err
        except Exception as err:
            return 1, err

    def create_local_dirs(self):
        """ 
        This will create a directory for the build in the LOCAL_REPO_DIR. 
        Each time the microbenchmark script is run it will create another dir
        by incrementing the last dir name created. If the script is run 3 times
        the LOCAL_REPO_DIR will have directories named 001 002 003 each
        containing the json Google benchmark result file.
        """
        build_dirs = next(os.walk(LOCAL_REPO_DIR))[1]
        last_build = max(build_dirs) if build_dirs else '000'
        next_build = os.path.join(LOCAL_REPO_DIR, "{:03}".format(int(last_build) + 1))
        LOG.info("Creating new result directory in local data repository {}".format(next_build))
        os.mkdir(next_build)

        self.last_build = os.path.basename(next_build)

        for bench_name in self.config.benchmarks:
            copy_benchmark_result(bench_name, next_build)


def is_package_installed(package_name, validation_command='--version'):
    """ Check to see if package is installed """
    try:
        subprocess.check_output("{PKG} {CMD}".format(PKG=package_name, CMD=validation_command), shell=True)
        return True
    except:
        return False


def generate_perf_command(bench_name):
    """ Return the command line string to execute perf """
    perf_output_file = "{}.perf".format(bench_name)
    LOG.debug("Enabling perf data collection [output={}]".format(perf_output_file))
    return "perf record --output={}".format(perf_output_file)


def generate_numa_command():
    """ Return the command line string to execute numactl """
    # use all the cpus from the highest numbered numa node
    nodes = subprocess.check_output("numactl --hardware | grep 'available: ' | cut -d' ' -f2", shell=True)
    if not nodes:
        return ""
    highest_cpu_node = int(nodes) - 1
    if highest_cpu_node > 0:
        LOG.debug("Number of NUMA Nodes = {}".format(highest_cpu_node))
        LOG.debug("Enabling NUMA support")
        return "numactl --cpunodebind={} --preferred={}".format(highest_cpu_node, highest_cpu_node)


def convert_result_xml(bench_name, bench_output_file):
    """ convert the gbench results to xml file named after the bench__name """
    xml_output_file = "{}.xml".format(bench_name)
    GBenchToJUnit(bench_output_file).convert(xml_output_file)


def copy_benchmark_result(bench_name, build_dir):
    """ Copy the benchmark result file. This is used when running in local mode """
    result_file = "{}.json".format(bench_name)
    shutil.copy(result_file, build_dir)
    LOG.debug("Copything result file {FROM} into {TO}".format(FROM=result_file, TO=build_dir))
