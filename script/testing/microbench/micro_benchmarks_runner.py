import json
import os
import shutil
import subprocess

from ..util.constants import LOG
from .constants import LOCAL_REPO_DIR
from .google_benchmark.gbench2junit import GBenchToJUnit


class MicroBenchmarksRunner(object):
    """ A runner for microbenchmark tests. It will run the microbenchmarks
    based on a config object passed to it """

    def __init__(self, config):
        self.config = config
        self.last_build = '000'
        return

    def run_benchmarks(self, enable_perf):
        """ Runs all the microbenchmarks.

        Parameters
        ----------
        enable_perf : bool
            Whether perf should be enabled for all the benchmarks.

        Returns
        -------
        ret_val : int
            the return value for the last failed benchmark. If no benchmarks
            fail then it will return 0.
        """
        if not len(self.config.benchmarks):
            LOG.error('Invlid benchmarks were specified to execute. \
                Try not specifying a benchmark and it will execute all.')
            return 0

        ret_val = 0
        benchmark_fail_count = 0

        # iterate over all benchmarks and run them
        for benchmark_count, bench_name in enumerate(self.config.benchmarks):
            LOG.info(f"Running '{bench_name}' with {self.config.num_threads} threads [{benchmark_count}/{len(self.config.benchmarks)}]")
            benchmark_ret_val = self.run_single_benchmark(bench_name, enable_perf)
            if benchmark_ret_val:
                ret_val = benchmark_ret_val
                benchmark_fail_count += 1

        LOG.info("{PASSED}/{TOTAL} benchmarks passed".format(PASSED=len(self.config.benchmarks) -
                                                             benchmark_fail_count, TOTAL=len(self.config.benchmarks)))

        return ret_val

    def run_single_benchmark(self, bench_name, enable_perf):
        """ Execute a single benchmark. The results will be stored in a JSON
        file and an XML file.

        Parameters
        ----------
        bench_name : str
            The name of the benchmark to run.
        enable_perf : bool
            Whether perf should be enabled for all the benchmarks.

        Returns
        -------
        ret_val : int
            The return value from the benchmark process. 0 if successful.
        """
        output_file = "{}.json".format(bench_name)
        cmd = self._build_benchmark_cmd(bench_name, output_file, enable_perf)

        # Environment Variables
        os.environ["TERRIER_BENCHMARK_THREADS"] = str(self.config.num_threads)  # has to be a str
        os.environ["TERRIER_BENCHMARK_LOGFILE_PATH"] = self.config.logfile_path

        ret_val, err = self._execute_benchmark(cmd)

        if ret_val == 0:
            convert_result_xml(bench_name, output_file)
        else:
            LOG.error(f'Unexpected failure of {bench_name} [ret_val={ret_val}]')
            LOG.error(err)

        # return the process exit code
        return ret_val

    def _build_benchmark_cmd(self, bench_name, output_file, enable_perf):
        """ Construct the command necessary to execute the microbenchmark test.

        Parameters
        ----------
        bench_name : str
            The name of the benchmark to run.
        output_file : str
            The path of the file where the benchmark result should be stored.
        enable_perf : bool
            Whether perf should be enabled for all the benchmarks.

        Returns
        -------
        cmd : str
            The command to be executed in order to run the microbenchmark.
        """
        benchmark_path = os.path.join(self.config.benchmark_path, bench_name)
        cmd = f'{benchmark_path} ' + \
              f' --benchmark_min_time={self.config.min_time} ' + \
              f' --benchmark_format=json' + \
              f' --benchmark_out={output_file}'

        if enable_perf:
            if not is_package_installed('perf'):
                raise Exception('Missing perf binary. Please install package.')
            perf_cmd = generate_perf_command(bench_name)
            cmd = f'{perf_cmd} {cmd}'

        if self.config.is_local:
            pass
        elif not is_package_installed('numactl', '--show'):
            raise Exception('Missing numactl binary. Please install package')
        else:
            numa_cmd = generate_numa_command()
            cmd = f'{numa_cmd} {cmd}'
        return cmd

    def _execute_benchmark(self, cmd):
        """ Execute the microbenchmark command provided.

        Parameters
        ----------
        cmd : str
            The command to be executed in order to run the microbenchmark.

        Returns
        -------
        ret_code : int
            The return value from the benchmark process. 0 if successful.
        err : Error
            The error that occured. None if successful.
        """
        LOG.debug(f'Executing command [num_threads={self.config.num_threads}]: {cmd}')
        try:
            output = subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
            pretty_format_json = json.dumps(json.loads(output.decode('utf8').replace("'", '"')), indent=4)
            LOG.debug(f'OUTPUT: {pretty_format_json}')
            return 0, None
        except subprocess.CalledProcessError as err:
            print(err)
            return err.returncode, err
        except Exception as err:
            return 1, err

    def create_local_dirs(self):
        """ Create directories to be used as historical results for future
        local runs.

        This will create a directory for the build in the LOCAL_REPO_DIR. 
        Each time the microbenchmark script is run it will create another dir
        by incrementing the last dir name created. If the script is run 3 times
        the LOCAL_REPO_DIR will have directories named 001 002 003 each
        containing the json Google benchmark result file.
        """
        build_dirs = next(os.walk(LOCAL_REPO_DIR))[1]
        last_build = max(build_dirs) if build_dirs else '000'
        next_build = os.path.join(LOCAL_REPO_DIR, f'{(int(last_build) + 1):03}')
        LOG.info(f'Creating new result directory in local data repository {next_build}')
        os.mkdir(next_build)

        self.last_build = os.path.basename(next_build)

        for bench_name in self.config.benchmarks:
            copy_benchmark_result(bench_name, next_build)


def is_package_installed(package_name, validation_command='--version'):
    """ Check to see if package is installed.

    Parameters
    ----------
    package_name : str
        The name of the executable to check.
    validation_command : str, optional
        The command to execute to check if the package has been installed.
        (The default is '--version')

    Returns
    is_installed : bool
        Whether the package is installed.
    """
    try:
        subprocess.check_output(f'{package_name} {validation_command}', shell=True)
        return True
    except:
        return False


def generate_perf_command(bench_name):
    """ Create the command line string to execute perf.

    Parameters
    ----------
    bench_name : str
        The name of the benchmark.

    Returns
    -------
    perf_cmd : str
        The command to execute pref data collection.
    """
    perf_output_file = f'{bench_name}.perf'
    LOG.debug(f'Enabling perf data collection [output={perf_output_file}]')
    return f'perf record --output={perf_output_file}'


def generate_numa_command():
    """ Create the command line string to execute numactl.

    Returns
    -------
    numa_cmd : str
        The command to execute using NUMA.
    """
    # use all the cpus from the highest numbered numa node
    nodes = subprocess.check_output("numactl --hardware | grep 'available: ' | cut -d' ' -f2", shell=True)
    if not nodes or int(nodes) == 1:
        return ''
    highest_cpu_node = int(nodes) - 1
    if highest_cpu_node > 0:
        LOG.debug(f'Number of NUMA Nodes = {highest_cpu_node}')
        LOG.debug('Enabling NUMA support')
        return f'numactl --cpunodebind={highest_cpu_node} --preferred={highest_cpu_node}'


def convert_result_xml(bench_name, bench_output_file):
    """ Convert the gbench results to xml file named after the bench_name.

    Parameters
    ----------
    bench_name : str
        The name of the microbenchmark.
    bench_output_file : str
        The path to the benchmark results file.
    """
    xml_output_file = f'{bench_name}.xml'
    GBenchToJUnit(bench_output_file).convert(xml_output_file)


def copy_benchmark_result(bench_name, build_dir):
    """ Copy the benchmark result file.

    This is used when running in local mode.

    Parameters
    ----------
    bench_name : str
        The name of the microbenchmark.
    build_dir : str
        The path to the build directory.
    """
    result_file = f'{bench_name}.json'
    shutil.copy(result_file, build_dir)
    LOG.debug(f'Copything result file {result_file} into {build_dir}')
