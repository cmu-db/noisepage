#!/usr/bin/env python

"""
Run micro-benchmarks on a PR, for the purpose of comparing performance with
the master branch.

Usage:
From the directory in which this script resides
./run_micro_bench.py
"""

import os
import sys
import argparse
import datetime
import json
import pprint
import subprocess
import urllib
import logging
import shutil
import glob
import csv
from pprint import pprint

import xml.etree.ElementTree as ElementTree

from types import (ListType, StringType)

## =========================================================
## LOGGING
## =========================================================

LOG = logging.getLogger(__name__)
LOG_handler = logging.StreamHandler()
LOG_formatter = logging.Formatter(fmt='%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s',
                                  datefmt='%m-%d-%Y %H:%M:%S')
LOG_handler.setFormatter(LOG_formatter)
LOG.addHandler(LOG_handler)
LOG.setLevel(logging.INFO)


## =========================================================

# Jenkins URL
JENKINS_URL = "http://jenkins.db.cs.cmu.edu:8080"

# Local Data Directory
# Instead of checking with Jenkins, you can build a local repository of results
LOCAL_REPO_DIR = os.path.realpath("local")

# How many historical values are "required" before enforcing the threshold check
MIN_REF_VALUES = 20

# Default failure threshold
# The regression threshold determines how much the benchmark is allowed to get
# slower from the previous runs before it counts as a failure if we are 
# using historical data (i.e., if min_ref_values are available).
# You really should not be messing with this value without asking somebody else first.
DEFAULT_FAILURE_THRESHOLD = 10

# LIST OF BENCHMARKS
# Add the name of your benchmark in the list below and it will automatically 
# get executed when this script runs. Some benchmarks are more sensitive / non-deterministic 
# and we get a lot of spurious failures due to funkiness on the jenkins cluster.
# So we will use a higher threshold for now. 
#
# Format:
#   benchmark_file => RegressionThreshold
#
BENCHMARKS_TO_RUN = {
    "catalog_benchmark":                    20,
    "data_table_benchmark":                 75,
    "garbage_collector_benchmark":          DEFAULT_FAILURE_THRESHOLD,
    "large_transaction_benchmark":          DEFAULT_FAILURE_THRESHOLD,
    "index_wrapper_benchmark":              DEFAULT_FAILURE_THRESHOLD,
    "logging_benchmark":                    DEFAULT_FAILURE_THRESHOLD,
    "recovery_benchmark":                   DEFAULT_FAILURE_THRESHOLD,
    "large_transaction_metrics_benchmark":  DEFAULT_FAILURE_THRESHOLD,
    "logging_metrics_benchmark":            DEFAULT_FAILURE_THRESHOLD,
    "tuple_access_strategy_benchmark":      15,
    "tpcc_benchmark":                       DEFAULT_FAILURE_THRESHOLD,
    "bwtree_benchmark":                     DEFAULT_FAILURE_THRESHOLD,
    "cuckoomap_benchmark":                  DEFAULT_FAILURE_THRESHOLD,
    "parser_benchmark":                     20,
    "slot_iterator_benchmark":              DEFAULT_FAILURE_THRESHOLD,
    "varlen_entry_benchmark":               30,
}

# The number of threads to use for multi-threaded benchmarks.
# This parameter will be passed in as an environment variable to each benchmark.
BENCHMARK_THREADS = 4

# The path to the logfile for the benchmarks.
BENCHMARK_LOGFILE_PATH = "/tmp/benchmark.log"

# Where to find the benchmarks to execute
BENCHMARK_PATH = os.path.realpath("../../build/release/")

## =========================================================

class Config(object):
    """ Configuration for run_micro_bench.
        All information is read-only.
    """
    def __init__(self):
        # benchmark executables to run
        self.benchmarks = BENCHMARKS_TO_RUN

        # if fewer than min_ref_values are available
        self.lax_tolerance = 50

        # minimum run time for the benchmark, seconds
        self.min_time = 10

        # Pull reference benchmark runs from this ordered list
        # of sources. Stop if the history requirements are met.
        self.ref_data_sources = [
            {"project" : "terrier-nightly",
             "min_build" : None, # 363,
            },
        ]
        return
## CLASS

## =========================================================


class TextTable(object):
    """ Print out data as text, in a formatted table """
    def __init__(self):
        """ Initialization """
        self.rows = []
        # Columns to print, each item is a dictinary
        self.columns = []
        return

    def add_row(self, item):
        """ item - dictionary or object with attributes
        """
        self.rows.append(item)
        return

    def add_column(self, column, heading=None, col_format=None,
                   right_justify=False):
        """ Add single column (by name), to be printed
            column : dictionary key of column
            heading: heading to print for column. If not specified,
                     uses the column key
            format: optional format for column
            right_justify: overrides default format justification
        """
        col_dict = {}
        col_dict['name'] = column
        if not col_format is None:
            col_dict['format'] = " " + col_format + " "
        if not heading is None:
            col_dict['heading'] = heading
        if right_justify:
            col_dict['right_justify'] = True
        self.columns.append(col_dict)
        return

    def sort(self, sort_spec):
        """Sort, single field or list of fields"""
        # remember the field name, and sort prior to output
        self.sort_key = sort_spec
        return

    def _width(self, row, col):
        return self._width_dict(row, col)

    def _col_str(self, row, col):
        return self._col_str_dict(row, col)

    def _col_str_dict(self, row, col):
        """ Return printable field (dictionary) """
        field = col['name']
        if not row.has_key(field):
            return u""
        if col.has_key('format'):
            return col['format'] % row[field]
        return u" {} ".format(row[field])

    def _width_dict(self, *width_args):
        """ Return width of field (dictionary) """
        return len(self._col_str_dict(*width_args))

    def _col_widths(self):
        """ Compute column widths"""
        # set initial col. widths
        for col in self.columns:
            max_width = 0
            hkey = 'heading'
            if col.has_key(hkey):
                # use the heading
                max_width = len(col[hkey])
            else:
                # use the field name (or attribute)

                max_width = len(col['name'])
            col['max_width'] = max_width

        # now set max column widths
        for col in self.columns:
            for row in self.rows:
                width = self._width(row, col)
                # print "width of %s is %d" % (col['name'], width)
                if width > col['max_width']:
                    col['max_width'] = width

    def _sort_key_list(self):
        """ produce a list for the sort key """
        key_list = []
        if isinstance(self.sort_key, StringType):
            key_list.append(self.sort_key)
        elif isinstance(self.sort_key, ListType):
            key_list = self.sort_key
        return key_list

    def _decorated_row(self, row):
        key_list = self._sort_key_list()

        ret_val = []
        for key in key_list:
            ret_val.append(row[key])
        ret_val.append(row)
        return ret_val

    def _undecorated_row(self, row):
        return row[-1]

    def __str__(self):
        """ printable table """
        if hasattr(self, 'sort_key'):
            # decorate
            temp_rows = []
            for row in self.rows:
                temp_rows.append(self._decorated_row(row))
            temp_rows.sort()

            self.rows = []
            for row in temp_rows:
                self.rows.append(self._undecorated_row(row))

        self._col_widths()

        # headings
        ret_str = u""
        for col in self.columns:
            hkey = 'heading'
            if col.has_key(hkey):
                col_heading = col[hkey]
            else:
                col_heading = col['name']
            ret_str = ret_str +  u"%-*s " % (col['max_width'],
                                             col_heading)
        ret_str = ret_str + u"\n"

        for col in self.columns:
            for i in range(col['max_width']):
                ret_str = ret_str + "-"
            ret_str = ret_str + "|"
        ret_str = ret_str + u"\n"

        for row in self.rows:
            for col in self.columns:
                rjkey = 'right_justify'
                if col.has_key(rjkey) and col[rjkey]:
                    format_st = u"%*s "
                else:
                    format_st = u"%-*s "
                ret_str = ret_str +  format_st % (
                    col['max_width'], self._col_str(row, col))

            # Remove any excess padding for the last column
            ret_str = ret_str.rstrip()
            ret_str = ret_str + u"\n"
        return ret_str

class Artifact(object):
    """ A Jenkins build artifact, as visible from the web api """
    def __init__(self, build_url, artifact_dict):
        self.build_url = build_url
        self.artifact_dict = artifact_dict
        return

    def get_filename(self):
        """ Return artifact file name """
        return self.artifact_dict['fileName']

    def get_data(self):
        """ Return the contents of the artifact  """
        url = "{}artifact/{}".format(self.build_url,
                                     self.artifact_dict['relativePath'])
        url_data = urllib.urlopen(url).read()
        return url_data

class Build(object):
    """ A Jenkins build, as visible from the web api """
    def __init__(self, build_dict):
        """ build_dict : dict as returned by the web api """
        self.build_dict = build_dict
        self.artifact_list = None
        return

    def get_artifact_by_filename(self, filename):
        """ Return Artifact object with specified fileName """
        for artifact_obj in self.artifact_list:
            if artifact_obj.get_filename() == filename:
                # return Artifact object
                return artifact_obj
        return None

    def get_artifacts(self):
        """ Return a list of Artifact (objects) """
        build_url = self.get_build_url()

        # get the list of artifacts
        python_url = "{}/api/json".format(build_url)
        data = json.loads(urllib.urlopen(python_url).read())
        artifacts_lod = data['artifacts']
        # returns a list of artifact dictionaries. These look like:
        #
        #    {'displayPath': 'data_table_benchmark.json',
        #     'fileName': 'data_table_benchmark.json',
        #     'relativePath': 'script/micro_bench/data_table_benchmark.json'
        #    }

        # turn them into Artifact objects
        self.artifact_list = []
        for item in artifacts_lod:
            # Make sure the filename ends with "_benchmark.json"
            if not item["fileName"].endswith("_benchmark.json"): continue
            self.artifact_list.append(Artifact(build_url, item))
        return self.artifact_list

    def get_build_url(self):
        """ return: the url for this build """
        return self.build_dict['url']

    def get_number(self):
        """ return the build number """
        return self.build_dict['number']

    def get_result(self):
        """ Return build result, SUCCESS, FAILURE or ABORTED"""
        return self.build_dict['result']

    def has_artifact_fileName(self, fileName):
        """ Does an artifact with the specified fileName, exist in
            this build
        """
        for artifact in self.artifact_list:
            if artifact.get_filename() == fileName:
                return True
        return False

class ArtifactProcessor(object):
    """ Compute summary stats from Google Benchmark results.
        Provide access by (suite_name, test_name)
    """
    def __init__(self, required_num_items=None):
        # key = (suite_name, test_name)
        self.results = {}
        self.required_num_items = required_num_items
        
        # Benchmark Name -> Suite Name
        self.name_suite_xref = { }
        self.suite_name_xref = { }
        return

    def get_required_num_items(self):
        return self.required_num_items

    def add_artifact_file(self, data):
        """
        Add an artifact file to the list of files to be used
        for computing summary statistics

        data : raw json data from the artifact file
        """

        # create a GBFileResult
        gbr = GBFileResult(json.loads(data))
        bench_name = gbr.get_benchmark_name()
        
        # iterate over the GBBenchResult objects
        for bench_result in gbr.benchmarks:
            suite_name = bench_result.get_suite_name()
            
            if not bench_name in self.name_suite_xref:
                self.name_suite_xref[bench_name] = suite_name
            if not suite_name in self.suite_name_xref:
                self.suite_name_xref[suite_name] = bench_name
            
            key = (suite_name, bench_result.get_test_name())

            # add to a GBBenchResultProcessor
            gbr_p = self.results.get(key)
            if gbr_p is None:
                gbr_p = GBBenchResultProcessor()
                self.results[key] = gbr_p

            if self.required_num_items:
                if gbr_p.get_num_items() < self.required_num_items:
                    gbr_p.add_gbresult(bench_result, gbr.get_datetime())
            else:
                gbr_p.add_gbresult(bench_result, gbr.get_datetime())
        return

    def have_min_history(self):
        """ Check if we have accumulated enough results
            required_num_items : minimum number of results required
            return:
                True: have them
                False: need more
        """
        if not self.required_num_items:
            LOG.debug("No required num of results is set???")
            return False
        
        # If there are no results at all, then presumably we don't have enough??
        if len(self.results) == 0:
            LOG.debug("No results are available")
            return False
        
        for suite_name, test_name in self.results.keys():
            result = self.get_result(suite_name, test_name)
            LOG.debug("# of results for %s.%s: %d [required=%d]", suite_name, test_name, result.get_num_items(), self.required_num_items)
            if result.get_num_items() < self.required_num_items:
                return False
        return True

    def get_result(self, suite_name, test_name):
        """ Return a GBBenchResultProcessor, that can supply
            summarized stats
        """
        key = (suite_name, test_name)
        if not self.results.has_key(key):
            raise RuntimeError("key {} not present".format(key))
        return self.results[key]

    def has_key(self, key):
        """ Do we have any results for key?
            key = (suite name, test name)
        """
        return self.results.has_key(key)

class GBFileResult(object):
    """ Holds the results from a single GoogleBench output file,
        which may have 1 or more benchmarks
    """
    def __init__(self, data):
        """ data - raw contents of the file """
        self.data = data
        # benchmark data converted to objects
        self.benchmarks = []

        # also available via (suite_name, test_name) as a key
        # used for comparing current and historical results
        self.benchmarks_dict = {}

        self._init_benchmark_objects()
        return

    def _init_benchmark_objects(self):
        for bench in self.data['benchmarks']:
            result_obj = GBBenchResult(bench)
            self.benchmarks.append(result_obj)

            key = (result_obj.get_suite_name(), result_obj.get_test_name())
            self.benchmarks_dict[key] = result_obj
        return

    def get_benchmark_name(self):
        return os.path.basename(self.data["context"]["executable"])

    def get_datetime(self):
        """ Return when the result was generated as a datetime """
        date_str = self.data['context']['date']
        return datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")

    def get_keys(self):
        """ Returns all result keys """
        return self.benchmarks_dict.keys()

    def get_result(self, key):
        """ Get a single result object
            key - a tuple (suite_name, test_name)
        """
        return self.benchmarks_dict[key]

class GBBenchResult(object):
    """Holds a single test result """
    def __init__(self, result_dict):
        """result_dict: single test dict, from json Google Benchmark <file>.json
        """
        # sample input below

        # "name": "DataTableBenchmark/SimpleInsert/real_time",
        # "iterations": 5,
        # "real_time": 1.2099044392001815e+03,
        # "cpu_time": 1.2098839266000000e+03,
        # "time_unit": "ms",
        # "items_per_second": 8.2651155545892473e+06

        self.attrs = set()
        for k, val in result_dict.items():
            setattr(self, k, val)
            self.attrs.add(k)
        self._process_name()
        return

    def _process_name(self):
        """Split name into components"""
        parts = self.name.split("/")
        self.suite_name = parts[0]
        self.attrs.add("suite_name")

        self.test_name = parts[1]
        self.attrs.add("test_name")

        # Check whether the last section of the benchmark contains the 
        # string "manual_time". If it does, then we will use the "real_time" 
        # measurement. All other benchmarks will just use "cpu_time"
        if parts[-1] == "manual_time":
            self.time_type = "real_time"
        
        # But it also means that the last section *could be* the arguments for a single 
        # benchmark invocation. This means that we need to make sure that we include them in
        # the test name only if there are more than two parts in the fullname, otherwise they will 
        # just overwrite each others results (which is a bad thing!)
        else:
            self.time_type = "cpu_time"
            if len(parts) == 3:
                self.test_name += "({})".format(parts[-1])
        
        self.attrs.add("time_type")
        
        return

    def add_timestamp(self, timestamp):
        """ timestamp: as a datetime """
        self.timestamp = timestamp
        return

    def get_suite_name(self):
        """ Return test suite name """
        return self.suite_name

    def get_test_name(self):
        """ Return test name """
        return self.test_name

    def get_time(self):
        """ Return execution time. Elapsed or CPU specified by the
            test.
        """
        return getattr(self, self.time_type)

    def get_time_secs(self):
        """ Return execution time, normalized to seconds """

        divisor_dict = {"ms" : 10**3,
                        "us" : 10**6,
                        "ns" : 10**9}
        tv = self.get_time()
        time_unit = self.get_time_unit()
        divisor = divisor_dict[time_unit]
        tv = float(tv)/divisor
        return tv

    def get_time_unit(self):
        """ Get execution time unit(s)
            One of
            unit,  multiplier
            ms     1e3
            us     1e6
            ns     1e9
        """
        return self.time_unit

    def get_timestamp(self):
        return self.timestamp

    def get_items_per_second(self):
        """ A performance measure, items per second """
        return self.items_per_second

    def __str__(self):
        ret_st = ""
        for k in self.attrs:
            ret_st = ret_st + "{} : {}\n".format(k, getattr(self, k))
        return ret_st

class GBBenchResultProcessor(object):
    """ Compute selected statistics from a list of GBBenchResult objects
        Computes:
        - mean time (real or cpu, autoselected)
        - mean items_per_second
        - std deviation (TODO)
    """
    def __init__(self):
        """ set initial values """
        self.test_suite = None
        self.test_name = None
        self.time_unit = None
        self.num_results = 0

        self.sum_time = 0.0
        self.sum_items_per_second = 0.0

        self.gbresults = []
        return

    def add_gbresult(self, gb_result, timestamp=None):
        """ add a result, ensuring we have a valid input, consistent
            with results being accumulated
        """
        # check test suite name
        if self.test_suite:
            assert self.test_suite == gb_result.get_suite_name()
        else:
            self.test_suite = gb_result.get_suite_name()

        # check test name
        if self.test_name:
            assert self.test_name == gb_result.get_test_name()
        else:
            self.test_name = gb_result.get_test_name()

        # check units
        if self.time_unit:
            assert self.time_unit == gb_result.get_time_unit()
        else:
            self.time_unit = gb_result.get_time_unit()

        # ok to use
        self.sum_time += gb_result.get_time()
        self.sum_items_per_second += gb_result.get_items_per_second()
        self.num_results += 1

        # save gb_result along with timestamp
        # TODO - possible do this externally, one time
        gb_result.add_timestamp(timestamp)
        self.gbresults.append(gb_result)
        return

    def get_items_per_second_series(self):
        """ Returns (items_per_second series, datetime series) """
        ts_series = []
        ips_series = []

        temp_list =[]
        for gbr in self.gbresults:
            temp_list.append((gbr.get_timestamp(), gbr))
        # sort into time order
        temp_list.sort()
        for ts, gbr in temp_list:
            ts_series.append(ts)
            ips_series.append(gbr.get_items_per_second())
        return (ips_series, ts_series)

    def get_mean_time(self):
        """ Return mean cpu or elapsed time.
            Which one depends upon which value the test is configured
            to use.
        """
        return self.sum_time/self.num_results

    def get_mean_items_per_second(self):
        """ Return mean of items_per_second """
        return self.sum_items_per_second/self.num_results

    def get_num_items(self):
        """ Return number of historical results for this (suite, test)
        """
        return self.num_results

    def get_suite_name(self):
        """ Return test suite name """
        return self.test_suite

    def get_test_name(self):
        """ Return test name """
        return self.test_name

    def get_time_unit(self):
        """ Return units of time """
        return self.time_unit

class GBenchToJUnit(object):
    """Convert a Google Benchmark output file (json) into Junit output file format (xml)
    """
    def __init__(self, input_file, output_file):
        self.input_file = input_file
        self.output_file = output_file

        testsuite_dict = self.read_gb_results(self.input_file)
        LOG.debug("Converting Google Benchmark file '%s' to JUNIT file '%s'", input_file, output_file)
        self.write_output(testsuite_dict)
        return

    def read_gb_results(self, input_file):
        """ Read GoogleBenchmark (json) results and convert to internal form
        """
        # suite level attributes:
        # errors

        # failures
        # name (of suite)?
        # skipped
        # tests (count)
        # timestamp
        # time (duration)

        # for each testcase
        # classname = suitname?
        # name = of test
        # time or perf measure?

        testcases = []
        test_suite = {"testcases" : testcases}

        # read the results file
        LOG.debug("Reading results file '%s'", input_file)
        with open(input_file) as rf:
            gb_data = json.load(rf)

        # convert to internal, intermediate form
        bench_list = gb_data["benchmarks"]
        for bench in bench_list:
            # bench_name = bench["name"]
            one_test_dict = GBBenchResult(bench)

            testcases.append(one_test_dict)

        # pull out the suite_name from the first testcase
        assert len(testcases) > 0
        test_suite["name"] = testcases[0].suite_name

        self._annotate_test_suite(test_suite)
        # returns a dictionary
        return test_suite

    def _annotate_test_suite(self, suite):
        """ Initialize values for computed items """
        suite["errors"] = "0"
        suite["failures"] = "0"
        suite["skipped"] = "0"
        suite["tests"] = str(len(suite["testcases"]))
        return

    def write_output(self, testsuite_dict):
        """ Write results to a JUnit compatible xml file """
        tree = ElementTree.ElementTree()

        test_suite_el = ElementTree.Element("testsuite")
        tree._setroot(test_suite_el)

        # add attributes to root, testsuite element
        for el_name in ["errors",
                        "failures",
                        "skipped",
                        "tests",
                        "name"]:
            test_suite_el.set(el_name, testsuite_dict[el_name])

        # add tests
        for test in testsuite_dict["testcases"]:
            test_el = ElementTree.SubElement(test_suite_el, "testcase")
            test_el.set("classname", test.get_suite_name())
            test_el.set("name", test.get_test_name())

            # set time based on real_time or cpu_time
            test_el.set("time", str(test.get_time_secs()))

        tree.write(self.output_file, xml_declaration=True, encoding='utf8')
        return

class RunMicroBenchmarks(object):
    """ Run micro benchmarks. Output is to json files for post processing.
        Returns True if all benchmarks run, False otherwise
    """
    def __init__(self, config):
        self.config = config
        return

    def run_benchmarks(self, benchmarks, enable_perf):
        """ Return 0 if all benchmarks succeed, otherwise return the error code
            code from the last benchmark to fail
        """
        ret_val = 0

        # iterate over all benchmarks and run them
        cnt = 1
        for bench_name in sorted(benchmarks):
            LOG.info("Running '{}' with {} threads [{}/{}]".format(bench_name, BENCHMARK_THREADS, cnt, len(benchmarks)))
            bench_ret_val = self.run_single_benchmark(bench_name, enable_perf)
            if bench_ret_val:
                LOG.debug("{} terminated with {}".format(bench_name, bench_ret_val))
                ret_val = bench_ret_val
            cnt += 1

        # return fail, if any of the benchmarks failed to run or complete
        return ret_val

    def run_single_benchmark(self, bench_name, enable_perf):
        """ Run benchmark, generate JSON results
        """
        benchmark_path = os.path.join(BENCHMARK_PATH, bench_name)
        output_file = "{}.json".format(bench_name)

        cmd = "{} --benchmark_min_time={} " + \
              " --benchmark_format=json" + \
              " --benchmark_out={}"
        cmd = cmd.format(benchmark_path, config.min_time, output_file)

        # Perf Counter
        if enable_perf:
            try:
                output = subprocess.check_output("perf --version", shell=True)
            except:
                raise Exception("Missing perf binary. Please install package")
            perf_result = "%s.perf" % bench_name
            LOG.debug("Enabling perf data collection [output=%s]", perf_result)
            cmd = "perf record --output={} {}".format(perf_result, cmd)
        
        # Environment Variables
        os.environ["TERRIER_BENCHMARK_THREADS"] = str(BENCHMARK_THREADS) # has to be a str
        os.environ["TERRIER_BENCHMARK_LOGFILE_PATH"] = BENCHMARK_LOGFILE_PATH

        # use all the cpus from the highest numbered numa node
        output = subprocess.check_output("numactl --hardware | grep 'available: ' | cut -d' ' -f2", shell=True)
        if not output:
            raise Exception("Missing numactl binary. Please install package")
        highest_cpu_node = int(output) - 1
        if highest_cpu_node > 0:
            LOG.debug("Number of NUMA Nodes = {}".format(highest_cpu_node))
            LOG.debug("Enabling NUMA support")
            cmd = "numactl --cpunodebind={} --preferred={} {}".format(highest_cpu_node, highest_cpu_node, cmd)
        
        LOG.debug("Executing command [num_threads={}]: {}".format(BENCHMARK_THREADS, cmd))
        proc = subprocess.Popen([cmd], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = proc.communicate()
        ret_val = proc.returncode
        LOG.debug("OUTPUT: %s" % out)

        # convert json results file to xml
        if ret_val == 0:
            xml_output_file = "{}.xml".format(bench_name)
            GBenchToJUnit(output_file, xml_output_file)
        else:
            LOG.error("Unexpected failure of '%s' [ret_val=%d]", bench_name, ret_val)
            LOG.error(err)

        # return the process exit code
        return ret_val

class Jenkins(object):
    """ Wrapper for Jenkins web api """
    def __init__(self, base_url):
        self.base_url = base_url
        return

    def get_builds(self, project, branch, status_filter=None, min_build=None):
        """
        Get the list of builds for the specified project/branch

        Parameters:
        project : string
            Name of project, e.g. Peloton. Required.
        branch : string
            May be None.
        status_filter:
            if provided, filter results

        Returns a list of Build objects
        """

        url = "{}/job/{}".format(self.base_url, project)
        if branch: url = "{}/job/{}".format(url, branch)
        json_url = "{}/api/json".format(url)
        LOG.debug("Retrieving Jenkins JSON data from %s" % json_url)
        try:
            # We are running eval here because Jenkins is returning back Python code
            #data = eval(urllib.urlopen(python_url).read())
            data = json.loads(urllib.urlopen(json_url).read())
        except:
            LOG.error("Unexpected error when retrieving Jenkins data")
            LOG.error(err)
            return []

        # Return a list of build dictionaries. These appear to be by
        # descending build number

        # Each build dict looks like:
        # {'_class': 'org.jenkinsci.plugins.workflow.job.WorkflowRun',
        #  'number': 8,
        #  'url': 'http://jenkins.db.cs.cmu.edu:8080/job/pa_terrier/job/micro_bench/8/'},

        # retrieve data for each build and turn into a Build object
        ret_list = []
        for item in data['builds']:
            build_url = "{}/api/json".format(item['url'])
            LOG.debug("Retrieving build #%d data from %s" % (item['number'], item['url']))
            data = json.loads(urllib.urlopen(build_url).read())
            ret_list.append(Build(data))

        if status_filter:
            ret_list = [build
                        for build in ret_list
                        if build.get_result() == status_filter]

        if min_build:
            ret_list = [build
                        for build in ret_list
                        if build.get_number() >= min_build]

        return ret_list
## CLASS

class ReferenceValue(object):
    """ Container to hold reference benchmark result + result of comparison
        with this execution's benchmark result
    """
    def __init__(self):
        self.key = None
        self.num_results = None
        self.time = None
        self.time_type = None
        self.iterations = 0

        # actual value from benchmark
        self.ips = None
        # reference value
        self.ref_ips = None

        # percentage
        self.tolerance = None

        # "history" or "config"
        self.reference_type = None

        # difference from reference value
        self.percent_diff = None

        # will be True or False
        self.result = None
        return

    def _get_ips_range(self):
        """ return allowable range for ips """
        assert self.ref_ips
        allowed_diff = float(self.ref_ips) * (float(self.tolerance)/100)
        ips_low = self.ref_ips - allowed_diff
        ips_high = self.ref_ips + allowed_diff
        return (ips_low, ips_high)

    def set_ips(self, ips):
        """ Set the current ips value """
        self.ips = ips
        return

    def get_pass_fail_status(self):
        """ Return a status code """
        if self.result is True:
            # passed
            return 0
        # failed
        return 1

    def set_pass_fail(self):
        """ Set pass/fail for this result """
        if self.reference_type == "none":
            # pass if we have no historical data
            self.result = True
        elif self.reference_type in ["history", "lax"]:
            assert self.ref_ips
            ips_low, ips_high = self._get_ips_range()
            # we used to check: (ips_low <= self.ips <= ips_high)
            # which enforces consistency, but fails a run when performance
            # enhancing changes are made. So, disallow low performance,
            # but allow higher
            self.result = (ips_low <= self.ips)

        # also set percentage different from reference
        assert self.ips
        if self.ref_ips:
            self.percent_diff = 100*(self.ips - self.ref_ips)/self.ref_ips
        return

    @classmethod
    def historical(cls, bench_name, in_key, config, gbrp):
        """ Return a ReferenceValue constructed from historical
            benchmark data
        """

        #for x in gbrp.gbresults:
            #pprint(x.__dict__)
        #sys.exit(1)

        suite_name = gbrp.get_suite_name()
        test_name = gbrp.get_test_name()
        LOG.debug("Loading history data for %s.%s [%s]" % (suite_name, test_name, bench_name))
        key = (suite_name, test_name)
        assert key == in_key
        
        ret_obj = cls()
        ret_obj.key = key
        ret_obj.num_results = gbrp.get_num_items()
        ret_obj.time = gbrp.get_mean_time()
        ret_obj.iterations = 888
        ret_obj.ref_ips = gbrp.get_mean_items_per_second()
        ret_obj.tolerance = BENCHMARKS_TO_RUN[bench_name]
        ret_obj.reference_type = "history"
        return ret_obj

    @classmethod
    def lax(cls, bench_name, in_key, config, gbrp):
        """ Return a ReferenceValue constructed from historical
            benchmark data, where fewer historical results are available
            than required. Checks are therefore less strict.
        """
        suite_name = gbrp.get_suite_name()
        test_name = gbrp.get_test_name()
        LOG.debug("Loading laxed history data for %s.%s [%s]" % (suite_name, test_name, bench_name))
        key = (suite_name, test_name)
        assert key == in_key
        
        ret_obj = cls()
        ret_obj.key = key
        ret_obj.num_results = gbrp.get_num_items()
        ret_obj.time = gbrp.get_mean_time()
        ret_obj.iterations = 777
        ret_obj.ref_ips = gbrp.get_mean_items_per_second()
        ret_obj.tolerance = config.lax_tolerance
        ret_obj.reference_type = "lax"
        return ret_obj

    @classmethod
    def config(cls, bench_name, key, config):
        """ Return a ReferenceValue constructed from configuration
            data
        """
        suite_name, test_name = key
        LOG.debug("Loading configuration(?) data for %s.%s [%s]" % (suite_name, test_name, bench_name))
        
        ret_obj = cls()
        ret_obj.key = key
        ret_obj.num_results = 0
        ret_obj.tolerance = 0
        ret_obj.reference_type = "none"
        return ret_obj

    def to_dict(self):
        """ Convert a ReferenceValue into dictionary.
            Used to print a text table
        """
        ret_dict = {}
        suite_name, test_name = self.key
        ret_dict["suite"] = suite_name
        ret_dict["test"] = test_name
        ret_dict["num_results"] = self.num_results
        ret_dict["value"] = self.ips
        ret_dict["iterations"] = self.iterations
        ret_dict["tolerance"] = self.tolerance
        if not self.ref_ips:
            self.ref_ips = 0.0
        ret_dict["reference"] = self.ref_ips

        if not self.percent_diff:
            self.percent_diff = 0
        ret_dict["p_diff"] = self.percent_diff
        ret_dict["reference_type"] = self.reference_type
        if self.result:
            ret_dict["pass"] = "pass"
        else:
            ret_dict["pass"] = "FAIL"
        return ret_dict

class ReferenceValueProvider(object):
    """ Provide reference value(s) for comparing against benchmark
        results, to determine if current results are acceptable.
    """
    def __init__(self, config, ap):
        self.config = config
        self.ap = ap
        return

    def get_reference(self, key, bench_name):
        """ Return reference value(s) """
        if self.ap.results.has_key(key):
            n_desired = ap.get_required_num_items()
            suite_name, test_name = key
            # GBBenchResultProcessor
            gbrp = self.ap.get_result(suite_name, test_name)
            n_actual = gbrp.get_num_items()

            if n_actual >= n_desired:
                # normal
                return ReferenceValue.historical(bench_name, key, self.config, gbrp)
            else:
                # relaxed
                return ReferenceValue.lax(bench_name, key, self.config, gbrp)

        # no checking
        return ReferenceValue.config(bench_name, key, self.config)


def csv_dump(benchmarks, ap):

    # Figure out what benchmark results we want to examine
    suite_test_names = { }
    timestamps = set()
    for suite_name, test_name in ap.results.keys():
        bench_name = ap.suite_name_xref[suite_name]
        if bench_name in benchmarks:
            if not suite_name in suite_test_names: suite_test_names[suite_name] = [ ]
            suite_test_names[suite_name].append(test_name)
            
            # Get all the timestamps for the results
            gbr_p = ap.get_result(suite_name, test_name)
            map(timestamps.add, [ gbr.timestamp for gbr in gbr_p.gbresults ])
    # FOR

    # Now for each timestamp, get all the results across all benchmarks
    # Each row in the CSV output will be for a single timestamp
    writer = csv.writer(sys.stdout, quoting=csv.QUOTE_ALL)
    header = [ "trial", "timestamp" ]
    for suite_name, test_names in sorted(suite_test_names.items()):
        map(header.append, [ "%s::%s" % (suite_name, test_name) for test_name in sorted(test_names) ])
    writer.writerow(header)
    
    trial_ctr = 0
    for timestamp in sorted(timestamps):
        row = [ trial_ctr, timestamp ]
        for suite_name, test_names in sorted(suite_test_names.items()):
            for test_name in sorted(test_names):
                gbr_p = ap.get_result(suite_name, test_name)
                for gbr in gbr_p.gbresults:
                    if gbr.timestamp != timestamp: continue
                    row.append(gbr.items_per_second)
                    #print suite_name, test_name, gbr.items_per_second
                # FOR
            # FOR
        # FOR
        writer.writerow(row)
        trial_ctr += 1
    # FOR

    return
# DEF

def table_dump(benchmarks, ap):
    """parse all the result files and compare current results vs. reference"""
    rvp = ReferenceValueProvider(config, ap)
    tt = TextTable()
    ret = 0
    for bench_name in sorted(benchmarks):
        filename = "{}.json".format(bench_name)
        LOG.debug("Loading local benchmark result file '%s'", filename)
        with open(filename) as fh:
            contents = fh.read()
            try:
                data = json.loads(contents)
            except:
                LOG.error("Invalid data read from benchmark result file '%s'", filename)
                LOG.error(contents)
                raise
        bench_results = GBFileResult(data)

        # iterate over (test suite, benchmark)
        for key in sorted(bench_results.get_keys()):
            # get the GBBenchResult object
            result = bench_results.get_result(key)
            LOG.debug("%s Result:\n%s", bench_name, result)

            # get reference value to compare against
            reference = rvp.get_reference(key, bench_name)

            # if reference.reference_type == "history":
            reference.set_ips(result.get_items_per_second())
            reference.iterations = result.iterations

            reference.set_pass_fail()
            tt.add_row(reference.to_dict())
            status = reference.get_pass_fail_status()
            if status:
                # failed, set exit error code
                ret = 1

    # benchmark key, value, reference, tolerance, reference type, pass
    # add difference
    tt.add_column("pass", heading="    ")
    tt.add_column("value", col_format="%01.4g")
    tt.add_column("iterations", col_format="%d")
    tt.add_column("reference", col_format="%01.4g")
    tt.add_column("tolerance", heading="%tolerance")
    tt.add_column("p_diff", heading="%change", col_format="%+3d")
    # add # ref values
    # hist, cfg
    tt.add_column("reference_type", heading="ref type")
    tt.add_column("num_results", heading="#results")
    tt.add_column("suite")
    tt.add_column("test")
    print("")
    print(tt)
    
    return (ret)
# DEF



## =========================================================
## MAIN
## =========================================================

if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument('benchmark',
                        nargs='*',
                        help="Benchmark suite to run [default=ALL]")

    parser.add_argument("--run",
                        action="store_true",
                        dest="run",
                        default=False,
                        help="Run Benchmarks")
    
    parser.add_argument("--local",
                        action="store_true",
                        default=False,
                        help="Store results in local directory")
    
    parser.add_argument("--num-threads",
                        metavar='N',
                        type=int,
                        default=BENCHMARK_THREADS,
                        help="# of threads to use for benchmarks")
    
    parser.add_argument("--logfile-path",
                        metavar='P',
                        type=str,
                        default=BENCHMARK_LOGFILE_PATH,
                        help="Path to use for benchmark WAL files")
    
    parser.add_argument("--min-ref-values",
                        metavar='M',
                        type=int,
                        default=MIN_REF_VALUES,
                        help="Minimal # of values needed to enforce threshold")
        
    parser.add_argument("--benchmark-path",
                        metavar='B',
                        type=str,
                        default=BENCHMARK_PATH,
                        help="Path to benchmark binaries")

    parser.add_argument("--csv-dump",
                        action="store_true",
                        default=False,
                        help="Print results to stdout as CSV")

    parser.add_argument("--debug",
                        action="store_true",
                        dest="debug",
                        default=False,
                        help="Enable debug output")

    parser.add_argument("--perf",
                        action="store_true",
                        default=False,
                        help="Enable perf counter recording")

    args = parser.parse_args()

    # -------------------------------------------------------


    if args.debug: LOG.setLevel(logging.DEBUG)
    if args.num_threads: BENCHMARK_THREADS = args.num_threads
    if args.logfile_path: BENCHMARK_LOGFILE_PATH = args.logfile_path
    if args.benchmark_path: BENCHMARK_PATH = args.benchmark_path

    # -------------------------------------------------------

    config = Config()

    if not os.path.exists(BENCHMARK_PATH):
        LOG.error("The benchmark executable path directory '%s' does not exist" % BENCHMARK_PATH)
        sys.exit(1)
    benchmarks = sorted(args.benchmark) if args.benchmark else config.benchmarks
    for b in benchmarks:
        if not b.endswith("_benchmark"):
            LOG.error("Invalid target benchmark '%s'" % b)
            sys.exit(1)
    
    builds_to_skip = [ ]
    
    if args.local:
        if not os.path.exists(LOCAL_REPO_DIR): os.mkdir(LOCAL_REPO_DIR)
    
    # Run benchmarks
    ret = 0
    if args.run:
        run_bench = RunMicroBenchmarks(config)
        ret = run_bench.run_benchmarks(benchmarks, args.perf)

        # Store them locally if necessary
        if args.local:
            # Figure out next directory
            build_dirs = next(os.walk(LOCAL_REPO_DIR))[1]
            last_dir = max(build_dirs) if build_dirs else '000'
            next_dir = os.path.join(LOCAL_REPO_DIR, "%03d" % (int(last_dir)+1))
            LOG.info("Creating new result directory in local data repository '%s'", next_dir)
            os.mkdir(next_dir)
            
            builds_to_skip.append(os.path.basename(next_dir))
            
            # Copy any JSON files that we find into our repository
            for bench_name in benchmarks:
                filename = "{}.json".format(bench_name)
                shutil.copy(filename, next_dir)
                LOG.debug("Copying result file '%s' into '%s'", filename, next_dir)
        # IF 
            
    # need <n> benchmark results to compare against
    ap = ArtifactProcessor(args.min_ref_values)
    LOG.debug("min_ref_values: %d" % args.min_ref_values)
    need_more_builds = True

    ## LOCAL REPOSITORY RESULTS
    if args.local:
        LOG.debug("Processing local data repository '%s'", LOCAL_REPO_DIR)
        for run_dir in reversed(sorted(next(os.walk(LOCAL_REPO_DIR))[1])):
            if os.path.basename(run_dir) in builds_to_skip: 
                LOG.debug("Skipping data dir '%s'", run_dir)
                continue
            LOG.debug("Reading results from local directory '%s'", run_dir)
            for build_file in glob.glob(os.path.join(LOCAL_REPO_DIR, run_dir, '*.json')):
                with open(build_file) as fh:
                    try:
                        contents = fh.read()
                        ap.add_artifact_file(contents)
                    except:
                        LOG.error("Invalid data read from benchmark result file '%s'", build_file)
                        LOG.error(contents)
                        raise
                # Determine if we have enough history. Stop collecting information if we do
                if ap.have_min_history():
                    need_more_builds = False
                    break
            ## FOR
            if not need_more_builds: break
        ## FOR
        
    ## REMOTE JENKINS REPOSITORY RESULTS
    else:
        h = Jenkins(JENKINS_URL)
        data_src_list = config.ref_data_sources
        for repo_dict in data_src_list:
            project = repo_dict.get("project")
            branch = repo_dict.get("branch")
            min_build = repo_dict.get("min_build")

            kwargs = {"min_build" : min_build }
            builds = h.get_builds(project, branch, **kwargs)

            for build in builds:
                LOG.debug("(%s, %s), build=#%d, status=%s", \
                          project, branch, build.get_number(), build.get_result())

                artifacts = build.get_artifacts()
                for artifact in artifacts:
                    artifact_filename = artifact.get_filename()
                    LOG.debug("artifact: {}".format(artifact_filename))

                    ap.add_artifact_file(artifact.get_data())

                # Determine if we have enough history. Stop collecting information if we do
                if ap.have_min_history():
                    need_more_builds = False
                    break

            if not need_more_builds: break
    ## IF (jenkins)

    """
    for key in ap.results.keys():
        suite_name, test_name = key
        v = ap.get_result(suite_name, test_name)
        print v.get_suite_name(), " ",  v.get_test_name()
        print "mean time = ", v.get_mean_time()
        print "num items = ", v.get_num_items()
        print "ips = ",  v.get_mean_items_per_second()
    """

    if args.csv_dump:
        ret = csv_dump(benchmarks, ap)
    else:
        ret = table_dump(benchmarks, ap)


    LOG.debug("Exit code = {}".format(ret))
    sys.exit(ret)
