#!/usr/bin/env python

"""
Run micro-benchmarks on a PR, for the purpose of comparing performance with
the master branch.

Usage:
From the directory in which this script resides
./run_micro_bench.py
"""

import json
import os
import subprocess
import sys
import xml.etree.ElementTree as ElementTree

class GBResult(object):
    """Holds single test result """
    def __init__(self, result_dict):
        """result_dict: single test dict, from json Google Benchmark <file>.json
        """
        
        # sample input below
        """                                                                     
        "name": "DataTableBenchmark/SimpleInsert/real_time",                    
        "iterations": 5,                                                        
        "real_time": 1.2099044392001815e+03,                                    
        "cpu_time": 1.2098839266000000e+03,                                     
        "time_unit": "ms",                                                      
        "items_per_second": 8.2651155545892473e+06                              
        """

        self.attrs = set()
        for k, v in result_dict.items():
            setattr(self, k, v)
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

        if len(parts) == 3:
            self.time_type = parts[2]
            self.attrs.add("time_type")
        return

    def __str__(self):
        st = ""
        for k in self.attrs:
            st = st + "{} : {}\n".format(k, getattr(self,k))
        return st

class GBenchToJUnit(object):
    """Convert a Google Benchmark output file (json) into Junit output file format (xml)
    """
    def __init__(self, input_file, output_file):
        self.input_file = input_file
        self.output_file = output_file

        testsuite_dict = self.read_gb_results(self.input_file)
        self.write_output(self.output_file, testsuite_dict)
        return

    def read_gb_results(self, input_file):
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
        test_suite = { "testcases" : testcases }

        # read the results file                                                 
        with open(input_file) as rf:
            gb_data = json.load(rf)

        # convert to internal, intermediate form                                
        bench_list = gb_data["benchmarks"]
        for bench in bench_list:
            # bench_name = bench["name"]                                        
            one_test_dict = GBResult(bench)
            testcases.append(one_test_dict)

        # pull out the suite_name from the first testcase                       
        assert(len(testcases) > 0)
        test_suite["name"] = testcases[0].suite_name

        self._annotate_test_suite(test_suite)
        # returns a dictionary                                                  
        return test_suite

    def _annotate_test_suite(self, suite):
        suite["errors"] = "0"
        suite["failures"] = "0"
        suite["skipped"] = "0"
        suite["tests"] = str(len(suite["testcases"]))

        return
    
    def write_output(self, output_file, testsuite_dict):
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
            test_el = ElementTree.SubElement(test_suite_el,"testcase")
            test_el.set("classname", getattr(test, "suite_name"))
            test_el.set("name", getattr(test,"test_name"))
            test_el.set("time", str(getattr(test, "items_per_second")))

        tree.write(self.output_file, xml_declaration=True, encoding='utf8')
        return

class RunMicroBenchmarks(object):
    """ Run micro benchmarks. Output is to json files for post processing.
        Returns True if all benchmarks run, False otherwise
    """
    def __init__(self):
        # list of benchmarks to run
        self.benchmark_list = ["data_table_benchmark",
                               "tuple_access_strategy_benchmark"]

        # minimum run time for the benchmark
        self.min_time = 10
        return

    def run_all_benchmarks(self):
        """ Return 0 if all benchmarks succeed, otherwise return the error code
            code from the last benchmark to fail
        """
        ret_val = 0

        # iterate over all benchmarks and run them
        for benchmark_name in self.benchmark_list:
            bench_ret_val = self.run_single_benchmark(benchmark_name)
            if bench_ret_val:
                print "{} terminated with {}".format(benchmark_name, bench_ret_val)
                ret_val = bench_ret_val

        # return fail, if any of the benchmarks failed to run or complete
        return ret_val

    def run_single_benchmark(self, benchmark_name):
        """ Run benchmark, generate JSON results
        """
        benchmark_path = os.path.join("../../build/release", benchmark_name)
        output_file = "{}_out.json".format(benchmark_name)

        cmd = "{} --benchmark_min_time={} " + \
              " --benchmark_format=json" + \
              " --benchmark_out={}"
        cmd = cmd.format(benchmark_path,
                         self.min_time,
                         output_file)

        ret_val = subprocess.call([cmd],
                                  shell=True,
                                  stdout=sys.stdout,
                                  stderr=sys.stderr)

        # convert json results file to xml
        xml_output_file = "{}_out.xml".format(benchmark_name)
        gb_to_ju = GBenchToJUnit(output_file, xml_output_file)
        
        # return the process exit code
        return ret_val

if __name__ == "__main__":
    run_bench = RunMicroBenchmarks()
    ret = run_bench.run_all_benchmarks()
    sys.exit(ret)

