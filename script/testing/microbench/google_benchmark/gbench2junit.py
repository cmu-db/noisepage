import json
from xml.etree import ElementTree

from ...util.constants import LOG
from .gbench_test_result import GBenchTestResult


class GBenchToJUnit(object):
    """Convert a Google Benchmark output file (json) into Junit output file format (xml) """

    def __init__(self, input_file):
        self.testcases = read_gbench_results(input_file)
        self.name = self._get_test_suite_name()
        self.tests = str(len(self.testcases))
        self.errors = "0"
        self.failures = "0"
        self.skipped = "0"
        return

    def _get_test_suite_name(self):
        assert len(self.testcases) > 0
        return self.testcases[0].suite_name

    def convert(self, output_file):
        """ Write results to a JUnit compatible xml file """
        LOG.debug("Converting Google Benchmark {NAME} to JUNIT file {JUNIT_FILE}".format(
            NAME=self.name, JUNIT_FILE=output_file
        ))
        tree = ElementTree.ElementTree()
        test_suite_el = ElementTree.Element("testsuite")
        tree._setroot(test_suite_el)

        # add attributes to root, testsuite element
        for el_name in ["errors",
                        "failures",
                        "skipped",
                        "tests",
                        "name"]:
            test_suite_el.set(el_name, getattr(self, el_name))

        # add test results
        for test in self.testcases:
            test_el = ElementTree.SubElement(test_suite_el, "testcases")
            test_el.set("classname", test.suite_name)
            test_el.set("name", test.name)

        tree.write(output_file, xml_declaration=True, encoding='utf8')
        return


def read_gbench_results(gbench_results_file):
    """ Based on a test result file get a list of all the GBenchTestResult
    objects """
    LOG.debug("Reading results file {}".format(gbench_results_file))
    testcases = []
    with open(gbench_results_file) as gbench_result_file:
        gbench_data = json.load(gbench_result_file)
        for benchmark in gbench_data.get('benchmarks'):
            test_result = GBenchTestResult(benchmark)
            testcases.append(test_result)

    return testcases
