#!/usr/bin/python3
from junit import constants
from util.test_server import TestServer
from util.common import write_file, xml_prettify
from xml.etree import ElementTree as et


class TestJUnit(TestServer):
    """
    Class to run JUnit tests
    """
    def __init__(self, args):
        TestServer.__init__(self, args)
        self.test_command = constants.JUNIT_TEST_COMMAND
        self.test_command_cwd = constants.JUNIT_TEST_DIR
        self.test_error_msg = constants.JUNIT_TEST_ERROR_MSG
        self.test_output_file = self.args.get("test_output_file")
        if not self.test_output_file:
            self.test_output_file = constants.JUNIT_OUTPUT_FILE

    def run_pre_test(self):
        self.write_config_file()

    def write_config_file(self):
        options = et.Element("Options")

        # config QueryMode
        query_mode_str = self.args.get("query_mode")
        query_mode_elem = et.SubElement(options, "QueryMode")
        query_mode_elem.text = query_mode_str

        write_file(constants.JUNIT_OPTION_XML, xml_prettify(options))
