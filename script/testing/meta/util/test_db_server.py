# test_db_server.py
# Unit tests for test database server class (db_server.py).

import unittest
from typing import List

# The module under test
from util.db_server import construct_server_argument, construct_server_args_string

# A dummy path to the binary directory (absolute path)
BIN_DIR = "/noisepage/build/bin"

class TestServerArgumentConstruction(unittest.TestCase):
    """
    Test the construction of individual arguments passed 
    to the database server. Tests in this TestCase verify
    that attribute and value preprocessors are behaving
    as expected.
    """

    def test_boolean_lowering_true(self):
        r = construct_server_argument("wal_enable", True, {})
        self.assertEqual(r, "-wal_enable=true")

    def test_boolean_lowering_false(self):
        r = construct_server_argument("wal_enable", False, {})
        self.assertEqual(r, "-wal_enable=false")

    def test_flag_is_none(self):
        r = construct_server_argument("attribute", None, {})
        self.assertEqual(r, "-attribute")

    def test_flag_not_none(self):
        r = construct_server_argument("attribute", "value", {})
        self.assertEqual(r, "-attribute=value")

    def test_non_path_is_not_expanded_current(self):
        r = construct_server_argument("some_argument", "./hello", {"bin_dir": "/some/binary/directory"})
        self.assertEqual(r, "-some_argument=./hello")

    def test_non_path_is_not_expanded_parent(self):
        r = construct_server_argument("some_argument", "../hello", {"bin_dir": "/some/binary/directory"})
        self.assertEqual(r, "-some_argument=../hello")

    def test_path_is_expanded_current(self):
        r = construct_server_argument("argument_path", "./hello", {"bin_dir": "/some/binary/directory"})
        self.assertEqual(r, "-argument_path=/some/binary/directory/hello")

    def test_path_is_expanded_parent(self):
        r = construct_server_argument("argument_path", "../hello", {"bin_dir": "/some/binary/directory"})
        self.assertEqual(r, "-argument_path=/some/binary/hello")

class TestServerArgumentStringConstruction(unittest.TestCase):
    """
    Test the construction of a full server argument string
    from one or many individual arguments (attribute:value)
    that must be forwarded to the DBMS server.
    """
    
    def test_single_argument_0(self):
        r = construct_server_args_string({"wal_enable": True}, BIN_DIR)
        self.assertEqual(r, "-wal_enable=true")

    def test_single_argument_1(self):
        r = construct_server_args_string({"wal_enable": False}, BIN_DIR)
        self.assertEqual(r, "-wal_enable=false")

    def test_multi_argument_0(self):
        r = construct_server_args_string({"attr0": "value0", "attr1": "value1"}, BIN_DIR)
        e = "-attr0=value0 -attr1=value1".split()
        self.assertTrue(contains_all_substrings(r, e))

    def test_multi_argument_1(self):
        r = construct_server_args_string({"attr0": True, "attr1": "value1"}, BIN_DIR)
        e = "-attr0=true -attr1=value1".split()
        self.assertTrue(contains_all_substrings(r, e))

    def test_multi_argument_2(self):
        r = construct_server_args_string({"attr0": "value0", "attr1": True}, BIN_DIR)
        e = "-attr0=value0 -attr1=true".split()
        self.assertTrue(contains_all_substrings(r, e))

    def test_multi_argument_3(self):
        r = construct_server_args_string({"attr0": True, "attr1": True}, BIN_DIR)
        e = "-attr0=true -attr1=true".split()
        self.assertTrue(contains_all_substrings(r, e))

def contains_all_substrings(haystack: str, needles: List[str]) -> bool:
    """
    Determine if the string `haystack` contains each of the
    strings in `needles` as substrings.

    We use this method to check the correctness of the full
    server argument string construction because we don't want
    to bake-in any guarantees regarding the order in which
    arguments are processed, because correctness does not
    ultimately depend on the ordering.

    :param haystack The full query string
    :param needles A list of substrings for which to search
    :return `True` if `haystack` contains all of the strings
    provided in `needles` as substrings, `False` otherwise
    (note that all([]) returns `True` (vacuously true))
    """
    return all(needle in haystack for needle in needles)

if __name__ == "__main__":
    unittest.main()
