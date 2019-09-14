#!/usr/bin/env python3

import argparse
import os
import subprocess
import sys

VM_TARGET_STRING = 'VM main() returned: '
ADAPTIVE_TARGET_STRING = 'ADAPTIVE main() returned: '
JIT_TARGET_STRING = 'JIT main() returned: '
TARGET_STRINGS = [VM_TARGET_STRING, ADAPTIVE_TARGET_STRING, JIT_TARGET_STRING]


def run(tpl_bin, tpl_file, is_sql):
    args = [tpl_bin]
    if is_sql:
        args.append("-sql")
    args.append(tpl_file)
    proc = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    result = []
    #print("tpl_file stdout:")
    #print(proc.stdout.decode('utf-8'))
    #print("tpl_file stderr:")
    #print(proc.stderr.decode('utf-8'))
    for line in reversed(proc.stdout.decode('utf-8').split('\n')):
        if "ERROR" in line or "error" in line:
            return []
        for target_string in TARGET_STRINGS:
            idx = line.find(target_string)
            if idx != -1:
                result.append(line[idx + len(target_string):])
    return result


def check(tpl_bin, tpl_folder, tpl_tests_file, build_dir):
    os.chdir(build_dir)
    with open(tpl_tests_file) as tpl_tests:
        num_tests, failed = 0, set()
        print('Tests:')

        for line in tpl_tests:
            line = line.strip()
            if not line or line[0] == '#':
                continue
            tpl_file, sql, expected_output = [x.strip() for x in line.split(',')]
            is_sql = sql.lower() == "true"
            res = run(tpl_bin, os.path.join(tpl_folder, tpl_file), is_sql)
            num_tests += 1

            report = 'PASS'
            if not res:
                report = 'ERR'
                failed.add(tpl_file)
            elif  len(res) != 3 or not all(output == expected_output for output in res):
                report = 'FAIL [expect: {}, actual: {}]'.format(expected_output,
                                                                res)
                failed.add(tpl_file)

            print('\t{}: {}'.format(tpl_file, report))
        print('{}/{} tests passed.'.format(num_tests - len(failed), num_tests))

        if len(failed) > 0:
            print('{} failed:'.format(len(failed)))
            for fail in failed:
                print('\t{}'.format(fail))
            sys.exit(-1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-b', dest='tpl_bin', help='TPL binary.')
    parser.add_argument('-f', dest='tpl_tests_file',
                        help='File containing <tpl_test, expected_output> lines.')
    parser.add_argument('-t', dest='tpl_folder', help='TPL tests folder.')
    parser.add_argument('-d', dest='build_dir', help='Build Directory.')
    args = parser.parse_args()
    check(args.tpl_bin, args.tpl_folder, args.tpl_tests_file, args.build_dir)


if __name__ == '__main__':
    main()
