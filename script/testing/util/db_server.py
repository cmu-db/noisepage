#!/usr/bin/env python3
import os
import socket
import subprocess
import sys
import time
import traceback
import shlex
from typing import List
from util.constants import (DEFAULT_DB_OUTPUT_FILE, DEFAULT_DB_HOST, DEFAULT_DB_PORT, DEFAULT_DB_BIN, DIR_REPO,
                            DB_START_ATTEMPTS)
from util.test_case import TestCase
from util.constants import LOG, ErrorCode
from util.periodic_task import PeriodicTask
from util.common import (run_command, print_file, run_check_pids,
                         run_kill_server, print_pipe, update_mem_info)


class NoisePageServer:
    def __init__(self, host=DEFAULT_DB_HOST, port=DEFAULT_DB_PORT, built_type='', server_args=''): #TODO: figure out if I need some kinda 
        self.db_output_file = DEFAULT_DB_OUTPUT_FILE
        self.db_host = host
        self.db_port = port
        self.build_path = get_build_path(build_type)
        self.server_args = server_args.strip()
        self.db_process = None
        #TODO: Do I need to set db path??

    def run_db(self):
        """ Start the DB server """
        # Allow ourselves to try to restart the DBMS multiple times
        for attempt in range(DB_START_ATTEMPTS):
            # Kill any other noisepage processes that our listening on our target port
            # early terminate the run_db if kill_server.py encounter any exceptions
            run_kill_server(self.db_port)
            
            # use memory buffer to hold db logs
            db_run_command = f'{self.build_path} {self.server_args}'
            self.db_process = subprocess.Popen(shlex.split(db_run_command),stdout=subprocess.PIPE, 
                                                stderr=subprocess.PIPE)
            LOG.info(f'Server start: {self.db_path} [PID={self.db_process.pid}]')

            if not run_check_pids(self.db_process.pid):
                # The DB process does not exist, try starting it again
                continue

            while True:
                raw_db_log_line = self.db_process.stdout.readline()
                if not raw_db_log_line:
                    break
                if self.has_db_started(raw_db_log_line):
                    LOG.info('DB process is verified as running')
                    return

        raise RuntimeError(f'Failed to start DB after {DB_START_ATTEMPTS} attempts')

    def has_db_started(raw_db_log_line):
        log_line = raw_db_log_line.decode("utf-8").rstrip("\n")
        LOG.debug(log_line)
        check_line = f'[info] Listening on Unix domain socket with port {self.db_port} [PID={self.db_process.pid}]'
        return log_line.endswith(check_line)

    def stop_db(self):
        """ Stop the Db server and print it's log file """
        if not self.db_process:
            LOG.debug('DB has already been stopped.')
            return 
        
        # get exit code if any
        self.db_process.poll()
        if self.db_process.returncode is not None:
            # DB already terminated
            msg = f'DB terminated with return code {self.db_process.returncode}'
            LOG.info(msg)
            self.print_db_logs()
            raise RuntimeError(msg)

    def restart_db(self):
        """ Restart the DB """
        self.stop_db()
        self.run_db()
    
    def print_db_logs(self):
        """	
        Print out the remaining DB logs	
        """
        LOG.info("************ DB Logs Start ************")
        print_pipe(self.db_process)
        LOG.info("************* DB Logs End *************")


def get_build_path(build_type):
    path_list = [
        ("standard","build"),
        ("CLion","cmake-build-{}".format(build_type)),
    ]
    for _, path in path_list:
        db_bin_path = os.path.join(DIR_REPO, path, DEFAULT_DB_BIN)
        if os.path.exists(db_bin_path):
            return db_bin_path
    
    raise RuntimeError(f'No DB binary found in {path_list}')




