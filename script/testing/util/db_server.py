#!/usr/bin/env python3
import os
import subprocess
import time
import shlex
import psycopg2 as psql
from util.constants import (DEFAULT_DB_USER, DEFAULT_DB_OUTPUT_FILE, DEFAULT_DB_HOST, DEFAULT_DB_PORT, DEFAULT_DB_BIN, DIR_REPO,
                            DB_START_ATTEMPTS, DEFAULT_DB_WAL_FILE)
from util.constants import LOG
from util.common import run_check_pids, run_kill_server, print_pipe


class NoisePageServer:
    def __init__(self, host=DEFAULT_DB_HOST, port=DEFAULT_DB_PORT, build_type='', server_args={}, db_output_file=DEFAULT_DB_OUTPUT_FILE):
        """ 
        This class creates an instance of the DB that can be started, stopped, or restarted. 

        Args:
            host - The hostname where the DB should run
            port - The port where the DB should run
            build_type - The name of the build (i.e. release, debug, etc.)
            server_args - A string of server args as you would pass them in the command line
            db_output_file - The file where the DB outputs its logs to
        """
        default_server_args = {
            'wal_file_path': DEFAULT_DB_WAL_FILE
        }
        self.db_host = host
        self.db_port = port
        self.build_path = get_build_path(build_type)
        self.server_args = {**default_server_args, **server_args}
        self.db_output_file = db_output_file
        self.db_process = None

    def run_db(self, is_dry_run=False):
        """ Start the DB server """
        # Allow ourselves to try to restart the DBMS multiple times
        attempt_to_start_time = time.perf_counter()
        server_args_str = generate_server_args_str(self.server_args)
        db_run_command = f'{self.build_path} {server_args_str}'
        if is_dry_run:
            LOG.info(f'Server start command: {db_run_command}')
            return
        for attempt in range(DB_START_ATTEMPTS):
            # Kill any other noisepage processes that our listening on our target port
            # early terminate the run_db if kill_server.py encounter any exceptions
            run_kill_server(self.db_port)

            # use memory buffer to hold db logs
            self.db_process = subprocess.Popen(shlex.split(db_run_command), stdout=subprocess.PIPE,
                                               stderr=subprocess.PIPE)
            LOG.info(f'Server start: {db_run_command} [PID={self.db_process.pid}]')

            if not run_check_pids(self.db_process.pid):
                LOG.info(f'{self.db_process.pid} does not exist. Trying again.')
                # The DB process does not exist, try starting it again
                continue

            while True:
                raw_db_log_line = self.db_process.stdout.readline()
                if not raw_db_log_line:
                    break
                if has_db_started(raw_db_log_line, self.db_port, self.db_process.pid):
                    db_start_time = time.perf_counter()
                    LOG.info(
                        f'DB process is verified as running in {round(db_start_time - attempt_to_start_time,2)} sec')
                    return
            time.sleep(2 ** attempt)  # exponential backoff
        db_failed_to_start_time = time.perf_counter()
        raise RuntimeError(
            f'Failed to start DB after {DB_START_ATTEMPTS} attempts and {round(db_failed_to_start_time - attempt_to_start_time,2)} sec')

    def stop_db(self, is_dry_run=False):
        """ Stop the Db server and print it's log file """
        if not self.db_process or is_dry_run:
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
        else:
            # still (correctly) running, terminate it
            self.db_process.terminate()
            LOG.info("Stopped DB successfully")
        self.db_process = None

    def restart_db(self, is_dry_run=False):
        """ Restart the DB """
        self.stop_db(is_dry_run)
        self.run_db(is_dry_run)

    def delete_wal(self):
        """ Check that the WAL exists and delete if it does """
        if not self.server_args.get('wal_enable', True):
            return
        wal_file_path = self.server_args.get('wal_file_path', DEFAULT_DB_WAL_FILE)
        if os.path.exists(wal_file_path):
            os.remove(wal_file_path)

    def print_db_logs(self):
        """	Print out the remaining DB logs	"""
        LOG.info("************ DB Logs Start ************")
        print_pipe(self.db_process)
        LOG.info("************* DB Logs End *************")

    def execute(self, sql, autocommit=True, expect_result=True, user=DEFAULT_DB_USER):
        """
        Opens up a connection at the DB, and execute a SQL.

        WARNING: this is a really simple (and barely thought-through) client execution interface. Users might need to
        extend this with more arguments or error checking. Only SET SQl command has been tested.

        :param sql: SQL to be execute
        :param autocommit: If the connection should be set to autocommit. For SQL that should not run in transactions,
            this should be set to True, e.g. SET XXX
        :param expect_result: True if results rows are fetched and returned
        :param user: User of this connection
        :return: None if error or not expecting results, rows fetched when expect_result is True
        """
        try:
            with psql.connect(port=self.db_port, host=self.db_host, user=user) as conn:
                conn.set_session(autocommit=autocommit)
                with conn.cursor() as cursor:
                    cursor.execute(sql)

                    if expect_result:
                        rows = cursor.fetchall()
                        return rows

                    return None
        except Exception as e:
            LOG.error(f"Executing SQL = {sql} failed: ")
            # Re-raise this
            raise e


def get_build_path(build_type):
    """ Get the path to the binary """
    path_list = [
        ("standard", "build"),
        ("CLion", "cmake-build-{}".format(build_type)),
    ]
    for _, path in path_list:
        db_bin_path = os.path.join(DIR_REPO, path, "bin", DEFAULT_DB_BIN)
        LOG.debug(f'Tring to find build path in {db_bin_path}')
        if os.path.exists(db_bin_path):
            return db_bin_path

    raise RuntimeError(f'No DB binary found in {path_list}')


def has_db_started(raw_db_log_line, port, pid):
    """ Check whether the db has started by checking its log """
    log_line = raw_db_log_line.decode("utf-8").rstrip("\n")
    LOG.debug(log_line)
    check_line = f'[info] Listening on Unix domain socket with port {port} [PID={pid}]'
    return log_line.endswith(check_line)


def generate_server_args_str(server_args):
    """ Create a server args string to pass to the DBMS """
    server_args_arr = []
    for attribute, value in server_args.items():
        value = str(value).lower() if isinstance(value, bool) else value
        value = f'={value}' if value != None else ''
        arg = f'-{attribute}{value}'
        server_args_arr.append(arg)

    return ' '.join(server_args_arr)
