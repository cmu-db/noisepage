#!/usr/bin/env python3
import os
import subprocess
import time
import shlex
import psycopg2 as psql

from .constants import (DEFAULT_DB_USER, DEFAULT_DB_OUTPUT_FILE, DEFAULT_DB_HOST, DEFAULT_DB_PORT, DEFAULT_DB_BIN,
                        DIR_REPO, DB_START_ATTEMPTS, DEFAULT_DB_WAL_FILE)
from .constants import LOG
from .common import print_pipe


class NoisePageServer:
    """
    NoisePageServer represents a NoisePage DBMS instance.
    """

    def __init__(self, host=DEFAULT_DB_HOST, port=DEFAULT_DB_PORT, build_type='', server_args=None,
                 db_output_file=DEFAULT_DB_OUTPUT_FILE):
        """
        Creates an instance of the DB that can be started, stopped, or restarted.

        Parameters
        ----------
        host : str
            The hostname where the DB should be bound to.
        port : int
            The port that the DB should listen on.
        build_type : str
            The type of build that the DBMS is.
            Valid arguments are "debug" and "release".
        server_args : dict
            A dictionary of arguments to pass to the server.
        db_output_file : str, filepath
            The output file that the DB should output its logs to.
        """
        if server_args is None:
            server_args = {}

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
        """
        Start the database server.


        Parameters
        ----------
        is_dry_run

        Returns
        -------
        True if the database was started successfully and false otherwise.

        Warnings
        --------
        The NoisePage DBMS MUST have logging enabled as it is the stdout output
        which is used to determine whether the database is ready to accept
        incoming connections.
        """
        server_args_str = generate_server_args_str(self.server_args)
        db_run_command = f'{self.build_path} {server_args_str}'

        if is_dry_run:
            LOG.info(f'Dry-run: {db_run_command}')
            return True

        LOG.info(f'Running: {db_run_command}')
        start_time = time.time()
        db_process = subprocess.Popen(shlex.split(db_run_command),
                                           stdout=subprocess.PIPE,
                                           stderr=subprocess.PIPE)
        LOG.info(f'Ran: {db_run_command} [PID={db_process.pid}]')

        while True:
            log_line = db_process.stdout.readline().decode("utf-8").rstrip("\n")
            if log_line != '':
                print(log_line)
            check_line = f'[info] Listening on Unix domain socket with port {self.db_port} [PID={db_process.pid}]'
            now = time.time()
            if log_line.endswith(check_line):
                LOG.info(f'DB process is verified as running in {round(now - start_time, 2)} sec.')
                self.db_process = db_process
                return True
            if now - start_time >= 5:
                LOG.error(f'DBMS [PID={db_process.pid} took more than five seconds to start up. Killing.')
                db_process.kill()
                return False

    def stop_db(self, is_dry_run=False):
        """
        A

        Parameters
        ----------
        is_dry_run

        Returns
        -------

        """
        if not self.db_process:
            raise Exception("System is in an invalid state.")

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


def generate_server_args_str(server_args):
    """ Create a server args string to pass to the DBMS """
    server_args_arr = []
    for attribute, value in server_args.items():
        value = str(value).lower() if isinstance(value, bool) else value
        value = f'={value}' if value != None else ''
        arg = f'-{attribute}{value}'
        server_args_arr.append(arg)

    return ' '.join(server_args_arr)
