import os
import shlex
import subprocess
import time

import psycopg2 as psql

from .common import print_pipe
from .constants import (DEFAULT_DB_BIN, DEFAULT_DB_HOST,
                        DEFAULT_DB_OUTPUT_FILE, DEFAULT_DB_PORT,
                        DEFAULT_DB_USER, DEFAULT_DB_WAL_FILE, DIR_REPO, LOG)


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
        Start the DBMS.

        Parameters
        ----------
        is_dry_run : bool
            True if the commands that will be run should be printed,
            with the commands themselves not actually executing.

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

        logs = []
        while True:
            log_line = db_process.stdout.readline().decode("utf-8").rstrip("\n")
            check_line = f'[info] Listening on Unix domain socket with port {self.db_port} [PID={db_process.pid}]'
            now = time.time()
            if log_line.endswith(check_line):
                LOG.info(f'DB process is verified as running in {round(now - start_time, 2)} sec.')
                self.db_process = db_process
                return True
            elif log_line.strip() != '':
                logs.append(log_line)

            if now - start_time >= 60:
                LOG.error('\n'.join(logs))
                LOG.error(f'DBMS [PID={db_process.pid}] took more than 60 seconds to start up. Killing.')
                db_process.kill()
                return False

    def stop_db(self, is_dry_run=False):
        """
        Stop the DBMS. The DBMS must be running!

        Parameters
        ----------
        is_dry_run : bool
            True if the commands that will be run should be printed,
            with the commands themselves not actually executing.

        Raises
        ------
        RuntimeError if the DBMS is not running when this is called.
        """
        if is_dry_run and self.db_process:
            raise RuntimeError("Dry-run! Why does the DBMS exist?")

        if not self.db_process:
            raise RuntimeError("System is in an invalid state.")

        return_code = self.db_process.poll()
        if return_code is None:
            try:
                # Try to kill the process politely and wait for 60 seconds.
                self.db_process.terminate()
                self.db_process.wait(60)
            except subprocess.TimeoutExpired:
                # Otherwise, try to kill the process forcefully and wait another 60 seconds.
                # If the process hasn't died yet, then something terrible has happened and we raise an error.
                self.db_process.kill()
                self.db_process.wait(60)
            except KeyboardInterrupt:
                raise KeyboardInterrupt
            finally:
                unix_socket = os.path.join("/tmp/", f".s.PGSQL.{self.db_port}")
                if os.path.exists(unix_socket):
                    os.remove(unix_socket)
                    LOG.info(f"Removing: {unix_socket}")
            LOG.info(f"DBMS stopped successfully, code: {self.db_process.returncode}")
            self.db_process = None
        else:
            msg = f"DBMS already terminated, code: {self.db_process.returncode}"
            LOG.info(msg)
            self.print_db_logs()
            raise RuntimeError(msg)

    def delete_wal(self):
        """
        If the WAL should exist according to our server arguments,
        then delete the WAL.
        """
        if not self.server_args.get('wal_enable', True):
            return
        wal = self.server_args.get('wal_file_path', DEFAULT_DB_WAL_FILE)
        if os.path.exists(wal):
            os.remove(wal)

    def print_db_logs(self):
        """
        Print out the remaining DBMS logs.
        """
        LOG.info("************ DB Logs Start ************")
        print_pipe(self.db_process)
        LOG.info("************* DB Logs End *************")

    def execute(self, sql, autocommit=True, expect_result=True, user=DEFAULT_DB_USER):
        """
        Create a new connection to the DBMS and execute the supplied SQL.

        Parameters
        ----------
        sql : str
            The SQL to be executed.
        autocommit : bool
            True if the connection should autocommit.
        expect_result : bool
            True if rows are expected to be fetched and returned.
        user : str
            The default username for this connection.

        Returns
        -------
        rows
            None if an error occurs or if no results are expected.
            Otherwise, the rows that are fetched are returned.
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
            LOG.error(f"Executing SQL failed: {sql}")
            raise e


def get_build_path(build_type):
    """
    Get the path to the DBMS binary.

    Parameters
    ----------
    build_type : str
        One of "standard", "debug", "release".

    Returns
    -------
    The path to the DBMS binary to be used.

    Warnings
    --------
    The "standard" build folder (called "build") is always checked first,
    so if the "build" folder has a DBMS binary, then that binary is always used.

    Raises
    ------
    RuntimeError if no DBMS binary is found.
    """
    path_list = [
        ("standard", "build"),
        ("CLion", "cmake-build-{}".format(build_type)),
    ]
    for _, path in path_list:
        db_bin_path = os.path.join(DIR_REPO, path, "bin", DEFAULT_DB_BIN)
        LOG.debug(f'Locating DBMS binary in: {db_bin_path}')
        if os.path.exists(db_bin_path):
            return db_bin_path

    raise RuntimeError(f'No DBMS binary found in: {path_list}')


def generate_server_args_str(server_args):
    """ Create a server args string to pass to the DBMS """
    server_args_arr = []
    for attribute, value in server_args.items():
        value = str(value).lower() if isinstance(value, bool) else value
        value = f'={value}' if value != None else ''
        arg = f'-{attribute}{value}'
        server_args_arr.append(arg)

    return ' '.join(server_args_arr)
