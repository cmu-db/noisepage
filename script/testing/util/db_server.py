# db_server.py
# Class definition for `NoisePageServer` used in JUnit tests.

import os
import time
import shlex
import pathlib
import subprocess

import psycopg2 as psql

from typing import Dict, List

from .common import print_pipe
from .constants import (DEFAULT_DB_BIN, DEFAULT_DB_HOST,
                        DEFAULT_DB_OUTPUT_FILE, DEFAULT_DB_PORT,
                        DEFAULT_DB_USER, DEFAULT_DB_WAL_FILE, DIR_REPO, LOG)

# -----------------------------------------------------------------------------
# NoisePageServer

class NoisePageServer:
    """
    NoisePageServer represents a NoisePage DBMS instance.
    """

    def __init__(self, host=DEFAULT_DB_HOST, port=DEFAULT_DB_PORT, build_type='', server_args={},
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
        default_server_args = {
            'wal_file_path': DEFAULT_DB_WAL_FILE
        }
        self.db_host = host
        self.db_port = port
        self.binary_dir = get_binary_directory(build_type)
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
        # Construct the server arguments string from the map of arguments
        server_args_str = construct_server_args_string(self.server_args, self.binary_dir)

        # Construct the complete command to launch the DBMS server
        db_run_command = f"{os.path.join(self.binary_dir, DEFAULT_DB_BIN)} {server_args_str}"

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

        check_line = f'[info] Listening on Unix domain socket with port {self.db_port} [PID={db_process.pid}]'
        LOG.info(f'Waiting until DBMS stdout contains: {check_line}')

        while True:
            log_line = db_process.stdout.readline().decode("utf-8").rstrip("\n")
            now = time.time()
            if log_line.strip() != '':
                logs.append(log_line)
            if log_line.endswith(check_line):
                LOG.info(f'DB process is verified as running in {round(now - start_time, 2)} sec.')
                self.db_process = db_process
                log_output = '\n' + '\n'.join(logs)
                LOG.info("************ DB Logs Start ************" + log_output)
                LOG.info("************* DB Logs End *************")
                return True

            if now - start_time >= 600:
                LOG.error('\n'.join(logs))
                LOG.error(f'DBMS [PID={db_process.pid}] took more than 600 seconds to start up. Killing.')
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

        Returns
        -------
        exit_code : int
            The exit code of the DBMS.

        Raises
        ------
        RuntimeError if the DBMS is not running when this is called.
        """
        if is_dry_run and self.db_process:
            raise RuntimeError("Dry-run! Why does the DBMS exist?")

        if not self.db_process:
            raise RuntimeError("System is in an invalid state.")

        # Check if the DBMS is still running.
        return_code = self.db_process.poll()
        if return_code is not None:
            msg = f"DBMS already terminated, code: {self.db_process.returncode}"
            LOG.info(msg)
            self.print_db_logs()
            raise RuntimeError(msg)

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
        self.print_db_logs()
        exit_code = self.db_process.returncode
        LOG.info(f"DBMS stopped, code: {exit_code}")
        self.db_process = None
        return exit_code

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

    @property
    def identity(self):
        return self.server_args.get("network_identity", "")

    def execute(self, sql, expect_result=True, quiet=True, user=DEFAULT_DB_USER, autocommit=True):
        """
        Create a new connection to the DBMS and execute the supplied SQL.

        Parameters
        ----------
        sql : str
            The SQL to be executed.
        expect_result : bool
            True if rows are expected to be fetched and returned.
        quiet : bool
            False if the SQL should be printed before executing. True otherwise.
        user : str
            The default username for this connection.
        autocommit : bool
            True if the connection should autocommit.

        Returns
        -------
        rows
            None if no results are expected.
            Otherwise, the rows that are fetched are returned.

        Raises
        ------
        Exception if any errors happened while executing the SQL.
        """
        try:
            with psql.connect(port=self.db_port, host=self.db_host, user=user) as conn:
                conn.set_session(autocommit=autocommit)
                with conn.cursor() as cursor:
                    if not quiet:
                        LOG.info(f"Executing SQL on {self.identity}: {sql}")
                    cursor.execute(sql)
                    if expect_result:
                        rows = cursor.fetchall()
                        return rows
                    return None
        except Exception as e:
            LOG.error(f"Executing SQL failed: {sql}")
            raise e

# -----------------------------------------------------------------------------
# Server Utilities

def get_binary_directory(build_type):
    """
    Get the path in which the DBMS binary resides.

    Parameters
    ----------
    build_type : str
        One of "standard", "debug", "release".

    Returns
    -------
    The absolute path to the directory in which the DBMS binary is located.

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
        bin_dir = os.path.join(DIR_REPO, path, "bin")
        bin_path = os.path.join(bin_dir, DEFAULT_DB_BIN)
        LOG.debug(f'Locating DBMS binary at: {bin_path}')
        if os.path.exists(bin_path):
            return bin_dir

    raise RuntimeError(f'No DBMS binary found in: {path_list}')

def construct_server_args_string(server_args, bin_dir):
    """ 
    Construct the arguments string to pass to the DBMS server.
    
    Parameters
    ----------
    server_args : dict
        The dictionary of argument:value
    bin_dir : str
        The absolute path to the directory in which the DBMS binary resides

    Returns
    -------
    The complete string of DBMS server arguments.
    """
    # Construct the metadata object that is provided to each preprocessor
    meta = {"bin_dir": bin_dir}

    # Construct the string DBMS argument string
    return " ".join([construct_server_argument(attribute, value, meta) for attribute, value in server_args.items()])

def construct_server_argument(attr, value, meta):
    """
    Construct a DBMS server argument from the associated attribute and value.

    Construction of the arguments to the DBMS server may rely on certain
    preprocessing steps to take place prior to injecting the completed
    argument into the string passed to the DBMS server itself. This function
    composes all of these preprocessing steps that must be applied to each
    attribute - value pair supplied to the DBMS.

    Arguments
    ---------
    attr : str
        The attribute string
    value : str
        The value string
    meta : Dict
        The dictionary of meta-information passed to each preprocessor

    Returns
    -------
    The preprocessed argument in the format expected by the DBMS.
    """
    # Currently do not require any attribute preprocessing
    ATTR_PREPROCESSORS = []

    # The preprocessing steps required for individual argument values
    # NOTE(Kyle): The order of preprocessors is important here because,
    # as one example, resolve_relative_paths looks for a relative path 
    # designation (i.e. './') at the front of the value, while handle_flags
    # will append the necessary '=' for non-flag arguments, which would 
    # obviously confound relative path expansion if applied first.
    VALUE_PREPROCESSORS = [
        resolve_relative_paths,
        lower_booleans,
        handle_flags,
    ]

    # Make the value available to the attribute preprocessors
    attr_meta = {**meta, **{"value": value}}

    # Make the attribute available to the value preprocessors
    value_meta = {**meta, **{"attr": attr}}

    preprocessed_attr  = apply_all(ATTR_PREPROCESSORS, attr, attr_meta)
    preprocessed_value = apply_all(VALUE_PREPROCESSORS, value, value_meta)
    return f"-{preprocessed_attr}{preprocessed_value}"

# -----------------------------------------------------------------------------
# Preprocessing Utilities

class AllTypes:
    """
    A dummy catch-all type for value or attribute preprocessors
    that should ALWAYS be applied, regardless of the type of the
    value or attribute being processed.
    """
    def __init__(self):
        pass

def applies_to(*target_types):
    """
    A decorator that produces a no-op function in the event that the 'target'
    (i.e. first) argument provided to a preprocessing function is not an 
    instance of one of the types specified in the decorator arguments.

    This function should not be invoked directly; it is intended to be used
    as a decorator for preprocessor functions to deal with the fact that 
    certain preprocessing operations are only applicable to certain types.
    """
    def wrap_outer(f):
        def wrap_inner(target, meta):
            # The argument is a targeted type if the catch-all type AllTypes 
            # is provided as an argument to the decorator OR the argument is 
            # an instance of any of the types provided as an argument
            arg_is_targeted_type = AllTypes in target_types or any(isinstance(target, ty) for ty in target_types)
            return f(target, meta) if arg_is_targeted_type else target
        return wrap_inner
    return wrap_outer

# -----------------------------------------------------------------------------
# Attribute Preprocessors
#
# The signature for attribute preprocessors must be:
#   def preprocessor(attr: str, meta: Dict) -> str:
#       ...


# -----------------------------------------------------------------------------
# Value Preprocessors
#
# The signature for value preprocessors must be:
#   def preprocessor(value: str, meta: Dict) -> str:
#       ...

@applies_to(bool)
def lower_booleans(value: str, meta: Dict) -> str:
    """
    Lower boolean string values to the format expected by the DBMS server.
    
    e.g. 
        `True` -> `true`
        `False` -> `false`
    
    Arguments
    ---------
    value : str 
        The DBMS server argument value
    meta : Dict
        Dictionary of meta-information available to all preprocessors

    Returns
    -------
    The preprocessed server argument value
    """
    assert value is True or value is False, "Input must be a first-class boolean type."
    return str(value).lower()

@applies_to(str)
def resolve_relative_paths(value: str, meta: Dict) -> str:
    """
    Resolve relative paths in the DBMS server arguments to their equivalent absolute paths.

    When specifying path arguments to the DBMS, it is often simpler to think in terms
    of how the path relates to the location of the DBMS server binary. However, because
    the command used by NoisePageServer to launch the DBMS server instance specifies
    the absolute path to the binary, relative paths (that necessarily encode the expected
    location of the binary) will fail to function properly. Expanding these relative paths
    to their absolute counterparts before passing them to the DBMS addresses this issue.

    The above highlights the fundamental limitation of this current implementation, namely:

        ALL RELATIVE PATHS ARE ASSUMED TO BE RELATIVE TO THE DBMS BINARY DIRECTORY

    All relative paths that are relative to another directory will fail to resolve properly.

    Arguments
    ---------
    value : str 
        The DBMS server argument value
    meta : Dict
        Dictionary of meta-information available to all preprocessors

    Returns
    -------
    The preprocessed server argument value
    """
    # NOTE(Kyle): This is somewhat dirty because it introduces a
    # 'hidden' dependency that is not reflected in the DBMS code:
    # we only resolve those arguments that actually end with `_path`.
    # In practice, I prefer this to the alternative of just assuming
    # that anything that starts with './' or '../' is a relative path,
    # and it ensures that, at worst, we do LESS resolving than might
    # otherwise be expected, never more.
    is_path = str.endswith(meta["attr"], "_path")
    is_relative = not os.path.isabs(value)
    
    if is_path and is_relative:
        return pathlib.Path(os.path.join(meta["bin_dir"], value)).resolve()
    else:
        return value

@applies_to(AllTypes)
def handle_flags(value: str, meta: Dict) -> str:
    """
    Handle DBMS server arguments with no associated value.

    Some arguments to the DBMS are flags that do not have an associated value;
    in these cases, we do not want to format the complete argument as
    `-attribute=value` and instead want to format it as `-attribute` alone.
    This preprocessor encapsulates the logic for this transformation.

    TODO(Kyle): Do we actually support any arguments like this? 
    I can't seem to come up with any actual examples...

    Arguments
    ---------
    value : str 
        The DBMS server argument value
    meta : Dict
        Dictionary of meta-information available to all preprocessors

    Returns
    -------
    The preprocessed server argument value
    """
    return f"={value}" if value is not None else ""

# -----------------------------------------------------------------------------
# Utility

def apply_all(functions: List, init_obj, meta: Dict):
    """
    Apply all of the functions in `functions` to object `init_obj` sequentially,
    supplying metadata object `meta` to each function invocation.

    TODO(Kyle): Initially I wanted to implement this with function composition
    in terms of functools.reduce() which makes it really beautiful, but there
    we run into issues with multi-argument callbacks, and the real solution is
    to use partial application, but this seemed like overkill... maybe revisit.

    Arguments
    ---------
    functions : List[function]
        The collection of functions to invoke
    init_obj : object
        Arbitrary object to which functions should be applied
    meta : object
        Arbitrary meta-object supplied as second argument to each function invocation

    Returns
    -------
    The result of applying each function in `functions` to `init_obj`.
    """
    obj = init_obj
    for function in functions:
        obj = function(obj, meta)
    return obj
