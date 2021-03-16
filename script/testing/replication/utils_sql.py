import time

from ..util.constants import LOG
from ..util.db_server import NoisePageServer


def sql_exec(node, sql, quiet=False):
    """
    Wrapper around NoisePageServer.execute() when no results are expected.

    Parameters
    ----------
    node : NoisePageServer
        The node to execute the SQL on.
    sql : str
        The SQL string to be executed.
    """
    return node.execute(sql, expect_result=False, quiet=quiet)


def sql_query(node, sql, quiet=False):
    """
    Wrapper around NoisePageServer.execute() when results are expected.

    Parameters
    ----------
    node : NoisePageServer
        The node to execute the SQL on.
    sql : str
        The SQL string to be executed.

    Returns
    -------
    The result rows from executing the SQL.
    """
    return node.execute(sql, expect_result=True, quiet=quiet)


def sql_get_last_record_id(node):
    """
    Get the last record ID of the node.

    Parameters
    ----------
    node : NoisePageServer
        The node that the last record ID is being obtained from.

    Returns
    -------
    last_record_id : int
        The last record ID of the node.
    """
    result = sql_query(node, "SELECT replication_get_last_record_id();")
    # The result comes back as a list of tuples.
    assert len(result) == 1
    assert len(result[0]) == 1
    return result[0][0]


def sql_check(node, sql, expected_result):
    """
    Check the result of executing the provided SQL on the provided node.

    Parameters
    ----------
    node : NoisePageServer
        The node to execute the SQL on.
    sql : str
        The SQL to be executed.
    expected_result : list
        The expected result of executing the SQL.

    Raises
    ------
    AssertionError if the results did not match.
    """
    result = sql_query(node, sql)
    if expected_result != result:
        raise AssertionError(f'Executed SQL "{sql}" on {node.identity}: got {result}, expected {expected_result}')


def replica_sync(primary, replicas, quiet=False):
    """
    Wait until the specified node has applied the last record ID on the primary.

    Parameters
    ----------
    primary : NoisePageServer
        The primary node.
    replicas : [NoisePageServer]
        The replicas to be synced.
    quiet : bool
        False if the checks should be printed as they happen.
    """
    primary_rec_id = sql_get_last_record_id(primary)
    for replica in replicas:
        while True:
            replica_rec_id = sql_get_last_record_id(replica)
            if not quiet:
                LOG.info(f"Syncing replica: [primary@{primary_rec_id}] [{replica.identity}@{replica_rec_id}]")
            if replica_rec_id >= primary_rec_id:
                break
            # A small sleep is added between checks to avoid spamming the log, giving replication a chance to happen.
            time.sleep(2)
