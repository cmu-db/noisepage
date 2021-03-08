import argparse

from ..util.constants import LOG
from ..util.db_server import NoisePageServer
from .utils_sql import replica_sync, sql_check, sql_exec


def test_insert_primary_select_replica(servers):
    """

    Parameters
    ----------
    servers : [NoisePageServer]
        The list of servers, where the first element must be the primary.

    Returns
    -------
    True if all tests succeeded. False otherwise.
    """
    primary = servers[0]
    replica1 = servers[1]
    replica2 = servers[2]

    try:
        sql_exec(primary, "CREATE TABLE foo (a INTEGER);")
        sql_exec(primary, "INSERT INTO foo VALUES (1);")

        replica_sync(primary, [replica1])
        sql_check(replica1, "SELECT a FROM foo ORDER BY a ASC;", [(1,)])

        sql_exec(primary, "INSERT INTO foo VALUES (2);")
        replica_sync(primary, [replica1, replica2])
        sql_check(replica2, "SELECT a FROM foo ORDER BY a ASC;", [(1,), (2,)])
    except AssertionError as e:
        LOG.error(e)
        return False
    return True


if __name__ == "__main__":
    aparser = argparse.ArgumentParser(description="Simple replication tests.")
    aparser.add_argument("--build-type",
                         default="debug",
                         choices=["debug", "release", "relwithdebinfo"],
                         help="Build type (default: %(default)s")
    args = vars(aparser.parse_args())

    servers = [NoisePageServer(build_type=args['build_type'], port=15721 + i, server_args={
        "port": 15721 + i,
        "messenger_port": 9022 + i,
        "replication_port": 15445 + i,
        "messenger_enable": True,
        "replication_enable": True,
        "network_identity": identity
    }) for (i, identity) in enumerate(["primary", "replica1", "replica2"])]

    try:
        for server in servers:
            server.run_db()
        test_insert_primary_select_replica(servers)
    finally:
        for server in servers:
            try:
                server.stop_db()
            except:
                pass
