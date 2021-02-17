#!/usr/bin/python3

import time

from ..util.db_server import NoisePageServer


def _run_sql(node : NoisePageServer, sql, expect_result, delay):
    print(f"Executing SQL on {node.server_args['network_identity']}: {sql}")
    result = node.execute(sql, expect_result=expect_result)
    time.sleep(delay)
    return result


def exec_sql(node, sql, delay=0):
    return _run_sql(node, sql, False, delay)


def query_sql(node, sql, delay=0):
    return _run_sql(node, sql, True, delay)


def get_last_record_id(node):
    return query_sql(primary, "SELECT replication_get_last_record_id();")


def wait_until_replicated(node, last_record_id):
    while True:
        r = query_sql(node, "SELECT replication_get_last_record_id();", delay=3)
        print(r, last_record_id)
        if r >= last_record_id:
            return

if __name__ == "__main__":
    # TODO(WAN): configurable build type
    servers = [NoisePageServer(build_type="debug", port=15721 + i, server_args={
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

        primary = servers[0]
        replica1 = servers[1]
        replica2 = servers[2]

        time.sleep(5)

        exec_sql(primary, "CREATE TABLE foo (a INTEGER);")
        exec_sql(primary, "INSERT INTO foo VALUES (1);")

        wait_until_replicated(replica1, get_last_record_id(primary))
        q1 = query_sql(replica1, "SELECT a FROM foo ORDER BY a ASC;")
        assert [(1,)] == q1, q1

        exec_sql(primary, "INSERT INTO foo VALUES (2);")
        wait_until_replicated(replica1, get_last_record_id(primary))
        wait_until_replicated(replica2, get_last_record_id(primary))
        q2 = query_sql(replica2, "SELECT a FROM foo ORDER BY a ASC;")
        assert [(1,),(2,)] == q2, q2
    # TODO(WAN): catch assertion errors
    finally:
        for server in servers:
            server.stop_db()
