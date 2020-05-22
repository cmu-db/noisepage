#include "traffic_cop/traffic_cop.h"

#include <memory>
#include <pqxx/pqxx>  // NOLINT
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/settings.h"
#include "gtest/gtest.h"
#include "main/db_main.h"
#include "network/connection_handle_factory.h"
#include "network/terrier_server.h"
#include "storage/garbage_collector.h"
#include "test_util/manual_packet_util.h"
#include "test_util/test_harness.h"
#include "traffic_cop/traffic_cop_defs.h"
#include "transaction/deferred_action_manager.h"
#include "transaction/transaction_manager.h"

namespace terrier::trafficcop {

class TrafficCopTests : public TerrierTest {
 protected:
  void SetUp() override {
    std::unordered_map<settings::Param, settings::ParamInfo> param_map;
    terrier::settings::SettingsManager::ConstructParamMap(param_map);

    db_main_ = terrier::DBMain::Builder()
                   .SetSettingsParameterMap(std::move(param_map))
                   .SetUseSettingsManager(true)
                   .SetUseGC(true)
                   .SetUseCatalog(true)
                   .SetUseGCThread(true)
                   .SetUseTrafficCop(true)
                   .SetUseStatsStorage(true)
                   .SetUseLogging(true)
                   .SetUseNetwork(true)
                   .SetUseExecution(true)
                   .Build();

    db_main_->GetNetworkLayer()->GetServer()->RunServer();

    port_ = static_cast<uint16_t>(db_main_->GetSettingsManager()->GetInt(settings::Param::port));
    catalog_ = db_main_->GetCatalogLayer()->GetCatalog();
    txn_manager_ = db_main_->GetTransactionLayer()->GetTransactionManager();
  }

  std::unique_ptr<DBMain> db_main_;
  uint16_t port_;
  common::ManagedPointer<catalog::Catalog> catalog_;
  common::ManagedPointer<transaction::TransactionManager> txn_manager_;
};

// NOLINTNEXTLINE
TEST_F(TrafficCopTests, EmptyCommitTest) {
  try {
    pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
                                            port_, catalog::DEFAULT_DATABASE));

    pqxx::work txn1(connection);
    txn1.commit();
  } catch (const std::exception &e) {
    EXPECT_TRUE(false);
  }
}

// NOLINTNEXTLINE
TEST_F(TrafficCopTests, EmptyAbortTest) {
  try {
    pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
                                            port_, catalog::DEFAULT_DATABASE));

    pqxx::work txn1(connection);
    txn1.abort();
  } catch (const std::exception &e) {
    EXPECT_TRUE(false);
  }
}

// NOLINTNEXTLINE
TEST_F(TrafficCopTests, EmptyStatementTest) {
  try {
    pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
                                            port_, catalog::DEFAULT_DATABASE));

    pqxx::work txn1(connection);
    pqxx::result r = txn1.exec(";");
    txn1.commit();
  } catch (const std::exception &e) {
    EXPECT_TRUE(false);
  }
}

// NOLINTNEXTLINE
TEST_F(TrafficCopTests, BadParseTest) {
  try {
    pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
                                            port_, catalog::DEFAULT_DATABASE));

    pqxx::work txn1(connection);
    pqxx::result r = txn1.exec("INSTERT INTO FOO VALUES (1,1);");
    txn1.commit();
  } catch (const std::exception &e) {
    std::string error(e.what());
    std::string expect("ERROR:  syntax error\n");
    EXPECT_EQ(error, expect);
  }
}

// NOLINTNEXTLINE
TEST_F(TrafficCopTests, BadBindingTest) {
  try {
    pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
                                            port_, catalog::DEFAULT_DATABASE));

    pqxx::work txn1(connection);
    pqxx::result r = txn1.exec("INSERT INTO FOO VALUES (1,1);");
    txn1.commit();
  } catch (const std::exception &e) {
    std::string error(e.what());
    std::string expect("ERROR:  binding failed\n");
    EXPECT_EQ(error, expect);
  }
}

// NOLINTNEXTLINE
TEST_F(TrafficCopTests, BasicTest) {
  try {
    pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
                                            port_, catalog::DEFAULT_DATABASE));

    pqxx::work txn1(connection);
    txn1.exec("CREATE TABLE TableA (id INT PRIMARY KEY, data TEXT);");
    txn1.exec("INSERT INTO TableA VALUES (1, 'abc');");

    pqxx::result r = txn1.exec("SELECT * FROM TableA");
    EXPECT_EQ(r.size(), 1);
    txn1.commit();
  } catch (const std::exception &e) {
    EXPECT_TRUE(false);
  }
}

/**
 * Test whether a temporary namespace is created for a connection to the database
 */
// NOLINTNEXTLINE
TEST_F(TrafficCopTests, TemporaryNamespaceTest) {
  try {
    pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
                                            port_, catalog::DEFAULT_DATABASE));

    pqxx::work txn1(connection);

    // Create a new namespace and make sure that its OID is higher than the default start OID,
    // which should have been assigned to the temporary namespace for this connection
    catalog::namespace_oid_t new_namespace_oid = catalog::INVALID_NAMESPACE_OID;
    do {
      auto txn = txn_manager_->BeginTransaction();
      auto db_oid = catalog_->GetDatabaseOid(common::ManagedPointer(txn), catalog::DEFAULT_DATABASE);
      EXPECT_NE(db_oid, catalog::INVALID_DATABASE_OID);
      auto db_accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid);
      EXPECT_NE(db_accessor, nullptr);
      new_namespace_oid = db_accessor->CreateNamespace(std::string(trafficcop::TEMP_NAMESPACE_PREFIX));
      txn_manager_->Abort(txn);
    } while (new_namespace_oid == catalog::INVALID_NAMESPACE_OID);
    EXPECT_GT(static_cast<uint32_t>(new_namespace_oid), catalog::START_OID);
    txn1.commit();
  } catch (const std::exception &e) {
    EXPECT_TRUE(false);
  }
}

// The tests below are from the old sqlite traffic cop era. Unclear if they should be removed at this time, but for now
// they're disabled

// NOLINTNEXTLINE
TEST_F(TrafficCopTests, DISABLED_ManualExtendedQueryTest) {
  try {
    auto io_socket_unique_ptr = network::ManualPacketUtil::StartConnection(port_);
    auto io_socket = common::ManagedPointer(io_socket_unique_ptr);
    network::PostgresPacketWriter writer(io_socket->GetWriteQueue());

    writer.WriteSimpleQuery("DROP TABLE IF EXISTS TableA");
    io_socket->FlushAllWrites();
    network::ManualPacketUtil::ReadUntilReadyOrClose(io_socket);

    writer.WriteSimpleQuery(
        "CREATE TABLE TableA (a_int INT PRIMARY KEY, a_dec DECIMAL, a_text TEXT, a_time TIMESTAMP, a_bigint BIGINT);");
    io_socket->FlushAllWrites();
    network::ManualPacketUtil::ReadUntilReadyOrClose(io_socket);

    writer.WriteSimpleQuery("INSERT INTO TableA VALUES(100, 3.14, 'nico', 114514, 1234)");
    io_socket->FlushAllWrites();
    network::ManualPacketUtil::ReadUntilReadyOrClose(io_socket);

    std::string stmt_name = "begin_statement";
    std::string query = "BEGIN";

    writer.WriteParseCommand(stmt_name, query, std::vector<int>());
    io_socket->FlushAllWrites();
    network::ManualPacketUtil::ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_PARSE_COMPLETE);

    {
      std::string portal_name = "test_portal";
      // Use text format, don't care about result column formats
      writer.WriteBindCommand(portal_name, stmt_name, {}, {}, {});
      io_socket->FlushAllWrites();
      network::ManualPacketUtil::ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_BIND_COMPLETE);

      writer.WriteExecuteCommand(portal_name, 0);
      io_socket->FlushAllWrites();
      network::ManualPacketUtil::ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_COMMAND_COMPLETE);

      writer.WriteSyncCommand();
      io_socket->FlushAllWrites();
      network::ManualPacketUtil::ReadUntilReadyOrClose(io_socket);
    }

    stmt_name = "test_statement";
    query = "SELECT * FROM TableA WHERE a_int = $1 AND a_dec = $2 AND a_text = $3 AND a_time = $4 AND a_bigint = $5";

    writer.WriteParseCommand(stmt_name, query,
                             std::vector<int>({static_cast<int32_t>(network::PostgresValueType::INTEGER),
                                               static_cast<int32_t>(network::PostgresValueType::DECIMAL),
                                               static_cast<int32_t>(network::PostgresValueType::VARCHAR),
                                               static_cast<int32_t>(network::PostgresValueType::TIMESTAMPS),
                                               static_cast<int32_t>(network::PostgresValueType::BIGINT)}));
    io_socket->FlushAllWrites();
    network::ManualPacketUtil::ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_PARSE_COMPLETE);

    // Bind, param = "100", "3.14", "nico", "114514" expressed in vector form
    auto param1 = std::vector<char>({'1', '0', '0'});
    auto param2 = std::vector<char>({'3', '.', '1', '4'});
    auto param3 = std::vector<char>({'n', 'i', 'c', 'o'});
    auto param4 = std::vector<char>({'1', '1', '4', '5', '1', '4'});
    auto param5 = std::vector<char>({'1', '2', '3', '4'});

    {
      std::string portal_name = "test_portal";
      // Use text format, don't care about result column formats
      writer.WriteBindCommand(portal_name, stmt_name, {}, {&param1, &param2, &param3, &param4, &param5}, {});
      io_socket->FlushAllWrites();
      network::ManualPacketUtil::ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_BIND_COMPLETE);

      writer.WriteDescribeCommand(network::DescribeCommandObjectType::STATEMENT, stmt_name);
      io_socket->FlushAllWrites();
      network::ManualPacketUtil::ReadUntilMessageOrClose(io_socket,
                                                         network::NetworkMessageType::PG_PARAMETER_DESCRIPTION);

      writer.WriteDescribeCommand(network::DescribeCommandObjectType::PORTAL, portal_name);
      io_socket->FlushAllWrites();
      network::ManualPacketUtil::ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_ROW_DESCRIPTION);

      writer.WriteExecuteCommand(portal_name, 0);
      io_socket->FlushAllWrites();
      network::ManualPacketUtil::ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_DATA_ROW);

      writer.WriteSyncCommand();
      io_socket->FlushAllWrites();
      network::ManualPacketUtil::ReadUntilReadyOrClose(io_socket);
    }

    {
      // Test single specifier for all paramters (1)
      // TODO(Weichen): Test binary format here

      std::string portal_name = "test_portal-2";
      // Use text format, don't care about result column formats, specify "0" for using text for all params
      writer.WriteBindCommand(portal_name, stmt_name, {0}, {&param1, &param2, &param3, &param4, &param5}, {});
      io_socket->FlushAllWrites();
      network::ManualPacketUtil::ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_BIND_COMPLETE);

      writer.WriteDescribeCommand(network::DescribeCommandObjectType::PORTAL, portal_name);
      io_socket->FlushAllWrites();
      network::ManualPacketUtil::ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_ROW_DESCRIPTION);

      writer.WriteExecuteCommand(portal_name, 0);
      io_socket->FlushAllWrites();
      network::ManualPacketUtil::ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_DATA_ROW);

      writer.WriteSyncCommand();
      io_socket->FlushAllWrites();
      network::ManualPacketUtil::ReadUntilReadyOrClose(io_socket);
    }

    {
      // Test individual specifier for each parameter

      std::string portal_name = "test_portal-3";
      // Use text format, don't care about result column formats
      writer.WriteBindCommand(portal_name, stmt_name, {0, 0, 0, 0, 0}, {&param1, &param2, &param3, &param4, &param5},
                              {});
      io_socket->FlushAllWrites();
      network::ManualPacketUtil::ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_BIND_COMPLETE);

      writer.WriteDescribeCommand(network::DescribeCommandObjectType::PORTAL, portal_name);
      io_socket->FlushAllWrites();
      network::ManualPacketUtil::ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_ROW_DESCRIPTION);

      writer.WriteExecuteCommand(portal_name, 0);
      io_socket->FlushAllWrites();
      network::ManualPacketUtil::ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_DATA_ROW);

      writer.WriteSyncCommand();
      io_socket->FlushAllWrites();
      network::ManualPacketUtil::ReadUntilReadyOrClose(io_socket);
    }

    // CloseCommand
    writer.WriteCloseCommand(network::DescribeCommandObjectType::STATEMENT, stmt_name);
    io_socket->FlushAllWrites();

    stmt_name = "commit_statement";
    query = "COMMIT";

    writer.WriteParseCommand(stmt_name, query, std::vector<int>());
    io_socket->FlushAllWrites();
    network::ManualPacketUtil::ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_PARSE_COMPLETE);

    {
      std::string portal_name = "test_portal";
      // Use text format, don't care about result column formats
      writer.WriteBindCommand(portal_name, stmt_name, {}, {}, {});
      io_socket->FlushAllWrites();
      network::ManualPacketUtil::ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_BIND_COMPLETE);

      writer.WriteExecuteCommand(portal_name, 0);
      io_socket->FlushAllWrites();

      writer.WriteSyncCommand();
      io_socket->FlushAllWrites();
      network::ManualPacketUtil::ReadUntilReadyOrClose(io_socket);
    }

    network::ManualPacketUtil::TerminateConnection(io_socket->GetSocketFd());
    io_socket->Close();
  } catch (const std::exception &e) {
    EXPECT_TRUE(false);
  }
}

// -------------------------------------------------------------------------

/**
 * The manual tests below are for debugging. They can be disabled when testing other components.
 * You can launch a Postgres backend and compare packets from terrier and from Postgres
 * to find if you have created correct packets.
 *
 */

// NOLINTNEXTLINE
TEST_F(TrafficCopTests, DISABLED_ManualRoundTripTest) {
  try {
    auto io_socket_unique_ptr = network::ManualPacketUtil::StartConnection(port_);
    auto io_socket = common::ManagedPointer(io_socket_unique_ptr);
    network::PostgresPacketWriter writer(io_socket->GetWriteQueue());

    writer.WriteSimpleQuery("DROP TABLE IF EXISTS TableA");
    io_socket->FlushAllWrites();
    network::ManualPacketUtil::ReadUntilReadyOrClose(io_socket);
    writer.WriteSimpleQuery("CREATE TABLE TableA (id INT PRIMARY KEY, data TEXT);");
    io_socket->FlushAllWrites();
    network::ManualPacketUtil::ReadUntilReadyOrClose(io_socket);
    writer.WriteSimpleQuery("INSERT INTO TableA VALUES (1, 'abc');");
    io_socket->FlushAllWrites();
    network::ManualPacketUtil::ReadUntilReadyOrClose(io_socket);

    writer.WriteSimpleQuery("SELECT * FROM TableA");
    io_socket->FlushAllWrites();
    network::ManualPacketUtil::ReadUntilReadyOrClose(io_socket);
    network::ManualPacketUtil::TerminateConnection(io_socket->GetSocketFd());
    io_socket->Close();
  } catch (const std::exception &e) {
    EXPECT_TRUE(false);
  }
}

// NOLINTNEXTLINE
TEST_F(TrafficCopTests, DISABLED_ErrorHandlingTest) {
  auto io_socket_unique_ptr = network::ManualPacketUtil::StartConnection(port_);
  auto io_socket = common::ManagedPointer(io_socket_unique_ptr);
  network::PostgresPacketWriter writer(io_socket->GetWriteQueue());

  writer.WriteSimpleQuery("DROP TABLE IF EXISTS TableA");
  io_socket->FlushAllWrites();
  network::ManualPacketUtil::ReadUntilReadyOrClose(io_socket);
  writer.WriteSimpleQuery("CREATE TABLE TableA (id INT PRIMARY KEY, data TEXT);");
  io_socket->FlushAllWrites();
  network::ManualPacketUtil::ReadUntilReadyOrClose(io_socket);
  writer.WriteSimpleQuery("INSERT INTO TableA VALUES (1, 'abc');");
  io_socket->FlushAllWrites();
  network::ManualPacketUtil::ReadUntilReadyOrClose(io_socket);

  std::string stmt_name = "test_statement";
  std::string query = "SELECT * FROM TableA WHERE id = $1";
  writer.WriteParseCommand(stmt_name, query,
                           std::vector<int>({static_cast<int32_t>(network::PostgresValueType::INTEGER)}));
  io_socket->FlushAllWrites();
  network::ManualPacketUtil::ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_PARSE_COMPLETE);

  {
    // Repeated statement name
    writer.WriteParseCommand(stmt_name, query,
                             std::vector<int>({static_cast<int32_t>(network::PostgresValueType::INTEGER)}));
    io_socket->FlushAllWrites();
    network::ManualPacketUtil::ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_ERROR_RESPONSE);
  }

  std::string portal_name = "test_portal";
  auto param1 = std::vector<char>({'1', '0', '0'});

  {
    // Binding a statement that doesn't exist
    writer.WriteBindCommand(portal_name, "FakeStatementName", {}, {&param1}, {});
    io_socket->FlushAllWrites();
    network::ManualPacketUtil::ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_ERROR_RESPONSE);
  }

  {
    // Wrong number of format codes
    writer.WriteBindCommand(portal_name, stmt_name, {0, 0, 0, 0, 0}, {&param1}, {});
    io_socket->FlushAllWrites();
    network::ManualPacketUtil::ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_ERROR_RESPONSE);
  }

  {
    // Wrong number of parameters
    auto param2 = std::vector<char>({'f', 'a', 'k', 'e'});
    writer.WriteBindCommand(portal_name, stmt_name, {}, {&param1, &param2}, {});
    io_socket->FlushAllWrites();
    network::ManualPacketUtil::ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_ERROR_RESPONSE);
  }

  writer.WriteBindCommand(portal_name, stmt_name, {}, {&param1}, {});
  io_socket->FlushAllWrites();
  network::ManualPacketUtil::ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_BIND_COMPLETE);

  {
    // Describe a statement and a portal that doesn't exist
    writer.WriteDescribeCommand(network::DescribeCommandObjectType::STATEMENT, "FakeStatementName");
    io_socket->FlushAllWrites();
    network::ManualPacketUtil::ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_ERROR_RESPONSE);

    writer.WriteDescribeCommand(network::DescribeCommandObjectType::PORTAL, "FakePortalName");
    io_socket->FlushAllWrites();
    network::ManualPacketUtil::ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_ERROR_RESPONSE);
  }

  {
    // Execute a portal that doesn't exist
    writer.WriteExecuteCommand("FakePortal", 0);
    io_socket->FlushAllWrites();
    network::ManualPacketUtil::ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_ERROR_RESPONSE);
  }

  {
    // A bad describe request
    writer.BeginPacket(network::NetworkMessageType::PG_DESCRIBE_COMMAND)
        .AppendRawValue('?')
        .AppendString(stmt_name)
        .EndPacket();
    io_socket->FlushAllWrites();
    network::ManualPacketUtil::ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_ERROR_RESPONSE);
  }

  network::ManualPacketUtil::TerminateConnection(io_socket->GetSocketFd());
  io_socket->Close();
}

}  // namespace terrier::trafficcop
