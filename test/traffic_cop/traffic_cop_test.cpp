#include <pqxx/pqxx> /* libpqxx is used to instantiate C++ client */

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/settings.h"
#include "gtest/gtest.h"
#include "loggers/main_logger.h"
#include "network/connection_handle_factory.h"
#include "network/terrier_server.h"
#include "storage/garbage_collector.h"
#include "test_util/manual_packet_util.h"
#include "test_util/test_harness.h"
#include "traffic_cop/traffic_cop.h"
#include "traffic_cop/traffic_cop_defs.h"
#include "transaction/deferred_action_manager.h"
#include "transaction/transaction_manager.h"

namespace terrier {
class TrafficCopTests : public TerrierTest {
 protected:
  std::unique_ptr<network::TerrierServer> server_;
  uint16_t port_ = common::Settings::SERVER_PORT;
  std::thread server_thread_;

  trafficcop::TrafficCop *tcop_;
  catalog::Catalog *catalog_;
  storage::RecordBufferSegmentPool buffer_pool_{100, 100};
  storage::BlockStore block_store_{100, 100};
  transaction::TimestampManager *timestamp_manager_;
  transaction::DeferredActionManager *deferred_action_manager_;
  transaction::TransactionManager *txn_manager_;

  storage::GarbageCollector *gc_;
  network::PostgresCommandFactory command_factory_;
  network::PostgresProtocolInterpreter::Provider interpreter_provider_{common::ManagedPointer(&command_factory_)};
  std::unique_ptr<network::ConnectionHandleFactory> handle_factory_;
  common::DedicatedThreadRegistry thread_registry_ = common::DedicatedThreadRegistry(DISABLED);

  void SetUp() override {
    TerrierTest::SetUp();

    timestamp_manager_ = new transaction::TimestampManager;
    deferred_action_manager_ = new transaction::DeferredActionManager(timestamp_manager_);
    txn_manager_ = new transaction::TransactionManager(timestamp_manager_, deferred_action_manager_, &buffer_pool_,
                                                       true, DISABLED);
    gc_ = new storage::GarbageCollector(timestamp_manager_, deferred_action_manager_, txn_manager_, nullptr);

    catalog_ = new catalog::Catalog(txn_manager_, &block_store_);

    tcop_ = new trafficcop::TrafficCop(common::ManagedPointer(txn_manager_), common::ManagedPointer(catalog_));

    auto txn = txn_manager_->BeginTransaction();
    catalog_->CreateDatabase(txn, catalog::DEFAULT_DATABASE, true);
    txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

    // Run the GC to flush it down to a clean system
    gc_->PerformGarbageCollection();
    gc_->PerformGarbageCollection();

    network::network_logger->set_level(spdlog::level::trace);
    test_logger->set_level(spdlog::level::debug);
    spdlog::flush_every(std::chrono::seconds(1));

    try {
      handle_factory_ = std::make_unique<network::ConnectionHandleFactory>(common::ManagedPointer(tcop_));
      server_ = std::make_unique<network::TerrierServer>(
          common::ManagedPointer<network::ProtocolInterpreter::Provider>(&interpreter_provider_),
          common::ManagedPointer(handle_factory_.get()),
          common::ManagedPointer<common::DedicatedThreadRegistry>(&thread_registry_));
      server_->SetPort(port_);
      server_->RunServer();
    } catch (NetworkProcessException &exception) {
      TEST_LOG_ERROR("[LaunchServer] exception when launching server");
      throw;
    }
    TEST_LOG_DEBUG("Server initialized");
  }

  void TearDown() override {
    server_->StopServer();
    TEST_LOG_DEBUG("Terrier has shut down");

    auto txn = txn_manager_->BeginTransaction();
    auto db_accessor = catalog_->GetAccessor(txn, catalog_->GetDatabaseOid(txn, catalog::DEFAULT_DATABASE));
    EXPECT_FALSE(db_accessor->DropNamespace(catalog::namespace_oid_t{catalog::START_OID}));
    txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

    catalog_->TearDown();
    // Run the GC to clean up transactions
    gc_->PerformGarbageCollection();
    gc_->PerformGarbageCollection();
    gc_->PerformGarbageCollection();

    delete tcop_;
    delete catalog_;
    delete gc_;
    delete txn_manager_;
    delete deferred_action_manager_;
    delete timestamp_manager_;
    TerrierTest::TearDown();
  }

  // The port used to connect a Postgres backend. Useful for debugging.
  const int postgres_port_ = 5432;

  /**
   * Read packet from the server (without parsing) until receiving ReadyForQuery or the connection is closed.
   * @param io_socket
   * @param expected_msg_type
   * @return true if reads the expected type message, false for closed.
   */
  bool ReadUntilMessageOrClose(const std::shared_ptr<network::NetworkIoWrapper> &io_socket,
                               const network::NetworkMessageType &expected_msg_type) {
    while (true) {
      io_socket->GetReadBuffer()->Reset();
      network::Transition trans = io_socket->FillReadBuffer();
      if (trans == network::Transition::TERMINATE) return false;

      while (io_socket->GetReadBuffer()->HasMore()) {
        auto type = io_socket->GetReadBuffer()->ReadValue<network::NetworkMessageType>();
        auto size = io_socket->GetReadBuffer()->ReadValue<int32_t>();
        if (size >= 4) io_socket->GetReadBuffer()->Skip(static_cast<size_t>(size - 4));

        if (type == expected_msg_type) return true;
      }
    }
  }

  /**
   * A wrapper for ReadUntilMessageOrClose since most of the times people expect PG_READY_FOR_QUERY.
   * @param io_socket
   * @return
   */
  bool ReadUntilReadyOrClose(const std::shared_ptr<network::NetworkIoWrapper> &io_socket) {
    return ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_READY_FOR_QUERY);
  }

  std::shared_ptr<network::NetworkIoWrapper> StartConnection(uint16_t port) {
    // Manually open a socket
    int socket_fd = socket(AF_INET, SOCK_STREAM, 0);

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    serv_addr.sin_port = htons(port);

    int64_t ret = connect(socket_fd, reinterpret_cast<sockaddr *>(&serv_addr), sizeof(serv_addr));
    if (ret < 0) TEST_LOG_ERROR("Connection Error");

    auto io_socket = std::make_shared<network::NetworkIoWrapper>(socket_fd);
    network::PostgresPacketWriter writer(io_socket->GetWriteQueue());

    std::unordered_map<std::string, std::string> params{
        {"user", catalog::DEFAULT_DATABASE}, {"database", catalog::DEFAULT_DATABASE}, {"application_name", "psql"}};

    writer.WriteStartupRequest(params);
    io_socket->FlushAllWrites();

    ReadUntilReadyOrClose(io_socket);
    return io_socket;
  }

/*
 * Read and write buffer size for the test
 */
#define TEST_BUF_SIZE 1000

  void TerminateConnection(int socket_fd) {
    char out_buffer[TEST_BUF_SIZE] = {};
    // Build a correct query message, "SELECT A FROM B"
    memset(out_buffer, 0, sizeof(out_buffer));
    out_buffer[0] = 'X';
    int len = sizeof(int32_t) + sizeof(char);
    reinterpret_cast<int32_t *>(out_buffer + 1)[0] = htonl(len);
    write(socket_fd, nullptr, len + 1);
  }
};

// NOLINTNEXTLINE
TEST_F(TrafficCopTests, RoundTripTest) {
  try {
    pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
                                            port_, catalog::DEFAULT_DATABASE));

    pqxx::work txn1(connection);
    txn1.exec("DROP TABLE IF EXISTS TableA");
    txn1.exec("CREATE TABLE TableA (id INT PRIMARY KEY, data TEXT);");
    txn1.exec("INSERT INTO TableA VALUES (1, 'abc');");

    pqxx::result r = txn1.exec("SELECT * FROM TableA");
    for (const pqxx::row &row : r) {
      std::string row_str;
      for (const pqxx::field &col : row) {
        row_str += col.c_str();
        row_str += '\t';
      }
      TEST_LOG_INFO(row_str);
    }
    txn1.commit();
    connection.disconnect();
    std::this_thread::sleep_for(std::chrono::seconds(5));
    EXPECT_EQ(r.size(), 1);
  } catch (const std::exception &e) {
    TEST_LOG_ERROR("Exception occurred: {0}", e.what());
    EXPECT_TRUE(false);
  }
}

// NOLINTNEXTLINE
TEST_F(TrafficCopTests, ManualExtendedQueryTest) {
  try {
    auto io_socket = network::ManualPacketUtil::StartConnection(port_);
    network::PostgresPacketWriter writer(io_socket->GetWriteQueue());

    writer.WriteSimpleQuery("DROP TABLE IF EXISTS TableA");
    io_socket->FlushAllWrites();
    ReadUntilReadyOrClose(io_socket);

    writer.WriteSimpleQuery(
        "CREATE TABLE TableA (a_int INT PRIMARY KEY, a_dec DECIMAL, a_text TEXT, a_time TIMESTAMP, a_bigint BIGINT);");
    io_socket->FlushAllWrites();
    ReadUntilReadyOrClose(io_socket);

    writer.WriteSimpleQuery("INSERT INTO TableA VALUES(100, 3.14, 'nico', 114514, 1234)");
    io_socket->FlushAllWrites();
    ReadUntilReadyOrClose(io_socket);

    std::string stmt_name = "begin_statement";
    std::string query = "BEGIN";

    writer.WriteParseCommand(stmt_name, query, std::vector<int>());
    io_socket->FlushAllWrites();
    ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_PARSE_COMPLETE);

    {
      std::string portal_name = "test_portal";
      // Use text format, don't care about result column formats
      writer.WriteBindCommand(portal_name, stmt_name, {}, {}, {});
      io_socket->FlushAllWrites();
      ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_BIND_COMPLETE);

      writer.WriteExecuteCommand(portal_name, 0);
      io_socket->FlushAllWrites();
      ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_COMMAND_COMPLETE);

      writer.WriteSyncCommand();
      io_socket->FlushAllWrites();
      ReadUntilReadyOrClose(io_socket);
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
    ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_PARSE_COMPLETE);

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
      ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_BIND_COMPLETE);

      writer.WriteDescribeCommand(network::DescribeCommandObjectType::STATEMENT, stmt_name);
      io_socket->FlushAllWrites();
      ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_PARAMETER_DESCRIPTION);

      writer.WriteDescribeCommand(network::DescribeCommandObjectType::PORTAL, portal_name);
      io_socket->FlushAllWrites();
      ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_ROW_DESCRIPTION);

      writer.WriteExecuteCommand(portal_name, 0);
      io_socket->FlushAllWrites();
      ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_DATA_ROW);

      writer.WriteSyncCommand();
      io_socket->FlushAllWrites();
      ReadUntilReadyOrClose(io_socket);
    }

    {
      // Test single specifier for all paramters (1)
      // TODO(Weichen): Test binary format here

      std::string portal_name = "test_portal-2";
      // Use text format, don't care about result column formats, specify "0" for using text for all params
      writer.WriteBindCommand(portal_name, stmt_name, {0}, {&param1, &param2, &param3, &param4, &param5}, {});
      io_socket->FlushAllWrites();
      ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_BIND_COMPLETE);

      writer.WriteDescribeCommand(network::DescribeCommandObjectType::PORTAL, portal_name);
      io_socket->FlushAllWrites();
      ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_ROW_DESCRIPTION);

      writer.WriteExecuteCommand(portal_name, 0);
      io_socket->FlushAllWrites();
      ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_DATA_ROW);

      writer.WriteSyncCommand();
      io_socket->FlushAllWrites();
      ReadUntilReadyOrClose(io_socket);
    }

    {
      // Test individual specifier for each parameter

      std::string portal_name = "test_portal-3";
      // Use text format, don't care about result column formats
      writer.WriteBindCommand(portal_name, stmt_name, {0, 0, 0, 0, 0}, {&param1, &param2, &param3, &param4, &param5},
                              {});
      io_socket->FlushAllWrites();
      ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_BIND_COMPLETE);

      writer.WriteDescribeCommand(network::DescribeCommandObjectType::PORTAL, portal_name);
      io_socket->FlushAllWrites();
      ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_ROW_DESCRIPTION);

      writer.WriteExecuteCommand(portal_name, 0);
      io_socket->FlushAllWrites();
      ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_DATA_ROW);

      writer.WriteSyncCommand();
      io_socket->FlushAllWrites();
      ReadUntilReadyOrClose(io_socket);
    }

    // CloseCommand
    writer.WriteCloseCommand(network::DescribeCommandObjectType::STATEMENT, stmt_name);
    io_socket->FlushAllWrites();

    stmt_name = "commit_statement";
    query = "COMMIT";

    writer.WriteParseCommand(stmt_name, query, std::vector<int>());
    io_socket->FlushAllWrites();
    ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_PARSE_COMPLETE);

    {
      std::string portal_name = "test_portal";
      // Use text format, don't care about result column formats
      writer.WriteBindCommand(portal_name, stmt_name, {}, {}, {});
      io_socket->FlushAllWrites();
      ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_BIND_COMPLETE);

      writer.WriteExecuteCommand(portal_name, 0);
      io_socket->FlushAllWrites();

      writer.WriteSyncCommand();
      io_socket->FlushAllWrites();
      ReadUntilReadyOrClose(io_socket);
    }

    TerminateConnection(io_socket->GetSocketFd());
    io_socket->Close();
  } catch (const std::exception &e) {
    TEST_LOG_ERROR("Exception occurred: {0}", e.what());
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

    auto txn = txn_manager_->BeginTransaction();
    auto db_accessor = catalog_->GetAccessor(txn, catalog_->GetDatabaseOid(txn, catalog::DEFAULT_DATABASE));

    // Create a new namespace and make sure that its OID is higher than the default start OID,
    // which should have been assigned to the temporary namespace for this connection
    auto new_namespace_oid = db_accessor->CreateNamespace(std::string(trafficcop::TEMP_NAMESPACE_PREFIX));
    EXPECT_GT(static_cast<uint32_t>(new_namespace_oid), catalog::START_OID);
    txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

    txn1.commit();
    connection.disconnect();
    std::this_thread::sleep_for(std::chrono::seconds(5));
  } catch (const std::exception &e) {
    TEST_LOG_ERROR("Exception occurred: {0}", e.what());
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
TEST_F(TrafficCopTests, ManualRoundTripTest) {
  try {
    auto io_socket = StartConnection(port_);
    network::PostgresPacketWriter writer(io_socket->GetWriteQueue());

    writer.WriteSimpleQuery("DROP TABLE IF EXISTS TableA");
    io_socket->FlushAllWrites();
    ReadUntilReadyOrClose(io_socket);
    writer.WriteSimpleQuery("CREATE TABLE TableA (id INT PRIMARY KEY, data TEXT);");
    io_socket->FlushAllWrites();
    ReadUntilReadyOrClose(io_socket);
    writer.WriteSimpleQuery("INSERT INTO TableA VALUES (1, 'abc');");
    io_socket->FlushAllWrites();
    ReadUntilReadyOrClose(io_socket);

    writer.WriteSimpleQuery("SELECT * FROM TableA");
    io_socket->FlushAllWrites();
    ReadUntilReadyOrClose(io_socket);
    TerminateConnection(io_socket->GetSocketFd());
    io_socket->Close();
  } catch (const std::exception &e) {
    TEST_LOG_ERROR("Exception occurred: {0}", e.what());
    EXPECT_TRUE(false);
  }
}

// NOLINTNEXTLINE
TEST_F(TrafficCopTests, ErrorHandlingTest) {
  auto io_socket = StartConnection(port_);
  network::PostgresPacketWriter writer(io_socket->GetWriteQueue());

  writer.WriteSimpleQuery("DROP TABLE IF EXISTS TableA");
  io_socket->FlushAllWrites();
  ReadUntilReadyOrClose(io_socket);
  writer.WriteSimpleQuery("CREATE TABLE TableA (id INT PRIMARY KEY, data TEXT);");
  io_socket->FlushAllWrites();
  ReadUntilReadyOrClose(io_socket);
  writer.WriteSimpleQuery("INSERT INTO TableA VALUES (1, 'abc');");
  io_socket->FlushAllWrites();
  ReadUntilReadyOrClose(io_socket);

  std::string stmt_name = "test_statement";
  std::string query = "SELECT * FROM TableA WHERE id = $1";
  writer.WriteParseCommand(stmt_name, query,
                           std::vector<int>({static_cast<int32_t>(network::PostgresValueType::INTEGER)}));
  io_socket->FlushAllWrites();
  ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_PARSE_COMPLETE);

  {
    // Repeated statement name
    writer.WriteParseCommand(stmt_name, query,
                             std::vector<int>({static_cast<int32_t>(network::PostgresValueType::INTEGER)}));
    io_socket->FlushAllWrites();
    ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_ERROR_RESPONSE);
  }

  std::string portal_name = "test_portal";
  auto param1 = std::vector<char>({'1', '0', '0'});

  {
    // Binding a statement that doesn't exist
    writer.WriteBindCommand(portal_name, "FakeStatementName", {}, {&param1}, {});
    io_socket->FlushAllWrites();
    ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_ERROR_RESPONSE);
  }

  {
    // Wrong number of format codes
    writer.WriteBindCommand(portal_name, stmt_name, {0, 0, 0, 0, 0}, {&param1}, {});
    io_socket->FlushAllWrites();
    ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_ERROR_RESPONSE);
  }

  {
    // Wrong number of parameters
    auto param2 = std::vector<char>({'f', 'a', 'k', 'e'});
    writer.WriteBindCommand(portal_name, stmt_name, {}, {&param1, &param2}, {});
    io_socket->FlushAllWrites();
    ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_ERROR_RESPONSE);
  }

  writer.WriteBindCommand(portal_name, stmt_name, {}, {&param1}, {});
  io_socket->FlushAllWrites();
  ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_BIND_COMPLETE);

  {
    // Describe a statement and a portal that doesn't exist
    writer.WriteDescribeCommand(network::DescribeCommandObjectType::STATEMENT, "FakeStatementName");
    io_socket->FlushAllWrites();
    ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_ERROR_RESPONSE);

    writer.WriteDescribeCommand(network::DescribeCommandObjectType::PORTAL, "FakePortalName");
    io_socket->FlushAllWrites();
    ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_ERROR_RESPONSE);
  }

  {
    // Execute a portal that doesn't exist
    writer.WriteExecuteCommand("FakePortal", 0);
    io_socket->FlushAllWrites();
    ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_ERROR_RESPONSE);
  }

  {
    // A bad describe request
    writer.BeginPacket(network::NetworkMessageType::PG_DESCRIBE_COMMAND)
        .AppendRawValue('?')
        .AppendString(stmt_name)
        .EndPacket();
    io_socket->FlushAllWrites();
    ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_ERROR_RESPONSE);
  }

  TerminateConnection(io_socket->GetSocketFd());
  io_socket->Close();
}

/**
 * I disabled this test because pqxx sends PARSE query with num_params=0, but we are requiring the client to specify
 * all param types in the PARSE query.
 * Please use manual tests before this is supported.
 */
// NOLINTNEXTLINE
TEST_F(TrafficCopTests, DISABLED_ExtendedQueryTest) {
  try {
    pqxx::connection connection(
        fmt::format("host=127.0.0.1 port={0} user=postgres sslmode=disable application_name=psql", port_));

    pqxx::work txn(connection);
    pqxx::result res;
    connection.prepare("DROP TABLE IF EXISTS TableA");
    res = txn.exec_prepared("");
    connection.prepare("CREATE TABLE TableA (a_int INT PRIMARY KEY)");
    res = txn.exec_prepared("");

    connection.prepare("INSERT INTO TableA VALUES(114)");
    res = txn.exec_prepared("");

    connection.prepare("SELECT * from TableA where a_int = $1");
    res = txn.exec_prepared("", 114);
    EXPECT_EQ(1, res.size());

    txn.commit();

    /*
    connection.prepare("SELECT * from TableA where a_dec = $1");
    res = txn.exec_prepared("", 1919.81);
    EXPECT_EQ(1, res.size());

    connection.prepare("SELECT * from TableA where a_text = $1");
    res = txn.exec_prepared("", "blacktea");
    EXPECT_EQ(1, res.size());
    */
  } catch (const std::exception &e) {
    TEST_LOG_ERROR("Exception occurred: {0}", e.what());
    EXPECT_TRUE(false);
  }
}

}  // namespace terrier
