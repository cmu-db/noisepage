#include <sys/socket.h>
#include <sys/types.h>
#include <util/test_harness.h>
#include <pqxx/pqxx> /* libpqxx is used to instantiate C++ client */

#include <cstdio>
#include <cstring>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include "common/settings.h"
#include "gtest/gtest.h"
#include "loggers/main_logger.h"
#include "network/connection_handle_factory.h"
#include "traffic_cop/result_set.h"
#include "traffic_cop/traffic_cop.h"
#include "util/manual_packet_helpers.h"

namespace terrier::network {

/*
 * The network tests does not check whether the result is correct. It only checks if the network layer works.
 * So, in network tests, we use a fake command factory to return empty results for every query.
 */
class FakeCommandFactory : public CommandFactory
{
  std::shared_ptr<PostgresNetworkCommand> PacketToCommand(PostgresInputPacket *packet) override {
    return std::static_pointer_cast<PostgresNetworkCommand, EmptyCommand>(std::make_shared<EmptyCommand>(packet));
  }
};

class NetworkTests : public TerrierTest {
 protected:
  TerrierServer server;
  uint16_t port = common::Settings::SERVER_PORT;
  std::thread server_thread;
  TrafficCop t_cop;
  FakeCommandFactory fake_command_factory;

  /**
   * Initialization
   */
  void SetUp() override {
    TerrierTest::SetUp();

    network_logger->set_level(spdlog::level::trace);
    spdlog::flush_every(std::chrono::seconds(1));

    try {
      server.SetPort(port);
      server.SetupServer();
    } catch (NetworkProcessException &exception) {
      TEST_LOG_ERROR("[LaunchServer] exception when launching server");
      throw;
    }

    ConnectionHandleFactory::GetInstance().SetConnectionDependencies(&t_cop, &fake_command_factory);

    TEST_LOG_DEBUG("Server initialized");
    server_thread = std::thread([&]() { server.ServerLoop(); });
  }

  void TearDown() override {
    server.Close();
    server_thread.join();
    TEST_LOG_DEBUG("Terrier has shut down");

    TerrierTest::TearDown();
  }
};

/**
 * Use std::thread to initiate peloton server and pqxx client in separate
 * threads
 * Simple query test to guarantee both sides run correctly
 * Callback method to close server after client finishes
 */
// NOLINTNEXTLINE
TEST_F(NetworkTests, SimpleQueryTest) {
  try {
    pqxx::connection C(
        fmt::format("host=127.0.0.1 port={0} user=postgres sslmode=disable application_name=psql", port));

    pqxx::work txn1(C);
    txn1.exec("INSERT INTO employee VALUES (1, 'Han LI');");
    txn1.exec("INSERT INTO employee VALUES (2, 'Shaokun ZOU');");
    txn1.exec("INSERT INTO employee VALUES (3, 'Yilei CHU');");

    pqxx::result R = txn1.exec("SELECT name FROM employee where id=1;");
    txn1.commit();
    EXPECT_EQ(R.size(), 0);
  } catch (const std::exception &e) {
    TEST_LOG_ERROR("[SimpleQueryTest] Exception occurred: {0}", e.what());
    EXPECT_TRUE(false);
  }
  TEST_LOG_DEBUG("[SimpleQueryTest] Client has closed");
}

// NOLINTNEXTLINE
TEST_F(NetworkTests, BadQueryTest) {
  try {
    TEST_LOG_INFO("[BadQueryTest] Starting, expect errors to be logged");
    std::shared_ptr<NetworkIoWrapper> io_socket = StartConnection(port);
    PostgresPacketWriter writer(io_socket->out_);

    // Build a correct query message, "SELECT A FROM B"
    std::string query = "SELECT A FROM B;";
    writer.WriteSimpleQuery(query);
    io_socket->FlushAllWrites();
    bool is_ready = ReadUntilReadyOrClose(io_socket);
    EXPECT_TRUE(is_ready);  // should be okay

    // Send a bad query packet
    std::string bad_query = "a_random_bad_packet";
    io_socket->out_->Reset();
    io_socket->out_->BufferWriteRaw(bad_query.data(), bad_query.length());
    io_socket->FlushAllWrites();

    is_ready = ReadUntilReadyOrClose(io_socket);
    EXPECT_FALSE(is_ready);
    io_socket->Close();
  } catch (const std::exception &e) {
    TEST_LOG_ERROR("[BadQueryTest] Exception occurred: {0}", e.what());
    EXPECT_TRUE(false);
  }
  TEST_LOG_INFO("[BadQueryTest] Completed");
}

// NOLINTNEXTLINE
TEST_F(NetworkTests, NoSSLTest) {
  try {
    pqxx::connection C(fmt::format("host=127.0.0.1 port={0} user=postgres application_name=psql", port));

    pqxx::work txn1(C);
    txn1.exec("INSERT INTO employee VALUES (1, 'Han LI');");
    txn1.exec("INSERT INTO employee VALUES (2, 'Shaokun ZOU');");
    txn1.exec("INSERT INTO employee VALUES (3, 'Yilei CHU');");
  } catch (const std::exception &e) {
    TEST_LOG_ERROR("[NoSSLTest] Exception occurred: {0}", e.what());
    EXPECT_TRUE(false);
  }
}

void TestExtendedQuery(uint16_t port) {
  std::shared_ptr<NetworkIoWrapper> io_socket = StartConnection(port);
  io_socket->out_->Reset();
  std::string stmt_name = "prepared_test";
  std::string query = "INSERT INTO foo VALUES($1, $2, $3, $4);";

  PostgresPacketWriter writer(io_socket->out_);
  auto type_oid = static_cast<int>(PostgresValueType::INTEGER);
  writer.WriteParseCommand(stmt_name, query, std::vector<int>(4, type_oid));
  io_socket->FlushAllWrites();
  EXPECT_TRUE(ReadUntilReadyOrClose(io_socket));

  std::string portal_name;
  writer.WriteBindCommand(portal_name, stmt_name, {}, {}, {});
  io_socket->FlushAllWrites();
  EXPECT_TRUE(ReadUntilReadyOrClose(io_socket));

  writer.WriteExecuteCommand(portal_name, 0);
  io_socket->FlushAllWrites();
  EXPECT_TRUE(ReadUntilReadyOrClose(io_socket));

  // DescribeCommand
  writer.WriteDescribeCommand(DescribeCommandObjectType::STATEMENT, stmt_name);
  io_socket->FlushAllWrites();
  EXPECT_TRUE(ReadUntilReadyOrClose(io_socket));

  // SyncCommand
  writer.WriteSyncCommand();
  io_socket->FlushAllWrites();
  EXPECT_TRUE(ReadUntilReadyOrClose(io_socket));

  // CloseCommand
  writer.WriteCloseCommand(DescribeCommandObjectType::STATEMENT, stmt_name);
  io_socket->FlushAllWrites();
  EXPECT_TRUE(ReadUntilReadyOrClose(io_socket));

  TerminateConnection(io_socket->sock_fd_);
}

// NOLINTNEXTLINE
TEST_F(NetworkTests, PgNetworkCommandsTest) {
  try {
    TestExtendedQuery(port);
  } catch (const std::exception &e) {
    TEST_LOG_ERROR("[PgNetworkCommandsTest] Exception occurred: {0}", e.what());
    EXPECT_TRUE(false);
  }
}

// NOLINTNEXTLINE
TEST_F(NetworkTests, LargePacketsTest) {
  try {
    pqxx::connection C(
        fmt::format("host=127.0.0.1 port={0} user=postgres sslmode=disable application_name=psql", port));

    pqxx::work txn1(C);
    std::string longQueryPacketString(255555, 'a');
    longQueryPacketString[longQueryPacketString.size() - 1] = '\0';
    txn1.exec(longQueryPacketString);
    txn1.commit();
  } catch (const std::exception &e) {
    TEST_LOG_ERROR("[LargePacketstest] Exception occurred: {0}", e.what());
    EXPECT_TRUE(false);
  }
}

}  // namespace terrier::network
