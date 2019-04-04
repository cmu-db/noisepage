#include <pqxx/pqxx> /* libpqxx is used to instantiate C++ client */
#include <string>
#include <vector>

#include "common/settings.h"
#include "gtest/gtest.h"
#include "loggers/main_logger.h"
#include "network/connection_handle_factory.h"
#include "network/network_defs.h"
#include "network/network_io_utils.h"
#include "network/postgres_protocol_utils.h"
#include "network/terrier_server.h"
#include "traffic_cop/traffic_cop.h"
#include "util/manual_packet_helpers.h"
#include "util/test_harness.h"

namespace terrier::traffic_cop {
class TrafficCopTests : public TerrierTest {
 protected:
  network::TerrierServer server;
  uint16_t port = common::Settings::SERVER_PORT;
  std::thread server_thread;

  void StartServer() {
    test_logger->set_level(spdlog::level::debug);
    spdlog::flush_every(std::chrono::seconds(1));

    try {
      server.SetPort(port);
      server.SetupServer();
    } catch (NetworkProcessException &exception) {
      TEST_LOG_ERROR("[LaunchServer] exception when launching server");
      throw;
    }
    TEST_LOG_DEBUG("Server initialized");
    server_thread = std::thread([&]() { server.ServerLoop(); });

    // Setup Traffic Cop
    network::TrafficCopPtr t_cop(new TrafficCop());
    network::ConnectionHandleFactory::GetInstance().SetTrafficCop(t_cop);
  }

  void StopServer() {
    server.Close();
    server_thread.join();
    TEST_LOG_DEBUG("Terrier has shut down");

    TerrierTest::TearDown();
  }
};

// NOLINTNEXTLINE
TEST_F(TrafficCopTests, RoundTripTest) {
  StartServer();
  try {
    pqxx::connection connection(
        fmt::format("host=127.0.0.1 port={0} user=postgres sslmode=disable application_name=psql", port));

    pqxx::work txn1(connection);
    txn1.exec("DROP TABLE IF EXISTS TableA");
    txn1.exec("CREATE TABLE TableA (id INT PRIMARY KEY, data TEXT);");
    txn1.exec("INSERT INTO TableA VALUES (1, 'abc');");

    pqxx::result R = txn1.exec("SELECT * FROM TableA");
    for (const pqxx::row &row : R) {
      std::string row_str;
      for (const pqxx::field &col : row) {
        row_str += col.c_str();
        row_str += '\t';
      }
      TEST_LOG_INFO(row_str);
    }
    txn1.commit();

    EXPECT_EQ(R.size(), 1);
  } catch (const std::exception &e) {
    TEST_LOG_ERROR("Exception occurred: {0}", e.what());
    EXPECT_TRUE(false);
  }

  StopServer();
}

// NOLINTNEXTLINE
TEST_F(TrafficCopTests, SimpleExtendedQueryTest) {
  StartServer();
  try {
    auto io_socket = network::StartConnection(port);
    network::PostgresPacketWriter writer(io_socket->out_);

    writer.WriteSimpleQuery("DROP TABLE IF EXISTS TableA");
    io_socket->FlushAllWrites();
    ReadUntilReadyOrClose(io_socket);

    writer.WriteSimpleQuery("CREATE TABLE TableA (a_int INT PRIMARY KEY, a_dec DECIMAL, a_text TEXT);");
    io_socket->FlushAllWrites();
    ReadUntilReadyOrClose(io_socket);

    writer.WriteSimpleQuery("INSERT INTO TableA VALUES(100, 3.14159, 'niconiconi')");
    io_socket->FlushAllWrites();
    ReadUntilReadyOrClose(io_socket);

    std::string stmt_name = "test_statement";
    std::string query = "SELECT * from TableA where a_int = ?1";

    writer.WriteParseCommand(stmt_name, query,
                             std::vector<int>(1, static_cast<int32_t>(network::PostgresValueType::INTEGER)));
    io_socket->FlushAllWrites();

    ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PARSE_COMPLETE);

    // Bind
    auto param1 = std::vector<char>({'1', '0', '0'});

    std::string portal_name = "test_portal";
    // Use text format Don't care about result column formats
    writer.WriteBindCommand(portal_name, stmt_name, {}, {&param1}, {});

    io_socket->FlushAllWrites();

    ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::BIND_COMPLETE);

    writer.WriteExecuteCommand(portal_name, 0);
    io_socket->FlushAllWrites();

    ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::DATA_ROW);
  } catch (const std::exception &e) {
    TEST_LOG_ERROR("Exception occurred: {0}", e.what());
    EXPECT_TRUE(false);
  }

  StopServer();
}

/*
 * This test is for debugging tests. It can be disabled when testing other components.
 * You can launch a Postgres backend and compare packets from terrier and from Postgres
 * to find if you have created correct packets.
 *
 * */
// NOLINTNEXTLINE
TEST_F(TrafficCopTests, ManualRoundTripTest) {
  StartServer();
  try {
    auto io_socket = network::StartConnection(port);
    network::PostgresPacketWriter writer(io_socket->out_);

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
  } catch (const std::exception &e) {
    TEST_LOG_ERROR("Exception occurred: {0}", e.what());
    EXPECT_TRUE(false);
  }

  StopServer();
}

}  // namespace terrier::traffic_cop
