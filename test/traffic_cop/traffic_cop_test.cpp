#include <pqxx/pqxx> /* libpqxx is used to instantiate C++ client */
#include "common/settings.h"
#include "gtest/gtest.h"
#include "util/test_harness.h"
#include "traffic_cop/traffic_cop.h"
#include "network/terrier_server.h"
#include "network/connection_handle_factory.h"
#include "loggers/main_logger.h"
#include "network/network_defs.h"
#include "network/postgres_protocol_utils.h"
#include "network/network_io_utils.h"
#include "util/manual_packet_helpers.h"


namespace terrier::traffic_cop{
class TrafficCopTests : public TerrierTest
{
 protected:
  network::TerrierServer server;
  uint16_t port = common::Settings::SERVER_PORT;
  std::thread server_thread;

  void StartServer()
  {
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
  }

  void StopServer()
  {
    server.Close();
    server_thread.join();
    TEST_LOG_DEBUG("Terrier has shut down");

    TerrierTest::TearDown();
  }
};

//NOLINTNEXTLINE
TEST_F(TrafficCopTests, RoundTripTest)
{
  StartServer();
  try {
    pqxx::connection connection(
        fmt::format("host=127.0.0.1 port={0} user=postgres sslmode=disable application_name=psql", port));

    pqxx::work txn1(connection);
    txn1.exec("DROP TABLE IF EXISTS TableA");
    txn1.exec("CREATE TABLE TableA (id INT PRIMARY KEY, data TEXT);");
    txn1.exec("INSERT INTO TableA VALUES (1, 'abc');");

    pqxx::result R = txn1.exec("SELECT * FROM TableA");
    for(const pqxx::row &row : R)
    {
      for(const auto &col : row)
      {
        std::cout<<col<<'\t';
      }
      std::cout<<std::endl;
    }
    txn1.commit();
  } catch (const std::exception &e) {
    TEST_LOG_ERROR("Exception occurred: {0}", e.what());
    EXPECT_TRUE(false);
  }

  StopServer();

}

/*
 * This test simply sends and receives the packet manually.
 * It is used to debug the tests (you can view the packet content). Can be disabled in actual testing.
 *
 * */
//NOLINTNEXTLINE
TEST_F(TrafficCopTests, ManualRoundTripTest)
{
  StartServer();
  try {
    // Manually open a socket
    int socket_fd = socket(AF_INET, SOCK_STREAM, 0);

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    serv_addr.sin_port = htons(5432);

    int64_t ret = connect(socket_fd, reinterpret_cast<sockaddr *>(&serv_addr), sizeof(serv_addr));
    if (ret < 0) TEST_LOG_ERROR("Connection Error");

    auto io_socket = std::make_shared<network::PosixSocketIoWrapper>(socket_fd);
    network::PostgresPacketWriter writer(io_socket->out_);

    std::unordered_map<std::string, std::string> params{
        {"user", "postgres"}, {"database", "postgres"}, {"application_name", "psql"}};

    writer.WriteStartupRequest(params);
    io_socket->FlushAllWrites();
    ReadUntilReadyOrClose(io_socket);

    writer.WriteQuery("DROP TABLE IF EXISTS TableA");
    io_socket->FlushAllWrites();
    ReadUntilReadyOrClose(io_socket);
    writer.WriteQuery("CREATE TABLE TableA (id INT PRIMARY KEY, data TEXT);");
    io_socket->FlushAllWrites();
    ReadUntilReadyOrClose(io_socket);
    writer.WriteQuery("INSERT INTO TableA VALUES (1, 'abc');");
    io_socket->FlushAllWrites();
    ReadUntilReadyOrClose(io_socket);

    writer.WriteQuery("SELECT * FROM TableA");
    io_socket->FlushAllWrites();
    ReadUntilReadyOrClose(io_socket);
  } catch (const std::exception &e) {
    TEST_LOG_ERROR("Exception occurred: {0}", e.what());
    EXPECT_TRUE(false);
  }

  StopServer();

}

} // namespace terrier
