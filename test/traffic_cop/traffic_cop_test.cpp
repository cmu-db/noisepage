#include <pqxx/pqxx> /* libpqxx is used to instantiate C++ client */
#include "common/settings.h"
#include "gtest/gtest.h"
#include "util/test_harness.h"
#include "traffic_cop/traffic_cop.h"
#include "network/terrier_server.h"
#include "network/connection_handle_factory.h"
#include "loggers/main_logger.h"
#include "network/network_defs.h"

namespace terrier{
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

    // Setup Traffic Cop
    network::TrafficCopPtr t_cop(new TrafficCop());
    network::ConnectionHandleFactory::GetInstance().SetTrafficCop(t_cop);
  }

  void StopServer()
  {
    server.Close();
    server_thread.join();
    TEST_LOG_DEBUG("Terrier has shut down");

    TerrierTest::TearDown();
  }
};

static int PrintRows(void *callback_param, int argc, char **values, char **col_name){
  int i;
  if(callback_param != nullptr)
    printf("callback param: %s", (const char*)callback_param);
  for(i=0; i<argc; i++){
    printf("%s = %s\n", col_name[i], values[i] ? values[i] : "NULL");
  }
  printf("\n");
  return 0;
}


//NOLINTNEXTLINE
TEST_F(TrafficCopTests, FirstTest) {
  TrafficCop traffic_cop;

  traffic_cop.ExecuteQuery("DROP TABLE IF EXISTS TableA", PrintRows);
  traffic_cop.ExecuteQuery("CREATE TABLE TableA (id INT PRIMARY KEY, data TEXT);", PrintRows);
  traffic_cop.ExecuteQuery("INSERT INTO TableA VALUES (1, 'abc');", PrintRows);
  traffic_cop.ExecuteQuery("INSERT INTO TableA VALUES (2, 'def');", PrintRows);
  traffic_cop.ExecuteQuery("SELECT * FROM TableA", PrintRows);

}

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
    txn1.commit();
  } catch (const std::exception &e) {
    TEST_LOG_ERROR("[SimpleQueryTest] Exception occurred: {0}", e.what());
    EXPECT_TRUE(false);
  }

  StopServer();

}

} // namespace terrier
