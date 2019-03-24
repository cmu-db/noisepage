#include <util/test_harness.h>
#include "gtest/gtest.h"

#include <cstdio>
#include <cstring>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <pqxx/pqxx>
#include "settings/settings_manager.h"
#include "gtest/gtest.h"
#include "loggers/main_logger.h"
#include "network/connection_handle_factory.h"

namespace terrier{

class SettingsTests : public TerrierTest {

 protected:
  network::TerrierServer server;
  uint16_t port = static_cast<uint16_t>(settings::SettingsManager::GetSmallInt(settings::Param::port));
  std::thread server_thread;

  /**
   * Initialization
   */
  void SetUp() override {
    TerrierTest::SetUp();

    network::network_logger->set_level(spdlog::level::debug);
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

  void TearDown() override {
    server.Close();
    server_thread.join();
    TEST_LOG_DEBUG("Terrier has shut down");

    TerrierTest::TearDown();
  }
};

// NOLINTNEXTLINE
TEST_F(SettingsTests, SimpleQueryTest) {
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


} //

