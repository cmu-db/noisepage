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

namespace terrier::settings{

class SettingsTests : public TerrierTest {

 protected:
  std::shared_ptr<SettingsManager> settings_manager;

  void SetUp() override {
    TerrierTest::SetUp();

    storage::RecordBufferSegmentPool buffer_pool_(100000, 10000);
    transaction::TransactionManager txn_manager_(&buffer_pool_, true, nullptr);
    catalog::terrier_catalog = std::make_shared<terrier::catalog::Catalog>(&txn_manager_);
    settings_manager = std::make_shared<SettingsManager>(terrier::catalog::terrier_catalog, &txn_manager_);
  }

};

// NOLINTNEXTLINE
TEST_F(SettingsTests, PortTest) {

  network::TerrierServer server;
  auto port = static_cast<uint16_t>(settings_manager->GetInt(Param::port));
  std::thread server_thread;

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
  TEST_LOG_DEBUG("[PortTest] Client has closed");

  server.Close();
  server_thread.join();
  TEST_LOG_DEBUG("Terrier has shut down");
}


} //

