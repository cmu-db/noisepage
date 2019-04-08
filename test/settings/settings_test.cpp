#include <util/test_harness.h>
#include "gtest/gtest.h"

#include <cstdio>
#include <cstring>
#include <memory>
#include <pqxx/pqxx>
#include <string>
#include <unordered_map>
#include <vector>
#include "gtest/gtest.h"
#include "loggers/main_logger.h"
#include "network/connection_handle_factory.h"
#include "settings/settings_manager.h"

namespace terrier::settings {

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
TEST_F(SettingsTests, BasicTest) {
  auto port = static_cast<uint16_t>(settings_manager->GetInt(Param::port));
  EXPECT_EQ(port, 15721);

  EXPECT_THROW(settings_manager->SetInt(Param::port, 23333), SettingsException);
}

}  // namespace terrier::settings
