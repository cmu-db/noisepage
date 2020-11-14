#include <memory>
#include <pqxx/pqxx>  // NOLINT
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/settings.h"
#include "gtest/gtest.h"
#include "main/db_main.h"
#include "test_util/test_harness.h"

namespace noisepage::catalog {

// These are end-to-end tests of the catalog which cannot be performed with the
// JUnit trace framework because we expect divergent behavior from PostgreSQL
// since there are slight differences between the implementations of both
// catalogs (most noticeably OIDs).
class CatalogSqlTests : public TerrierTest {
 protected:
  void SetUp() override {
    std::unordered_map<settings::Param, settings::ParamInfo> param_map;
    noisepage::settings::SettingsManager::ConstructParamMap(param_map);

    db_main_ = noisepage::DBMain::Builder()
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
  }

  std::unique_ptr<DBMain> db_main_;
  uint16_t port_;
};

TEST_F(CatalogSqlTests, OidCheck) {
  try {
    pqxx::connection connection(
        fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql", port_, DEFAULT_DATABASE));

    pqxx::work txn1(connection);
    pqxx::result r = txn1.exec("SELECT reloid FROM pg_class WHERE reloid >= 1000;");
    EXPECT_EQ(r.size(), 0);  // All of the bootstrapped tables should have catalog OIDs
    txn1.commit();
  } catch (const std::exception &e) {
    EXPECT_TRUE(false);
  }
}

TEST_F(CatalogSqlTests, OptimizerHashmapTest) {
  // This directly tests for #1132 because the catalog is the only place hashmaps can exist at the moment.
  try {
    pqxx::connection connection(
        fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql", port_, DEFAULT_DATABASE));

    pqxx::work txn1(connection);
    pqxx::result r = txn1.exec("SELECT relname FROM pg_class WHERE relkind = 114 AND relnamespace = 15;");
    r = txn1.exec("SELECT * FROM pg_catalog.pg_class WHERE reloid > 1;");
    txn1.commit();
  } catch (const std::exception &e) {
    EXPECT_TRUE(false);
  }
}
}  // namespace noisepage::catalog
