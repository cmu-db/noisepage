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
#include "test_util/test_harness.h"

namespace noisepage::trafficcop {

class TrafficCopTests : public TerrierTest {
 protected:
  void StartServer(const bool wal_async_commit_enable) {
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
                   .SetWalAsyncCommit(wal_async_commit_enable)
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
  StartServer(false);
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
  StartServer(false);
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
  StartServer(false);
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
  StartServer(false);
  try {
    pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
                                            port_, catalog::DEFAULT_DATABASE));

    pqxx::work txn1(connection);
    pqxx::result r = txn1.exec("INSTERT INTO FOO VALUES (1,1);");
    txn1.commit();
  } catch (const std::exception &e) {
    std::string error(e.what());
    std::string expect(
        "ERROR:  syntax error at or near \"INSTERT\"\nLINE 1: INSTERT INTO FOO VALUES (1,1);\n        ^\n");
    EXPECT_EQ(error, expect);
  }
}

// NOLINTNEXTLINE
TEST_F(TrafficCopTests, BadBindingTest) {
  StartServer(false);
  try {
    pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
                                            port_, catalog::DEFAULT_DATABASE));

    pqxx::work txn1(connection);
    pqxx::result r = txn1.exec("INSERT INTO FOO VALUES (1,1);");
    txn1.commit();
  } catch (const std::exception &e) {
    std::string error(e.what());
    std::string expect("ERROR:  relation \"foo\" does not exist\n");
    EXPECT_EQ(error, expect);
  }
}

// NOLINTNEXTLINE
TEST_F(TrafficCopTests, BasicTest) {
  StartServer(false);
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
  StartServer(false);
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
      auto db_accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);
      EXPECT_NE(db_accessor, nullptr);
      new_namespace_oid = db_accessor->CreateNamespace(std::string(trafficcop::TEMP_NAMESPACE_PREFIX));
      txn_manager_->Abort(txn);
    } while (new_namespace_oid == catalog::INVALID_NAMESPACE_OID);
    EXPECT_GT(new_namespace_oid.UnderlyingValue(), catalog::START_OID);
    txn1.commit();
  } catch (const std::exception &e) {
    EXPECT_TRUE(false);
  }
}

// NOLINTNEXTLINE
TEST_F(TrafficCopTests, ArithmeticErrorTest) {
  StartServer(false);
  pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
                                          port_, catalog::DEFAULT_DATABASE));

  pqxx::work txn1(connection);
  try {
    pqxx::result r = txn1.exec("SELECT ASIN(10.0);");
  } catch (const std::exception &e) {
    std::string error(e.what());
    std::string expect("ERROR:  ASin is undefined outside [-1,1]\n");
    EXPECT_EQ(error, expect);
  }
}

// NOLINTNEXTLINE
TEST_F(TrafficCopTests, AsyncCommitTest) {
  StartServer(true);
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

}  // namespace noisepage::trafficcop
