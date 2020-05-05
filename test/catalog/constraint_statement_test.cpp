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
#include "network/connection_handle_factory.h"
#include "network/terrier_server.h"
#include "storage/garbage_collector.h"
#include "test_util/manual_packet_util.h"
#include "test_util/test_harness.h"
#include "traffic_cop/traffic_cop_defs.h"
#include "transaction/deferred_action_manager.h"
#include "transaction/transaction_manager.h"

namespace terrier::trafficcop {

class ConstraintStatementTest : public TerrierTest {
 protected:
  void SetUp() override {
    std::unordered_map<settings::Param, settings::ParamInfo> param_map;
    terrier::settings::SettingsManager::ConstructParamMap(param_map);

    db_main_ = terrier::DBMain::Builder()
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
    catalog_ = db_main_->GetCatalogLayer()->GetCatalog();
    txn_manager_ = db_main_->GetTransactionLayer()->GetTransactionManager();
  }

  std::unique_ptr<DBMain> db_main_;
  uint16_t port_;
  common::ManagedPointer<catalog::Catalog> catalog_;
  common::ManagedPointer<transaction::TransactionManager> txn_manager_;
};

// NOLINTNEXTLINE
//TEST_F(ConstraintStatementTest, BasicTableCreationTest) {
//  try {
//    pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
//                                            port_, catalog::DEFAULT_DATABASE));
//
//    pqxx::work txn1(connection);
//    std::cerr << "start create table test\n";
//    txn1.exec("CREATE TABLE TableA (id INT PRIMARY KEY, data TEXT);");
//    std::cerr << "pass create table test\n";
//
////    auto txn = txn_manager_->BeginTransaction();
////    auto db_oid = catalog_->GetDatabaseOid(common::ManagedPointer(txn), catalog::DEFAULT_DATABASE);
////    EXPECT_NE(db_oid, catalog::INVALID_DATABASE_OID);
////    auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid);
////    EXPECT_NE(accessor, nullptr);
////
////    catalog::table_oid_t table_oid = accessor->GetTableOid("tablea");
////    const auto &schema = accessor->GetSchema(table_oid);
////    std::vector<catalog::col_oid_t> pk_cols;
////    pk_cols.push_back(schema.GetColumn("id").Oid());
////    EXPECT_NE(pk_cols.size(), 1);
////    auto index_oid = accessor->GetIndexOids(table_oid)[0];
////    auto new_namespace_oid = accessor->CreateNamespace(std::string(trafficcop::TEMP_NAMESPACE_PREFIX));
////    accessor->CreatePKConstraint(new_namespace_oid, table_oid, "test_pk", index_oid, pk_cols);
////    txn_manager_->Abort(txn);
//
//    txn1.exec("INSERT INTO TableA VALUES (1, 'abc');");
//    std::cerr << "pass insert test\n";
//   pqxx::result r2 = txn1.exec("SELECT * FROM pg_constraint;");
//   EXPECT_EQ(r2.size(), 1);
//   std::cerr << "pass select test 1\n";
//    pqxx::result r = txn1.exec("SELECT * FROM TableA;");
//    EXPECT_EQ(r.size(), 1);
//    std::cerr << "pass select test\n";
//    txn1.commit();
//    connection.disconnect();
//  } catch (const std::exception &e) {
//    EXPECT_TRUE(false);
//    std::cerr << e.what();
//  }
//}
//
///**
// * Test whether a temporary namespace is created for a connection to the database
// */
//// NOLINTNEXTLINE
//TEST_F(ConstraintStatementTest, TemporaryNamespaceTest) {
//  try {
//    pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
//                                            port_, catalog::DEFAULT_DATABASE));
//
//    pqxx::work txn1(connection);
//
//    // Create a new namespace and make sure that its OID is higher than the default start OID,
//    // which should have been assigned to the temporary namespace for this connection
//    catalog::namespace_oid_t new_namespace_oid = catalog::INVALID_NAMESPACE_OID;
//    do {
//      auto txn = txn_manager_->BeginTransaction();
//      auto db_oid = catalog_->GetDatabaseOid(common::ManagedPointer(txn), catalog::DEFAULT_DATABASE);
//      EXPECT_NE(db_oid, catalog::INVALID_DATABASE_OID);
//      auto db_accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid);
//      EXPECT_NE(db_accessor, nullptr);
//      new_namespace_oid = db_accessor->CreateNamespace(std::string(trafficcop::TEMP_NAMESPACE_PREFIX));
//      txn_manager_->Abort(txn);
//    } while (new_namespace_oid == catalog::INVALID_NAMESPACE_OID);
//    EXPECT_GT(static_cast<uint32_t>(new_namespace_oid), catalog::START_OID);
//    txn1.commit();
//    connection.disconnect();
//  } catch (const std::exception &e) {
//    EXPECT_TRUE(false);
//  }
//}
//
///**
// * Test whether a temporary namespace is created for a connection to the database
// */
//// NOLINTNEXTLINE
//TEST_F(ConstraintStatementTest, CreateSinglePKTest) {
//  try {
//    pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
//                                            port_, catalog::DEFAULT_DATABASE));
//
//    pqxx::work txn1(connection);
//    txn1.exec("CREATE TABLE TableA (id INT PRIMARY KEY, data TEXT);");
//    txn1.exec("INSERT INTO TableA VALUES (1, 'abc');");
//    pqxx::result r = txn1.exec("SELECT * FROM TableA");
//    EXPECT_EQ(r.size(), 1);
//   r = txn1.exec("SELECT * FROM pg_constraint");
//   EXPECT_EQ(r.size(), 1);
//    txn1.commit();
//    connection.disconnect();
//  } catch (const std::exception &e) {
//    EXPECT_TRUE(false);
//  }
//}
//
//TEST_F(ConstraintStatementTest, CreateMultiplePKTest) {
//  try {
//    pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
//                                            port_, catalog::DEFAULT_DATABASE));
//
//    pqxx::work txn1(connection);
//    txn1.exec("CREATE TABLE TableA (id INT PRIMARY KEY, data TEXT);");
//    txn1.exec("CREATE TABLE TableB (id INT PRIMARY KEY, data TEXT);");
//    txn1.exec("CREATE TABLE TableC (id INT PRIMARY KEY, data TEXT);");
//    txn1.exec("INSERT INTO TableA VALUES (1, 'abc');");
//    pqxx::result r = txn1.exec("SELECT * FROM TableA");
//    EXPECT_EQ(r.size(), 1);
//    r = txn1.exec("SELECT * FROM pg_constraint");
//    EXPECT_EQ(r.size(), 3);
//    txn1.commit();
//    connection.disconnect();
//  } catch (const std::exception &e) {
//    EXPECT_TRUE(false);
//  }
//}
//
//TEST_F(ConstraintStatementTest, DeleteMultiplePKTest) {
//try {
//pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
//                                        port_, catalog::DEFAULT_DATABASE));
//
//pqxx::work txn1(connection);
//txn1.exec("CREATE TABLE TableA (id INT PRIMARY KEY, data TEXT);");
//txn1.exec("CREATE TABLE TableB (id INT PRIMARY KEY, data TEXT);");
//txn1.exec("CREATE TABLE TableC (id INT PRIMARY KEY, data TEXT);");
//txn1.exec("INSERT INTO TableA VALUES (1, 'abc');");
//pqxx::result r = txn1.exec("SELECT * FROM TableA");
//EXPECT_EQ(r.size(), 1);
//r = txn1.exec("SELECT * FROM pg_constraint");
//EXPECT_EQ(r.size(), 3);
//r = txn1.exec("DROP TABLE TableA");
//std::cerr << "pass drop 1\n";
//r = txn1.exec("SELECT * FROM pg_constraint");
//EXPECT_EQ(r.size(), 2);
//r = txn1.exec("DROP TABLE TableC");
//std::cerr << "pass drop 2\n";
//r = txn1.exec("SELECT * FROM pg_constraint");
//EXPECT_EQ(r.size(), 1);
//r = txn1.exec("DROP TABLE TableB");
//std::cerr << "pass drop 3\n";
//r = txn1.exec("SELECT * FROM pg_constraint");
//EXPECT_EQ(r.size(), 0);
//txn1.commit();
//connection.disconnect();
//} catch (const std::exception &e) {
//EXPECT_TRUE(false);
//}
//}
//
//TEST_F(ConstraintStatementTest, CreateUNIQUETest) {
//try {
//pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
//                                        port_, catalog::DEFAULT_DATABASE));
//
//pqxx::work txn1(connection);
//txn1.exec("CREATE TABLE TableA (id INT PRIMARY KEY, number INT UNIQUE, data TEXT);");
//txn1.exec("INSERT INTO TableA VALUES (1, 1, 'abc');");
//pqxx::result r = txn1.exec("SELECT * FROM TableA");
//EXPECT_EQ(r.size(), 1);
//r = txn1.exec("SELECT * FROM pg_constraint");
//EXPECT_EQ(r.size(), 2);
//txn1.commit();
//connection.disconnect();
//} catch (const std::exception &e) {
//EXPECT_TRUE(false);
//}
//}
//
//TEST_F(ConstraintStatementTest, CreateMultipleFKTest) {
//try {
//pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
//                                        port_, catalog::DEFAULT_DATABASE));
//
//pqxx::work txn1(connection);
//txn1.exec("CREATE TABLE TableA (id INT PRIMARY KEY, data TEXT);");
//txn1.exec("CREATE TABLE TableB (id INT PRIMARY KEY, fk1 INT references TableA(id));");
//txn1.exec("CREATE TABLE TableC (id INT PRIMARY KEY, fk2 INT references TableB(fk1));");
//txn1.exec("INSERT INTO TableA VALUES (1, 'abc');");
//pqxx::result r = txn1.exec("SELECT * FROM TableA");
//EXPECT_EQ(r.size(), 1);
//r = txn1.exec("SELECT * FROM pg_constraint");
//EXPECT_EQ(r.size(), 5);
//r = txn1.exec("SELECT * FROM fk_constraint");
//EXPECT_EQ(r.size(), 2);
//txn1.commit();
//connection.disconnect();
//} catch (const std::exception &e) {
//EXPECT_TRUE(false);
//}
//}
//
//TEST_F(ConstraintStatementTest, DropUNIQUETest) {
//try {
//pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
//                                        port_, catalog::DEFAULT_DATABASE));
//
//pqxx::work txn1(connection);
//txn1.exec("CREATE TABLE TableA (id INT PRIMARY KEY, data TEXT UNIQUE);");
//txn1.exec("INSERT INTO TableA VALUES (1, 'abc');");
//pqxx::result r = txn1.exec("SELECT * FROM TableA");
//EXPECT_EQ(r.size(), 1);
//r = txn1.exec("SELECT * FROM pg_constraint");
//EXPECT_EQ(r.size(), 2);
//r = txn1.exec("DROP TABLE TableA;");
//r = txn1.exec("SELECT * FROM pg_constraint");
//EXPECT_EQ(r.size(), 0);
//txn1.commit();
//connection.disconnect();
//} catch (const std::exception &e) {
//EXPECT_TRUE(false);
//}
//}
//
//TEST_F(ConstraintStatementTest, DeleteMultipleFKTest) {
//try {
//pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
//                                        port_, catalog::DEFAULT_DATABASE));
//
//pqxx::work txn1(connection);
//txn1.exec("CREATE TABLE TableA (id INT PRIMARY KEY, data TEXT);");
//txn1.exec("CREATE TABLE TableB (id INT PRIMARY KEY, fk1 INT references TableA(id));");
//txn1.exec("CREATE TABLE TableC (id INT PRIMARY KEY, fk2 INT references TableB(fk1));");
//txn1.exec("INSERT INTO TableA VALUES (1, 'abc');");
//pqxx::result r = txn1.exec("SELECT * FROM TableA");
//EXPECT_EQ(r.size(), 1);
//r = txn1.exec("SELECT * FROM pg_constraint");
//EXPECT_EQ(r.size(), 5);
//r = txn1.exec("SELECT * FROM fk_constraint");
//EXPECT_EQ(r.size(), 2);
//r = txn1.exec("DROP TABLE TableB");
//r = txn1.exec("SELECT * FROM fk_constraint");
//EXPECT_EQ(r.size(), 1);
//r = txn1.exec("DROP TABLE TableC");
//r = txn1.exec("SELECT * FROM fk_constraint");
//EXPECT_EQ(r.size(), 0);
//std::cerr << "till end\n";
//txn1.commit();
//connection.disconnect();
//} catch (const std::exception &e) {
//EXPECT_TRUE(false);
//}
//}
//
//
//
//TEST_F(ConstraintStatementTest, EnforcePKTest) {
//try {
//pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
//                                        port_, catalog::DEFAULT_DATABASE));
//
//pqxx::work txn1(connection);
//txn1.exec("CREATE TABLE TableA (id INT PRIMARY KEY, data TEXT);");
//txn1.exec("INSERT INTO TableA VALUES (1, 'abc');");
//pqxx::result r = txn1.exec("SELECT * FROM TableA");
//EXPECT_EQ(r.size(), 1);
//r = txn1.exec("SELECT * FROM pg_constraint");
//EXPECT_EQ(r.size(), 1);
////txn1.exec("INSERT INTO TableA VALUES (1, 'abc');");
//r = txn1.exec("SELECT * FROM TableA");
//EXPECT_EQ(r.size(), 1);
//txn1.exec("INSERT INTO TableA VALUES (2, 'abc');");
//r = txn1.exec("SELECT * FROM TableA");
//EXPECT_EQ(r.size(), 2);
//std::cerr << "till end\n";
//txn1.commit();
//connection.disconnect();
//} catch (const std::exception &e) {
//EXPECT_TRUE(false);
//std::cerr << e.what();
//}
//}
//
//TEST_F(ConstraintStatementTest, EnforceFKTestSimple) {
//try {
//pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
//                                        port_, catalog::DEFAULT_DATABASE));
//
//pqxx::work txn1(connection);
//txn1.exec("CREATE TABLE TableA (id INT PRIMARY KEY, data TEXT);");
//txn1.exec("CREATE TABLE TableB (id INT PRIMARY KEY, fk1 INT references TableA(id));");
//txn1.exec("INSERT INTO TableA VALUES (1, 'abc');");
//pqxx::result r = txn1.exec("SELECT * FROM TableA");
//EXPECT_EQ(r.size(), 1);
//r = txn1.exec("SELECT * FROM pg_constraint");
//EXPECT_EQ(r.size(), 3);
//r = txn1.exec("SELECT * FROM TableA");
//EXPECT_EQ(r.size(), 1);
//r = txn1.exec("SELECT * FROM TableB");
//EXPECT_EQ(r.size(), 0);
//txn1.exec("INSERT INTO TableB VALUES (1, 1);");
//r = txn1.exec("SELECT * FROM TableB");
//EXPECT_EQ(r.size(), 1);
//std::cerr << "till end\n";
//txn1.commit();
//connection.disconnect();
//} catch (const std::exception &e) {
//EXPECT_TRUE(false);
//std::cerr << e.what();
//}
//}

TEST_F(ConstraintStatementTest, EnforceFKTest) {
try {
pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
                                        port_, catalog::DEFAULT_DATABASE));

pqxx::work txn1(connection);
txn1.exec("CREATE TABLE TableA (id INT PRIMARY KEY, data TEXT);");
txn1.exec("CREATE TABLE TableB (id INT PRIMARY KEY, fk1 INT references TableA(id));");
txn1.exec("INSERT INTO TableA (id, data) VALUES (1, 'abc');");
//txn1.exec("INSERT INTO TableA (id, data) VALUES (2, 7);");
//pqxx::result r = txn1.exec("SELECT * FROM TableA");
//EXPECT_EQ(r.size(), 1);
//r = txn1.exec("SELECT * FROM pg_constraint");
//EXPECT_EQ(r.size(), 5);
//r = txn1.exec("SELECT * FROM TableA");
//EXPECT_EQ(r.size(), 1);
//r = txn1.exec("SELECT * FROM TableB");
//EXPECT_EQ(r.size(), 0);
txn1.exec("INSERT INTO TableB VALUES (1, 1);");
pqxx::result r = txn1.exec("SELECT * FROM TableB");
EXPECT_EQ(r.size(), 1);
//txn1.exec("INSERT INTO TableC VALUES (1, 1);");
//r = txn1.exec("SELECT * FROM TableC");
//EXPECT_EQ(r.size(), 1);
txn1.exec("INSERT INTO TableB VALUES (2, 1);");
r = txn1.exec("SELECT * FROM TableB");
EXPECT_EQ(r.size(), 1);
std::cerr << "till end\n";
txn1.commit();
connection.disconnect();
} catch (const std::exception &e) {
EXPECT_TRUE(false);
std::cerr << e.what();
}
}

}  // namespace terrier::trafficcop
