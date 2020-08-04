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
#include "traffic_cop/traffic_cop.h"
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
TEST_F(ConstraintStatementTest, BasicTableCreationTest) {
  try {
    pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
                                            port_, catalog::DEFAULT_DATABASE));

    // BasicTableCreationTest;
    pqxx::work txn1(connection);
    // start create table test;
    txn1.exec("CREATE TABLE TableA (id INT PRIMARY KEY, data TEXT);");
    // pass create table test;

    txn1.exec("INSERT INTO TableA VALUES (1, 'abc');");
    // pass insert test;
    pqxx::result r2 = txn1.exec("SELECT * FROM pg_constraint;");
    EXPECT_EQ(r2.size(), 1) << "This is a test !!!!!\n";
    // pass select test 1;
    pqxx::result r = txn1.exec("SELECT * FROM TableA;");
    EXPECT_EQ(r.size(), 1);
    // pass select test;
    txn1.commit();
    connection.disconnect();
  } catch (const std::exception &e) {
    EXPECT_TRUE(false);
  }
}

// NOLINTNEXTLINE
TEST_F(ConstraintStatementTest, FKCreationRestrictionSuccess) {
  try {
    pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
                                            port_, catalog::DEFAULT_DATABASE));

    // FKCreationRestriction;
    pqxx::work txn1(connection);
    txn1.exec("CREATE TABLE TableA (id INT PRIMARY KEY, data INT UNIQUE, name TEXT UNIQUE);");
    txn1.exec("CREATE TABLE TableB (id INT PRIMARY KEY, fk1 INT references TableA(id));");
    txn1.exec(
        "CREATE TABLE TableC (id INT PRIMARY KEY, fk1 INT, fk2 TEXT, FOREIGN KEY (fk1, fk2) references TableA(id, "
        "name));");
    // till end;
    txn1.commit();
    connection.disconnect();
  } catch (const std::exception &e) {
    EXPECT_TRUE(false);
  }
}

TEST_F(ConstraintStatementTest, FKCreationReferenceNoneUnique) {
  try {
    pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
                                            port_, catalog::DEFAULT_DATABASE));
    //FKCreationReferenceNoneUnique;
    pqxx::work txn1(connection);
    txn1.exec("CREATE TABLE TableA (id INT PRIMARY KEY, data INT, name TEXT UNIQUE);");
    txn1.exec("CREATE TABLE TableC (id INT PRIMARY KEY, fk1 INT, fk2 TEXT REFERENCES TableA (data));");
    EXPECT_TRUE(false);  // expect creation raise error, should never reach here
    txn1.commit();
    connection.disconnect();
  } catch (const std::exception &e) {
    EXPECT_TRUE(true);
  }
}

TEST_F(ConstraintStatementTest, FKCreationIncludingNoneUnique) {
  try {
    pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
                                            port_, catalog::DEFAULT_DATABASE));

    // FKCreationIncludingNoneUnique;
    pqxx::work txn1(connection);
    txn1.exec("CREATE TABLE TableA (id INT PRIMARY KEY, data INT, name TEXT UNIQUE);");
    txn1.exec(
        "CREATE TABLE TableC (id INT PRIMARY KEY, fk1 INT, fk2 TEXT, FOREIGN KEY (fk1, fk2) REFERENCES TableA (data, "
        "name));");
    EXPECT_TRUE(false);
    txn1.commit();
    connection.disconnect();
  } catch (const std::exception &e) {
    EXPECT_TRUE(true);
  }
}

TEST_F(ConstraintStatementTest, FKCreationDisjointUniquePKConstraint) {
  try {
    pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
                                            port_, catalog::DEFAULT_DATABASE));

    // FKCreationDisjointUniquePKConstraint;
    pqxx::work txn1(connection);
    txn1.exec(
        "CREATE TABLE TableA (id INT, data1 INT, data2 INT, data3 INT, data4 INT, data5 INT UNIQUE, PRIMARY KEY(id, "
        "data1));");
    txn1.exec(
        "CREATE TABLE TableC (id INT PRIMARY KEY, fk1 INT, fk2 INT, FOREIGN KEY (fk1, fk2) REFERENCES TableA (id, "
        "data5));");
    EXPECT_TRUE(false);
    txn1.commit();
    connection.disconnect();
  } catch (const std::exception &e) {
    EXPECT_TRUE(true);
  }
}

TEST_F(ConstraintStatementTest, FKCreationRestrictionTrueComples3) {
  try {
    pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
                                            port_, catalog::DEFAULT_DATABASE));

    // FKCreationRestrictionTrueComples3;
    pqxx::work txn1(connection);
    txn1.exec(
        "CREATE TABLE TableA (id INT, data1 INT, data2 INT, data3 INT, data4 INT, data5 INT UNIQUE, PRIMARY KEY(id, "
        "data1));");
    txn1.exec(
        "CREATE TABLE TableC (id INT PRIMARY KEY, fk1 INT, fk2 INT, fk3 INT, FOREIGN KEY (fk1, fk2, fk3) REFERENCES "
        "TableA (id, data1, data5));");
    txn1.commit();
    connection.disconnect();
  } catch (const std::exception &e) {
    EXPECT_TRUE(false);
  }
}

TEST_F(ConstraintStatementTest, FKCreationTypeMismatch) {
  try {
    pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
                                            port_, catalog::DEFAULT_DATABASE));

    // "FKCreationTypeMismatch";
    pqxx::work txn1(connection);
    txn1.exec("CREATE TABLE TableA (id INT PRIMARY KEY, data INT, name TEXT UNIQUE);");
    txn1.exec("CREATE TABLE TableC (id INT PRIMARY KEY, fk1 INT REFERENCES TableA(name));");
    EXPECT_TRUE(false);
    txn1.commit();
    connection.disconnect();
  } catch (const std::exception &e) {
    EXPECT_TRUE(true);
  }
}

/**
 * Test whether a temporary namespace is created for a connection to the database
 */
// NOLINTNEXTLINE
TEST_F(ConstraintStatementTest, TemporaryNamespaceTest) {
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
    EXPECT_GT(static_cast<uint32_t>(new_namespace_oid), catalog::START_OID);
    txn1.commit();
    connection.disconnect();
  } catch (const std::exception &e) {
    EXPECT_TRUE(false);
  }
}

/**
 * Test whether a temporary namespace is created for a connection to the database
 */
// NOLINTNEXTLINE
TEST_F(ConstraintStatementTest, CreateSinglePKTest) {
  try {
    pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
                                            port_, catalog::DEFAULT_DATABASE));

    pqxx::work txn1(connection);
    txn1.exec("CREATE TABLE TableA (id INT PRIMARY KEY, data TEXT);");
    txn1.exec("INSERT INTO TableA VALUES (1, 'abc');");
    pqxx::result r = txn1.exec("SELECT * FROM TableA");
    EXPECT_EQ(r.size(), 1);
    r = txn1.exec("SELECT * FROM pg_constraint");
    EXPECT_EQ(r.size(), 1);
    txn1.commit();
    connection.disconnect();
  } catch (const std::exception &e) {
    EXPECT_TRUE(false);
  }
}

// NOLINTNEXTLINE
TEST_F(ConstraintStatementTest, CreateMultiplePKTest) {
  try {
    pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
                                            port_, catalog::DEFAULT_DATABASE));

    pqxx::work txn1(connection);
    txn1.exec("CREATE TABLE TableA (id INT PRIMARY KEY, data TEXT);");
    txn1.exec("CREATE TABLE TableB (id INT PRIMARY KEY, data TEXT);");
    txn1.exec("CREATE TABLE TableC (id INT PRIMARY KEY, data TEXT);");
    txn1.exec("INSERT INTO TableA VALUES (1, 'abc');");
    pqxx::result r = txn1.exec("SELECT * FROM TableA");
    EXPECT_EQ(r.size(), 1);
    r = txn1.exec("SELECT * FROM pg_constraint");
    EXPECT_EQ(r.size(), 3);
    txn1.commit();
    connection.disconnect();
  } catch (const std::exception &e) {
    EXPECT_TRUE(false);
  }
}

// NOLINTNEXTLINE
TEST_F(ConstraintStatementTest, DeleteMultiplePKTest) {
  try {
    pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
                                            port_, catalog::DEFAULT_DATABASE));

    pqxx::work txn1(connection);
    txn1.exec("CREATE TABLE TableA (id INT PRIMARY KEY, data TEXT);");
    txn1.exec("CREATE TABLE TableB (id INT PRIMARY KEY, data TEXT);");
    txn1.exec("CREATE TABLE TableC (id INT PRIMARY KEY, data TEXT);");
    txn1.exec("INSERT INTO TableA VALUES (1, 'abc');");
    pqxx::result r = txn1.exec("SELECT * FROM TableA");
    EXPECT_EQ(r.size(), 1);
    r = txn1.exec("SELECT * FROM pg_constraint");
    EXPECT_EQ(r.size(), 3);
    r = txn1.exec("DROP TABLE TableA");
    // pass drop 1;
    r = txn1.exec("SELECT * FROM pg_constraint");
    EXPECT_EQ(r.size(), 2);
    r = txn1.exec("DROP TABLE TableC");
    // pass drop 2;
    r = txn1.exec("SELECT * FROM pg_constraint");
    EXPECT_EQ(r.size(), 1);
    r = txn1.exec("DROP TABLE TableB");
    r = txn1.exec("SELECT * FROM pg_constraint");
    EXPECT_EQ(r.size(), 0);
    txn1.commit();
    connection.disconnect();
  } catch (const std::exception &e) {
    EXPECT_TRUE(false);
  }
}

// NOLINTNEXTLINE
TEST_F(ConstraintStatementTest, CreateUNIQUETest) {
  try {
    pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
                                            port_, catalog::DEFAULT_DATABASE));

    pqxx::work txn1(connection);
    txn1.exec("CREATE TABLE TableA (id INT PRIMARY KEY, number INT UNIQUE, data TEXT);");
    txn1.exec("INSERT INTO TableA VALUES (1, 1, 'abc');");
    pqxx::result r = txn1.exec("SELECT * FROM TableA");
    EXPECT_EQ(r.size(), 1);
    r = txn1.exec("SELECT * FROM pg_constraint");
    EXPECT_EQ(r.size(), 2);
    txn1.commit();
    connection.disconnect();
  } catch (const std::exception &e) {
    EXPECT_TRUE(false);
  }
}

// NOLINTNEXTLINE
TEST_F(ConstraintStatementTest, CreateMultipleFKTest) {
  try {
    pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
                                            port_, catalog::DEFAULT_DATABASE));

    pqxx::work txn1(connection);
    txn1.exec("CREATE TABLE TableA (id INT PRIMARY KEY, data TEXT);");
    txn1.exec("CREATE TABLE TableB (id INT PRIMARY KEY, fk1 INT references TableA(id));");
    txn1.exec("CREATE TABLE TableC (id INT PRIMARY KEY, fk2 INT references TableB(fk1));");
    txn1.exec("INSERT INTO TableA VALUES (1, 'abc');");
    pqxx::result r = txn1.exec("SELECT * FROM TableA");
    EXPECT_EQ(r.size(), 1);
    r = txn1.exec("SELECT * FROM pg_constraint");
    EXPECT_EQ(r.size(), 5);
    txn1.commit();
    connection.disconnect();
  } catch (const std::exception &e) {
    EXPECT_TRUE(false);
  }
}

// NOLINTNEXTLINE
TEST_F(ConstraintStatementTest, DropUNIQUETest) {
  try {
    pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
                                            port_, catalog::DEFAULT_DATABASE));

    pqxx::work txn1(connection);
    txn1.exec("CREATE TABLE TableA (id INT PRIMARY KEY, data TEXT UNIQUE);");
    txn1.exec("INSERT INTO TableA VALUES (1, 'abc');");
    pqxx::result r = txn1.exec("SELECT * FROM TableA");
    EXPECT_EQ(r.size(), 1);
    r = txn1.exec("SELECT * FROM pg_constraint");
    EXPECT_EQ(r.size(), 2);
    r = txn1.exec("DROP TABLE TableA;");
    r = txn1.exec("SELECT * FROM pg_constraint");
    EXPECT_EQ(r.size(), 0);
    txn1.commit();
    connection.disconnect();
  } catch (const std::exception &e) {
    EXPECT_TRUE(false);
  }
}

// NOLINTNEXTLINE
TEST_F(ConstraintStatementTest, DeleteMultipleFKTest) {
  try {
    pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
                                            port_, catalog::DEFAULT_DATABASE));

    pqxx::work txn1(connection);
    txn1.exec("CREATE TABLE TableA (id INT PRIMARY KEY, data TEXT);");
    txn1.exec("CREATE TABLE TableB (id INT PRIMARY KEY, fk1 INT references TableA(id));");
    txn1.exec("CREATE TABLE TableC (id INT PRIMARY KEY, fk2 INT references TableB(fk1));");
    txn1.exec("INSERT INTO TableA VALUES (1, 'abc');");
    pqxx::result r = txn1.exec("SELECT * FROM TableA");
    EXPECT_EQ(r.size(), 1);
    r = txn1.exec("SELECT * FROM pg_constraint");
    EXPECT_EQ(r.size(), 5);
    r = txn1.exec("DROP TABLE TableB");
    r = txn1.exec("DROP TABLE TableC");
    r = txn1.exec("SELECT * FROM pg_constraint");
    EXPECT_EQ(r.size(), 1);
    txn1.commit();
    connection.disconnect();
  } catch (const std::exception &e) {
    EXPECT_TRUE(false);
  }
}

// NOLINTNEXTLINE
TEST_F(ConstraintStatementTest, EnforcePKTest) {
  try {
    pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
                                            port_, catalog::DEFAULT_DATABASE));

    pqxx::work txn1(connection);
    txn1.exec("CREATE TABLE TableA (id INT PRIMARY KEY, data TEXT);");
    txn1.exec("INSERT INTO TableA VALUES (1, 'abc');");
    pqxx::result r = txn1.exec("SELECT * FROM TableA");
    EXPECT_EQ(r.size(), 1);
    r = txn1.exec("SELECT * FROM pg_constraint");
    EXPECT_EQ(r.size(), 1);
    // txn1.exec("INSERT INTO TableA VALUES (1, 'abc');");
    r = txn1.exec("SELECT * FROM TableA");
    EXPECT_EQ(r.size(), 1);
    txn1.exec("INSERT INTO TableA VALUES (2, 'abc');");
    r = txn1.exec("SELECT * FROM TableA");
    EXPECT_EQ(r.size(), 2);
    txn1.commit();
    connection.disconnect();
  } catch (const std::exception &e) {
    EXPECT_TRUE(false);
  }
}

// NOLINTNEXTLINE
TEST_F(ConstraintStatementTest, EnforceFKTestSimple) {
  try {
    pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
                                            port_, catalog::DEFAULT_DATABASE));

    pqxx::work txn1(connection);
    txn1.exec("CREATE TABLE TableA (id INT PRIMARY KEY, data TEXT);");
    txn1.exec("CREATE TABLE TableB (id INT PRIMARY KEY, fk1 INT references TableA(id));");
    txn1.exec("INSERT INTO TableA VALUES (1, 'abc');");
    pqxx::result r = txn1.exec("SELECT * FROM TableA");
    EXPECT_EQ(r.size(), 1);
    r = txn1.exec("SELECT * FROM pg_constraint");
    EXPECT_EQ(r.size(), 3);
    r = txn1.exec("SELECT * FROM TableA");
    EXPECT_EQ(r.size(), 1);
    r = txn1.exec("SELECT * FROM TableB");
    EXPECT_EQ(r.size(), 0);
    txn1.exec("INSERT INTO TableB VALUES (1, 1);");
    r = txn1.exec("SELECT * FROM TableB");
    EXPECT_EQ(r.size(), 1);
    txn1.commit();
    connection.disconnect();
  } catch (const std::exception &e) {
    EXPECT_TRUE(false);
  }
}

// NOLINTNEXTLINE
TEST_F(ConstraintStatementTest, EnforceFKTest) {
  try {
    pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
                                            port_, catalog::DEFAULT_DATABASE));

    pqxx::work txn1(connection);
    txn1.exec("CREATE TABLE TableA (id INT PRIMARY KEY, data TEXT UNIQUE);");
    txn1.exec("CREATE TABLE TableB (id INT PRIMARY KEY, fk1 INT references TableA(id));");
    txn1.exec("CREATE TABLE TableC (id INT PRIMARY KEY, fk1 TEXT references TableA(data));");
    txn1.exec("INSERT INTO TableA (id, data) VALUES (1, 'abcacb');");
    // txn1.exec("INSERT INTO TableA (id, data) VALUES (1, 2);");
    pqxx::result r = txn1.exec("SELECT * FROM TableA");
    EXPECT_EQ(r.size(), 1);
    r = txn1.exec("SELECT * FROM pg_constraint");
    EXPECT_EQ(r.size(), 6);
    r = txn1.exec("SELECT * FROM TableA");
    EXPECT_EQ(r.size(), 1);
    r = txn1.exec("SELECT * FROM TableB");
    EXPECT_EQ(r.size(), 0);
    txn1.exec("INSERT INTO TableB VALUES (1, 1);");
    r = txn1.exec("SELECT * FROM TableB");
    EXPECT_EQ(r.size(), 1);
    txn1.exec("INSERT INTO TableB VALUES (2, 1);");
    // txn1.exec("INSERT INTO TableB VALUES (3, 2);");
    r = txn1.exec("SELECT * FROM TableB");
    EXPECT_EQ(r.size(), 2);
    txn1.exec("INSERT INTO TableC VALUES (1, 'abcacb');");
    r = txn1.exec("SELECT * FROM TableC");
    EXPECT_EQ(r.size(), 1);
    txn1.commit();
    connection.disconnect();
  } catch (const std::exception &e) {
    EXPECT_TRUE(false);
  }
}

// NOLINTNEXTLINE
TEST_F(ConstraintStatementTest, VerifyUpdateFKTest2) {
  try {
    pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
                                            port_, catalog::DEFAULT_DATABASE));

    pqxx::work txn1(connection);
    txn1.exec("CREATE TABLE TableA (id INT PRIMARY KEY, data INT UNIQUE, name TEXT UNIQUE);");
    txn1.exec("CREATE TABLE TableB (id INT PRIMARY KEY, fk1 INT references TableA(id));");
    txn1.exec("CREATE TABLE TableC (id INT PRIMARY KEY, fk2 TEXT references TableA(name));");
    txn1.exec("INSERT INTO TableA (id, data, name) VALUES (1, 2, 'abcd');");
    txn1.exec("INSERT INTO TableA (id, data, name) VALUES (2, 3, 'defg');");
    txn1.exec("INSERT INTO TableB (id, fk1) VALUES (1, 2);");
    txn1.exec("INSERT INTO TableC (id, fk2) VALUES (1, 'defg');");
    pqxx::result r = txn1.exec("SELECT * FROM TableA");
    EXPECT_EQ(r.size(), 2);

    txn1.exec("UPDATE TableB SET fk1 = 3 WHERE id = 1;");
    EXPECT_TRUE(false);
    txn1.commit();
    connection.disconnect();
  } catch (const std::exception &e) {
    EXPECT_TRUE(true);
  }
}

TEST_F(ConstraintStatementTest, VerifyUpdateFKTest3) {
  try {
    pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
                                            port_, catalog::DEFAULT_DATABASE));

    pqxx::work txn1(connection);
    txn1.exec("CREATE TABLE TableA (id INT PRIMARY KEY, data INT UNIQUE, name TEXT UNIQUE);");
    txn1.exec("CREATE TABLE TableB (id INT PRIMARY KEY, fk1 INT references TableA(id));");
    txn1.exec("CREATE TABLE TableC (id INT PRIMARY KEY, fk2 TEXT references TableA(name));");
    txn1.exec("INSERT INTO TableA (id, data, name) VALUES (1, 2, 'abcd');");
    txn1.exec("INSERT INTO TableA (id, data, name) VALUES (2, 3, 'defg');");
    txn1.exec("INSERT INTO TableB (id, fk1) VALUES (1, 2);");
    txn1.exec("INSERT INTO TableC (id, fk2) VALUES (1, 'defg');");
    pqxx::result r = txn1.exec("SELECT * FROM TableA");
    EXPECT_EQ(r.size(), 2);

    txn1.exec("UPDATE TableA SET data = 2, name = 'abcd' WHERE id = 1;");
    txn1.exec("UPDATE TableB SET fk1 = 2 WHERE id = 1;");
    txn1.commit();
    connection.disconnect();
  } catch (const std::exception &e) {
    EXPECT_TRUE(false);
  }
}

TEST_F(ConstraintStatementTest, VerifyUpdateUNIQUETest) {
  try {
    pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
                                            port_, catalog::DEFAULT_DATABASE));

    pqxx::work txn1(connection);
    txn1.exec("CREATE TABLE TableA (id INT PRIMARY KEY, data INT UNIQUE);");
    txn1.exec("INSERT INTO TableA (id, data) VALUES (1, 2);");
    txn1.exec("INSERT INTO TableA (id, data) VALUES (2, 3);");
    pqxx::result r = txn1.exec("SELECT * FROM TableA");
    EXPECT_EQ(r.size(), 2);

    txn1.exec("UPDATE TableA SET data = 4 WHERE id = 1;");
    txn1.exec("UPDATE TableA SET id = 2 WHERE id = 1;");
    EXPECT_TRUE(false);
    txn1.commit();
    connection.disconnect();
  } catch (const std::exception &e) {
    EXPECT_TRUE(true);
  }
}

TEST_F(ConstraintStatementTest, VerifyUpdateFKTestComplex) {
  try {
    pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
                                            port_, catalog::DEFAULT_DATABASE));

    pqxx::work txn1(connection);
    txn1.exec("CREATE TABLE TableA (id INT PRIMARY KEY, data INT UNIQUE, name TEXT UNIQUE);");
    txn1.exec("CREATE TABLE TableB (id INT PRIMARY KEY, fk1 INT references TableA(id));");
    txn1.exec("CREATE TABLE TableC (id INT PRIMARY KEY, fk2 TEXT references TableA(name));");
    txn1.exec("INSERT INTO TableA (id, data, name) VALUES (1, 2, 'abcd');");
    txn1.exec("INSERT INTO TableA (id, data, name) VALUES (2, 3, 'defg');");
    txn1.exec("INSERT INTO TableB (id, fk1) VALUES (1, 2);");
    txn1.exec("INSERT INTO TableC (id, fk2) VALUES (1, 'defg');");
    pqxx::result r = txn1.exec("SELECT * FROM TableA");
    EXPECT_EQ(r.size(), 2);

    txn1.exec("UPDATE TableA SET data = 2 WHERE id = 1;");
    txn1.commit();
    connection.disconnect();
  } catch (const std::exception &e) {
    EXPECT_TRUE(false);
  }
}

TEST_F(ConstraintStatementTest, VerifyDelete) {
  try {
    pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
                                            port_, catalog::DEFAULT_DATABASE));

    pqxx::work txn1(connection);
    txn1.exec("CREATE TABLE TableA (id INT PRIMARY KEY, data INT, data2 INT);");
    txn1.exec("INSERT INTO TableA VALUES (1, 3, 2);");
    txn1.exec("UPDATE TableA SET data = 4, data2 = 4 WHERE id = 1;");
    txn1.exec("INSERT INTO TableA VALUES (2, 4, 4);");
    pqxx::result r = txn1.exec("SELECT * FROM TableA;");
    EXPECT_EQ(r.size(), 2);

    txn1.exec("DELETE FROM TableA WHERE data = 4;");
    r = txn1.exec("SELECT * FROM TableA;");
    EXPECT_EQ(r.size(), 0);
    txn1.commit();
    connection.disconnect();
  } catch (const std::exception &e) {
    EXPECT_TRUE(false);
  }
}
//
// TEST_F(ConstraintStatementTest, UpdateCascadeSimple) {
//  try {
//    pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
//                                            port_, catalog::DEFAULT_DATABASE));
//
//    pqxx::work txn1(connection);
//    txn1.exec("CREATE TABLE TableA (id INT PRIMARY KEY, data INT UNIQUE, name TEXT UNIQUE);");
//    txn1.exec(
//        "CREATE TABLE TableB (id INT PRIMARY KEY, fk1 INT references TableA(data) ON UPDATE CASCADE ON DELETE "
//        "CASCADE);");
//    txn1.exec("INSERT INTO TableA (id, data, name) VALUES (1, 2, 'abcd');");
//    txn1.exec("INSERT INTO TableB (id, fk1) VALUES (1, 2);");
//    txn1.exec("INSERT INTO TableB (id, fk1) VALUES (2, 2);");
//    pqxx::result r = txn1.exec("SELECT * FROM TableA;");
//    EXPECT_EQ(r.size(), 1);
//    r = txn1.exec("SELECT * FROM TableB;");
//    EXPECT_EQ(r.size(), 2);
//    txn1.exec("UPDATE TableA SET data = 3 WHERE id = 1;");
//    r = txn1.exec("SELECT * FROM TableB WHERE fk1 = 2;");
//    EXPECT_EQ(r.size(), 0);
//    r = txn1.exec("SELECT * FROM TableB WHERE fk1 = 3;");
//    EXPECT_EQ(r.size(), 2);
//    txn1.commit();
//    connection.disconnect();
//  } catch (const std::exception &e) {
//    EXPECT_TRUE(false);
//    std::cerr << e.what();
//  }
//}
//
// TEST_F(ConstraintStatementTest, UpdateCascadeRecursive) {
//  try {
//    pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
//                                            port_, catalog::DEFAULT_DATABASE));
//
//    pqxx::work txn1(connection);
//    txn1.exec("CREATE TABLE TableA (id INT PRIMARY KEY, data INT UNIQUE, name TEXT UNIQUE);");
//    txn1.exec(
//        "CREATE TABLE TableB (id INT PRIMARY KEY, fk1 INT UNIQUE, FOREIGN KEY (fk1) references TableA(data) ON UPDATE
//        " "CASCADE ON DELETE CASCADE);");
//    txn1.exec(
//        "CREATE TABLE TableC (id INT PRIMARY KEY, fk2 INT, FOREIGN KEY (fk2) references TableB(fk1) ON UPDATE CASCADE
//        " "ON DELETE CASCADE);");
//    txn1.exec(
//        "CREATE TABLE TableD (id INT PRIMARY KEY, fk3 INT, FOREIGN KEY (fk3) references TableB(fk1) ON UPDATE CASCADE
//        " "ON DELETE CASCADE);");
//    txn1.exec("INSERT INTO TableA (id, data, name) VALUES (1, 2, 'abcd');");
//    txn1.exec("INSERT INTO TableB VALUES (1, 2);");
//    txn1.exec("INSERT INTO TableC VALUES (1, 2);");
//    txn1.exec("INSERT INTO TableC VALUES (2, 2);");
//    txn1.exec("INSERT INTO TableD VALUES (1, 2);");
//    txn1.exec("INSERT INTO TableD VALUES (2, 2);");
//    pqxx::result r = txn1.exec("SELECT * FROM TableA;");
//    EXPECT_EQ(r.size(), 1);
//    r = txn1.exec("SELECT * FROM TableB;");
//    EXPECT_EQ(r.size(), 1);
//    r = txn1.exec("SELECT * FROM TableC;");
//    EXPECT_EQ(r.size(), 2);
//    r = txn1.exec("SELECT * FROM TableD;");
//    EXPECT_EQ(r.size(), 2);
//    txn1.exec("UPDATE TableA SET data = 3 WHERE id = 1;");
//    r = txn1.exec("SELECT * FROM TableB WHERE fk1 = 2;");
//    EXPECT_EQ(r.size(), 0);
//    r = txn1.exec("SELECT * FROM TableC WHERE fk2 = 2;");
//    EXPECT_EQ(r.size(), 0);
//    r = txn1.exec("SELECT * FROM TableD WHERE fk3 = 2;");
//    EXPECT_EQ(r.size(), 0);
//    r = txn1.exec("SELECT * FROM TableB WHERE fk1 = 3;");
//    EXPECT_EQ(r.size(), 1);
//    r = txn1.exec("SELECT * FROM TableC WHERE fk2 = 3;");
//    EXPECT_EQ(r.size(), 2);
//    r = txn1.exec("SELECT * FROM TableD WHERE fk3 = 3;");
//    EXPECT_EQ(r.size(), 2);
//    txn1.commit();
//    connection.disconnect();
//  } catch (const std::exception &e) {
//    EXPECT_TRUE(false);
//    std::cerr << e.what();
//  }
//}

TEST_F(ConstraintStatementTest, DeleteCascadeSimple) {
  try {
    pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
                                            port_, catalog::DEFAULT_DATABASE));

    pqxx::work txn1(connection);
    txn1.exec("CREATE TABLE TableA (id INT PRIMARY KEY, data INT UNIQUE, name TEXT UNIQUE);");
    txn1.exec(
        "CREATE TABLE TableB (id INT PRIMARY KEY, fk1 INT references TableA(data) ON UPDATE CASCADE ON DELETE "
        "CASCADE);");
    txn1.exec("INSERT INTO TableA (id, data, name) VALUES (1, 2, 'abcd');");
    txn1.exec("INSERT INTO TableB (id, fk1) VALUES (1, 2);");
    txn1.exec("INSERT INTO TableB (id, fk1) VALUES (2, 2);");
    pqxx::result r = txn1.exec("SELECT * FROM TableA;");
    EXPECT_EQ(r.size(), 1);
    r = txn1.exec("SELECT * FROM TableB;");
    EXPECT_EQ(r.size(), 2);
    txn1.exec("DELETE FROM TableA WHERE id = 1;");
    r = txn1.exec("SELECT * FROM TableB;");
    EXPECT_EQ(r.size(), 0);
    txn1.commit();
    connection.disconnect();
  } catch (const std::exception &e) {
    EXPECT_TRUE(false);
  }
}

TEST_F(ConstraintStatementTest, DeleteCascadeRecursive) {
  try {
    pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
                                            port_, catalog::DEFAULT_DATABASE));

    pqxx::work txn1(connection);
    txn1.exec("CREATE TABLE TableA (id INT PRIMARY KEY, data INT UNIQUE, name TEXT UNIQUE);");
    txn1.exec(
        "CREATE TABLE TableB (id INT PRIMARY KEY, fk1 INT UNIQUE, FOREIGN KEY (fk1) references TableA(data) ON UPDATE "
        "CASCADE ON DELETE CASCADE);");
    txn1.exec(
        "CREATE TABLE TableC (id INT PRIMARY KEY, fk2 INT, FOREIGN KEY (fk2) references TableB(fk1) ON UPDATE CASCADE "
        "ON DELETE CASCADE);");
    txn1.exec(
        "CREATE TABLE TableD (id INT PRIMARY KEY, fk3 INT, FOREIGN KEY (fk3) references TableB(fk1) ON UPDATE CASCADE "
        "ON DELETE CASCADE);");
    txn1.exec("INSERT INTO TableA (id, data, name) VALUES (1, 2, 'abcd');");
    txn1.exec("INSERT INTO TableB VALUES (1, 2);");
    txn1.exec("INSERT INTO TableC VALUES (1, 2);");
    txn1.exec("INSERT INTO TableC VALUES (2, 2);");
    txn1.exec("INSERT INTO TableD VALUES (1, 2);");
    txn1.exec("INSERT INTO TableD VALUES (2, 2);");
    pqxx::result r = txn1.exec("SELECT * FROM TableA;");
    EXPECT_EQ(r.size(), 1);
    r = txn1.exec("SELECT * FROM TableB;");
    EXPECT_EQ(r.size(), 1);
    r = txn1.exec("SELECT * FROM TableC;");
    EXPECT_EQ(r.size(), 2);
    r = txn1.exec("SELECT * FROM TableD;");
    EXPECT_EQ(r.size(), 2);
    txn1.exec("DELETE FROM TableA WHERE id = 1;");
    r = txn1.exec("SELECT * FROM TableB;");
    EXPECT_EQ(r.size(), 0);
    r = txn1.exec("SELECT * FROM TableC;");
    EXPECT_EQ(r.size(), 0);
    r = txn1.exec("SELECT * FROM TableD;");
    EXPECT_EQ(r.size(), 0);
    txn1.commit();
    connection.disconnect();
  } catch (const std::exception &e) {
    EXPECT_TRUE(false);
  }
}
}  // namespace terrier::trafficcop