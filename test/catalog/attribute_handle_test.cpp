#include <algorithm>
#include <random>
#include <vector>
#include "catalog/catalog.h"
#include "catalog/namespace_handle.h"
#include "transaction/transaction_manager.h"
#include "util/test_harness.h"
#include "util/transaction_test_util.h"
namespace terrier {

struct AttributeHandleTests : public TerrierTest {
  void SetUp() override {
    TerrierTest::SetUp();
    txn_manager_ = new transaction::TransactionManager(&buffer_pool_, true, LOGGING_DISABLED);

    txn_ = txn_manager_->BeginTransaction();
    catalog_ = new catalog::Catalog(txn_manager_, txn_);
  }

  void TearDown() override {
    txn_manager_->Commit(txn_, TestCallbacks::EmptyCallback, nullptr);
    TerrierTest::TearDown();
    delete catalog_;  // delete catalog first
    delete txn_manager_;
    delete txn_;
  }

  catalog::Catalog *catalog_;
  storage::RecordBufferSegmentPool buffer_pool_{100, 100};

  transaction::TransactionContext *txn_;
  transaction::TransactionManager *txn_manager_;
};

// Tests that we can get the default namespace and get the correct value from the corresponding row in pg_namespace
// NOLINTNEXTLINE
TEST_F(AttributeHandleTests, BasicCorrectnessTest) {
  // terrier has db_oid_t DEFAULT_DATABASE_OID
  const catalog::db_oid_t terrier_oid(catalog::DEFAULT_DATABASE_OID);
  auto db_handle = catalog_->GetDatabaseHandle();
  auto class_handle = db_handle.GetClassTable(txn_, terrier_oid);
  auto attribute_handle = db_handle.GetAttributeTable(txn_, terrier_oid);
  // lookup the table oid for pg_database
  auto class_entry = class_handle.GetClassEntry(txn_, "pg_database");
  const catalog::table_oid_t terrier_table_oid(!class_entry->GetOid());

  auto table_handle = db_handle.GetNamespaceTable(txn_, terrier_oid).GetTableHandle(txn_, "pg_catalog");

  // pg_database has columns: oid | datname
  auto attribute_entry_ptr = attribute_handle.GetAttributeEntry(txn_, terrier_table_oid, "oid");
  EXPECT_NE(attribute_entry_ptr, nullptr);

  // the oid should belongs to pg_database table
  uint32_t rel_id = type::TransientValuePeeker::PeekInteger(attribute_entry_ptr->GetColumn(1));
  EXPECT_EQ(rel_id, !table_handle.NameToOid(txn_, "pg_database"));

  attribute_entry_ptr = attribute_handle.GetAttributeEntry(txn_, terrier_table_oid, "attrlid");
  EXPECT_EQ(nullptr, attribute_entry_ptr);
}
}  // namespace terrier
