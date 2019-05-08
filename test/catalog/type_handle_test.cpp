#include "catalog/catalog.h"
#include "transaction/transaction_manager.h"
#include "type/transient_value_peeker.h"
#include "util/test_harness.h"
#include "util/transaction_test_util.h"
namespace terrier {

struct TypeHandleTest : public TerrierTest {
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

// NOLINTNEXTLINE
TEST_F(TypeHandleTest, BasicCorrectnessTest) {
  // terrier has db_oid_t DEFAULT_DATABASE_OID
  const catalog::db_oid_t terrier_oid(catalog::DEFAULT_DATABASE_OID);
  auto db_handle = catalog_->GetDatabaseHandle();
  auto type_handle = db_handle.GetTypeTable(txn_, terrier_oid);

  auto type_entry_ptr = type_handle.GetTypeEntry(txn_, "integer");
  EXPECT_NE(type_entry_ptr, nullptr);

  EXPECT_EQ(type::TransientValuePeeker::PeekInteger(type_entry_ptr->GetColumn(3)),
            type::TypeUtil::GetTypeSize(type::TypeId::INTEGER));
}

// NOLINTNEXTLINE
TEST_F(TypeHandleTest, OidLookupTest) {
  // terrier has db_oid_t DEFAULT_DATABASE_OID
  const catalog::db_oid_t terrier_oid(catalog::DEFAULT_DATABASE_OID);
  auto db_handle = catalog_->GetDatabaseHandle();
  auto type_handle = db_handle.GetTypeTable(txn_, terrier_oid);

  // check name to oid for a valid name
  auto UNUSED_ATTRIBUTE type_oid = type_handle.TypeToOid(txn_, "integer");

  EXPECT_THROW(type_handle.TypeToOid(txn_, "no_such_integer"), CatalogException);
}

}  // namespace terrier
