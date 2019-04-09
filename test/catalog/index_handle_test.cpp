#include "catalog/catalog.h"
#include "transaction/transaction_manager.h"
#include "type/transient_value_peeker.h"
#include "util/test_harness.h"
#include "util/transaction_test_util.h"
namespace terrier {
struct IndexHandleTest : public TerrierTest {
  void SetUp() override {
    TerrierTest::SetUp();
    txn_manager_ = new transaction::TransactionManager(&buffer_pool_, true, LOGGING_DISABLED);

    catalog_ = new catalog::Catalog(txn_manager_);
    txn_ = txn_manager_->BeginTransaction();
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

// Tests that we can correctly add new index entry to the pg_index and correctly get the index entry by the index oid.
// NOLINTNEXTLINE
TEST_F(IndexHandleTest, BasicCorrectnessTest) {
  // terrier has db_oid_t DEFAULT_DATABASE_OID
  const catalog::db_oid_t terrier_oid(catalog::DEFAULT_DATABASE_OID);
  auto db_handle = catalog_->GetDatabaseHandle();
  auto index_handle = db_handle.GetIndexHandle(txn_, terrier_oid);

  auto indexrelid = catalog::index_oid_t(catalog_->GetNextOid());
  auto indrelid = catalog::table_oid_t(catalog_->GetNextOid());
  int32_t indnatts = 123;
  int32_t indnkeyatts = 456;
  bool indisunique = true;
  bool indisprimary = false;
  bool indisvalid = true;
  bool indisready = false;
  bool indislive = true;
  index_handle.AddEntry(txn_, indexrelid, indrelid, indnatts, indnkeyatts, indisunique, indisprimary, indisvalid,
                        indisready, indislive);

  auto index_entry = index_handle.GetIndexEntry(txn_, indexrelid);

  EXPECT_NE(index_entry, nullptr);
  EXPECT_EQ(!index_entry->GetIndexOid(), !indexrelid);
  EXPECT_EQ(!catalog::index_oid_t(type::TransientValuePeeker::PeekInteger(index_entry->GetColumn(0))), !indexrelid);
  EXPECT_EQ(!catalog::table_oid_t(type::TransientValuePeeker::PeekInteger(index_entry->GetColumn(1))), !indrelid);
  EXPECT_EQ(type::TransientValuePeeker::PeekInteger(index_entry->GetColumn(2)), indnatts);
  EXPECT_EQ(type::TransientValuePeeker::PeekInteger(index_entry->GetColumn(3)), indnkeyatts);
  EXPECT_EQ(type::TransientValuePeeker::PeekBoolean(index_entry->GetColumn(4)), indisunique);
  EXPECT_EQ(type::TransientValuePeeker::PeekBoolean(index_entry->GetColumn(5)), indisprimary);
  EXPECT_EQ(type::TransientValuePeeker::PeekBoolean(index_entry->GetColumn(6)), indisvalid);
  EXPECT_EQ(type::TransientValuePeeker::PeekBoolean(index_entry->GetColumn(7)), indisready);
  EXPECT_EQ(type::TransientValuePeeker::PeekBoolean(index_entry->GetColumn(8)), indislive);
}

}  // namespace terrier
