#include "catalog/index_handle.h"
#include "catalog/catalog.h"
#include "transaction/transaction_manager.h"
#include "type/transient_value_factory.h"
#include "type/transient_value_peeker.h"
#include "util/test_harness.h"
#include "util/transaction_test_util.h"

namespace terrier {
struct IndexHandleTest : public TerrierTest {
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

// Tests that we can correctly add new index entry to the pg_index and correctly get the index entry by the index oid.
// NOLINTNEXTLINE
TEST_F(IndexHandleTest, BasicCorrectnessTest) {
  // terrier has db_oid_t DEFAULT_DATABASE_OID
  const catalog::db_oid_t terrier_oid(catalog::DEFAULT_DATABASE_OID);
  auto db_handle = catalog_->GetDatabaseHandle();
  auto index_handle = db_handle.GetIndexTable(txn_, terrier_oid);

  storage::index::Index *index_ptr = nullptr;
  auto indexrelid = catalog::index_oid_t(catalog_->GetNextOid());
  auto indrelid = catalog::table_oid_t(catalog_->GetNextOid());
  int32_t indnatts = 123;
  int32_t indnkeyatts = 456;
  bool indisunique = true;
  bool indisprimary = false;
  bool indisvalid = true;
  bool indisready = false;
  bool indislive = true;
  bool indisblocking = false;
  index_handle.AddEntry(txn_, index_ptr, indexrelid, indrelid, indnatts, indnkeyatts, indisunique, indisprimary,
                        indisvalid, indisready, indislive, indisblocking);

  auto index_entry = index_handle.GetIndexEntry(txn_, indexrelid);

  EXPECT_NE(index_entry, nullptr);

  EXPECT_EQ(!index_entry->GetOid(), !indexrelid);
  EXPECT_EQ(
      reinterpret_cast<storage::index::Index *>(type::TransientValuePeeker::PeekBigInt(index_entry->GetColumn(0))),
      nullptr);
  EXPECT_EQ(!catalog::index_oid_t(type::TransientValuePeeker::PeekInteger(index_entry->GetColumn(1))), !indexrelid);
  EXPECT_EQ(!catalog::table_oid_t(type::TransientValuePeeker::PeekInteger(index_entry->GetColumn(2))), !indrelid);
  EXPECT_EQ(type::TransientValuePeeker::PeekInteger(index_entry->GetColumn(3)), indnatts);
  EXPECT_EQ(type::TransientValuePeeker::PeekInteger(index_entry->GetColumn(4)), indnkeyatts);
  EXPECT_EQ(type::TransientValuePeeker::PeekBoolean(index_entry->GetColumn(5)), indisunique);
  EXPECT_EQ(type::TransientValuePeeker::PeekBoolean(index_entry->GetColumn(6)), indisprimary);
  EXPECT_EQ(type::TransientValuePeeker::PeekBoolean(index_entry->GetColumn(7)), indisvalid);
  EXPECT_EQ(type::TransientValuePeeker::PeekBoolean(index_entry->GetColumn(8)), indisready);
  EXPECT_EQ(type::TransientValuePeeker::PeekBoolean(index_entry->GetColumn(9)), indislive);
  EXPECT_EQ(type::TransientValuePeeker::PeekBoolean(index_entry->GetColumn(10)), indisblocking);
}

// NOLINTNEXTLINE
TEST_F(IndexHandleTest, IndexHandleModificationTest) {
  // terrier has db_oid_t DEFAULT_DATABASE_OID
  const catalog::db_oid_t terrier_oid(catalog::DEFAULT_DATABASE_OID);
  auto db_handle = catalog_->GetDatabaseHandle();
  auto index_handle = db_handle.GetIndexTable(txn_, terrier_oid);
  storage::index::Index *index_ptr = nullptr;
  auto indexrelid = catalog::index_oid_t(catalog_->GetNextOid());
  auto indrelid = catalog::table_oid_t(catalog_->GetNextOid());
  int32_t indnatts = 123;
  int32_t indnkeyatts = 456;
  bool indisunique = true;
  bool indisprimary = false;
  bool indisvalid = false;
  bool indisready = false;
  bool indislive = true;
  bool indisblocking = false;
  index_handle.AddEntry(txn_, index_ptr, indexrelid, indrelid, indnatts, indnkeyatts, indisunique, indisprimary,
                        indisvalid, indisready, indislive, indisblocking);
  auto index_entry = index_handle.GetIndexEntry(txn_, indexrelid);

  EXPECT_NE(index_entry, nullptr);
  EXPECT_EQ(!index_entry->GetOid(), !indexrelid);
  EXPECT_EQ(reinterpret_cast<storage::index::Index *>(index_entry->GetBigIntColumn("indexptr")), nullptr);
  EXPECT_EQ(!catalog::index_oid_t(index_entry->GetIntegerColumn("indexrelid")), !indexrelid);
  EXPECT_EQ(!catalog::table_oid_t(index_entry->GetIntegerColumn("indrelid")), !indrelid);
  EXPECT_EQ(index_entry->GetIntegerColumn("indnatts"), indnatts);
  EXPECT_EQ(index_entry->GetIntegerColumn("indnkeyatts"), indnkeyatts);
  EXPECT_EQ(index_entry->GetBooleanColumn("indisunique"), indisunique);
  EXPECT_EQ(index_entry->GetBooleanColumn("indisprimary"), indisprimary);
  EXPECT_EQ(index_entry->GetBooleanColumn("indisvalid"), indisvalid);
  EXPECT_EQ(index_entry->GetBooleanColumn("indisready"), indisready);
  EXPECT_EQ(index_entry->GetBooleanColumn("indislive"), indislive);
  EXPECT_EQ(index_entry->GetBooleanColumn("indisblocking"), indisblocking);

  index_handle.SetEntryColumn(txn_, indexrelid, "indisvalid", type::TransientValueFactory::GetBoolean(true));
  index_entry = index_handle.GetIndexEntry(txn_, indexrelid);
  EXPECT_EQ(index_entry->GetBooleanColumn("indisvalid"), true);
}

// NOLINTNEXTLINE
TEST_F(IndexHandleTest, IndexHandleDeletionTest) {
  // terrier has db_oid_t DEFAULT_DATABASE_OID
  const catalog::db_oid_t terrier_oid(catalog::DEFAULT_DATABASE_OID);
  auto db_handle = catalog_->GetDatabaseHandle();
  auto index_handle = db_handle.GetIndexTable(txn_, terrier_oid);
  storage::index::Index *index_ptr = nullptr;
  auto indexrelid = catalog::index_oid_t(catalog_->GetNextOid());
  auto indrelid = catalog::table_oid_t(catalog_->GetNextOid());
  int32_t indnatts = 123;
  int32_t indnkeyatts = 456;
  bool indisunique = true;
  bool indisprimary = false;
  bool indisvalid = false;
  bool indisready = false;
  bool indislive = true;
  bool indisblocking = false;
  index_handle.AddEntry(txn_, index_ptr, indexrelid, indrelid, indnatts, indnkeyatts, indisunique, indisprimary,
                        indisvalid, indisready, indislive, indisblocking);
  auto index_entry = index_handle.GetIndexEntry(txn_, indexrelid);
  EXPECT_NE(index_entry, nullptr);
  index_handle.DeleteEntry(txn_, index_entry);
  index_entry = index_handle.GetIndexEntry(txn_, indexrelid);
  EXPECT_EQ(index_entry, nullptr);
}

}  // namespace terrier
