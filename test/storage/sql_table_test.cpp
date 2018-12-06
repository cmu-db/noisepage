#include "storage/sql_table.h"
#include <algorithm>
#include <random>
#include <vector>
#include "catalog/catalog.h"
#include "catalog/database_handle.h"
#include "transaction/transaction_manager.h"
#include "util/test_harness.h"
#include "util/transaction_test_util.h"
namespace terrier {

struct SqlTableTests : public TerrierTest {
  void SetUp() override {
    TerrierTest::SetUp();
    txn_manager_ = new transaction::TransactionManager(&buffer_pool_, true, LOGGING_DISABLED);
  }

  void TearDown() override {
    TerrierTest::TearDown();
    delete txn_manager_;
  }

  storage::RecordBufferSegmentPool buffer_pool_{100, 100};

  transaction::TransactionManager *txn_manager_;
};

// NOLINTNEXTLINE
TEST_F(SqlTableTests, SelectInsertTest) {
  auto txn1 = txn_manager_->BeginTransaction();

  // create a sql table
  storage::BlockStore block_store{100, 100};
  std::vector<catalog::Schema::Column> cols;
  cols.emplace_back("id", type::TypeId::INTEGER, false, catalog::col_oid_t(0));
  cols.emplace_back("datname", type::TypeId::INTEGER, false, catalog::col_oid_t(1));
  catalog::Schema schema(cols);
  storage::SqlTable test(&block_store, schema, catalog::table_oid_t(2));

  std::vector<catalog::col_oid_t> col_oids;
  col_oids.emplace_back(cols[0].GetOid());
  col_oids.emplace_back(cols[1].GetOid());
  auto row_pair = test.InitializerForProjectedRow(col_oids);

  // Insert a row
  byte *insert_buffer = common::AllocationUtil::AllocateAligned(row_pair.first.ProjectedRowSize());
  storage::ProjectedRow *insert = row_pair.first.InitializeRow(insert_buffer);
  // set the first value to be 100
  byte *first = insert->AccessForceNotNull(row_pair.second[col_oids[0]]);
  (*reinterpret_cast<uint32_t *>(first)) = 100;
  // set the second attribute to be 15271
  byte *second = insert->AccessForceNotNull(row_pair.second[col_oids[1]]);
  (*reinterpret_cast<uint32_t *>(second)) = 15721;
  auto slot = test.Insert(txn1, *insert);

  txn_manager_->Commit(txn1, TestCallbacks::EmptyCallback, nullptr);
  delete txn1;

  auto txn2 = txn_manager_->BeginTransaction();
  // read that row
  auto read_buffer = common::AllocationUtil::AllocateAligned(row_pair.first.ProjectedRowSize());
  storage::ProjectedRow *read = row_pair.first.InitializeRow(read_buffer);

  test.Select(txn2, slot, read);
  txn_manager_->Commit(txn2, TestCallbacks::EmptyCallback, nullptr);
  delete txn2;
  EXPECT_TRUE(true);
  delete[] insert_buffer;
  delete[] read_buffer;
  // TODO: check actual value
}

}  // namespace terrier
