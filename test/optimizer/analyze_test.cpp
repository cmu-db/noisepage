#include <limits>
#include <map>
#include <memory>
#include <vector>

#include "main/db_main.h"
#include "optimizer/analyze.h"
#include "optimizer/statistics/column_stats.h"
#include "storage/garbage_collector_thread.h"
#include "storage/projected_row.h"
#include "storage/sql_table.h"
#include "test_util/catalog_test_util.h"
#include "test_util/test_harness.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"
#include "type/type_id.h"
#include "type/type_util.h"

namespace terrier::storage::index {

class AnalyzeTests : public TerrierTest {
 private:
  catalog::IndexSchema unique_schema_;
  catalog::IndexSchema default_schema_;

 public:
  std::unique_ptr<DBMain> db_main_;
  common::ManagedPointer<transaction::TransactionManager> txn_manager_;

  // SqlTable
  catalog::Schema table_schema_;
  storage::SqlTable *sql_table_;
  storage::ProjectedRowInitializer tuple_initializer_ =
      storage::ProjectedRowInitializer::Create(std::vector<uint16_t>{1}, std::vector<uint16_t>{1});

 protected:
  void SetUp() override {
    db_main_ = terrier::DBMain::Builder().SetUseGC(true).SetUseGCThread(true).SetRecordBufferSegmentSize(1e6).Build();
    txn_manager_ = db_main_->GetTransactionLayer()->GetTransactionManager();

    auto col = catalog::Schema::Column(
        "attribute", type::TypeId::INTEGER, false,
        parser::ConstantValueExpression(type::TransientValueFactory::GetNull(type::TypeId::INTEGER)));
    StorageTestUtil::ForceOid(&(col), catalog::col_oid_t(1));
    table_schema_ = catalog::Schema({col});
    sql_table_ = new storage::SqlTable(db_main_->GetStorageLayer()->GetBlockStore().Get(), table_schema_);
    tuple_initializer_ = sql_table_->InitializerForProjectedRow({catalog::col_oid_t(1)});

    std::vector<catalog::IndexSchema::Column> keycols;
    keycols.emplace_back("", type::TypeId::INTEGER, false,
                         parser::ColumnValueExpression(CatalogTestUtil::TEST_DB_OID, CatalogTestUtil::TEST_TABLE_OID,
                                                       catalog::col_oid_t(1)));
    StorageTestUtil::ForceOid(&(keycols[0]), catalog::indexkeycol_oid_t(1));
  }
  void TearDown() override {
    db_main_->GetTransactionLayer()->GetDeferredActionManager()->RegisterDeferredAction([=]() { delete sql_table_; });
  }
};

/**
 * This test creates multiple worker threads that all try to insert [0,num_inserts) as tuples in the table and into the
 * primary key index. At completion of the workload, only num_inserts_ txns should have committed with visible versions
 * in the index and table.
 */
// NOLINTNEXTLINE
TEST_F(AnalyzeTests, SingleColumnTest) {
  const uint32_t num_inserts = 100;

  std::vector<catalog::col_oid_t> col_oids;
  col_oids.reserve(table_schema_.GetColumns().size());
  for (const auto &col : table_schema_.GetColumns()) {
    col_oids.push_back(col.Oid());
  }
  ProjectionMap projection_list_indices = sql_table_->ProjectionMapForOids(col_oids);

  for (uint32_t i = 0; i < num_inserts; i++) {
    for (uint32_t j = 0; j < i; j++) {
      auto *const insert_txn = txn_manager_->BeginTransaction();
      auto *const insert_redo =
          insert_txn->StageWrite(CatalogTestUtil::TEST_DB_OID, CatalogTestUtil::TEST_TABLE_OID, tuple_initializer_);
      auto *const insert_tuple = insert_redo->Delta();
      *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(projection_list_indices[catalog::col_oid_t(1)])) =
          static_cast<int32_t>(i) * 2;
      sql_table_->Insert(insert_txn, insert_redo);
      txn_manager_->Commit(insert_txn, transaction::TransactionUtil::EmptyCallback, nullptr);
    }
  }

  auto *const scan_txn = txn_manager_->BeginTransaction();

  auto result_analyze =
      Analyze(CatalogTestUtil::TEST_DB_OID, CatalogTestUtil::TEST_TABLE_OID, table_schema_, sql_table_, scan_txn, 10);

  txn_manager_->Commit(scan_txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  std::vector<double> result_most_common_values{198, 196, 194, 192, 190, 188, 186, 184, 182, 180};
  std::vector<double> result_most_common_frequencies{99, 98, 97, 96, 95, 94, 93, 92, 91, 90};
  std::vector<double> result_histogram_bounds{2, 198};

  EXPECT_EQ(result_analyze[0].GetDatabaseID(), CatalogTestUtil::TEST_DB_OID);
  EXPECT_EQ(result_analyze[0].GetTableID(), CatalogTestUtil::TEST_TABLE_OID);
  EXPECT_EQ(result_analyze[0].GetColumnID(), catalog::col_oid_t(1));
  EXPECT_EQ(result_analyze[0].GetNumRows(), 99 * 50); /* n*(n+1) / 2*/
  EXPECT_EQ(result_analyze[0].GetCardinality(), 99);
  EXPECT_EQ(result_analyze[0].GetFracNull(), (double)0);
  EXPECT_TRUE(result_analyze[0].BaseTable());

  for (unsigned i = 0; i < 10; i++) {
    EXPECT_EQ(result_analyze[0].GetCommonVals()[i], result_most_common_values[i]);
  }
  for (unsigned i = 0; i < 10; i++) {
    EXPECT_EQ(result_analyze[0].GetCommonFreqs()[i], result_most_common_frequencies[i]);
  }
  for (unsigned i = 0; i < 2; i++) {
    EXPECT_EQ(result_analyze[0].GetHistogramBounds()[i], result_histogram_bounds[i]);
  }
}

}  // namespace terrier::storage::index
