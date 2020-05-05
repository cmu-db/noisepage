#include <array>
#include <memory>
#include <vector>

#include "execution/sql/cte_scan_iterator.h"
#include "execution/sql/storage_interface.h"

#include "catalog/catalog_defs.h"
#include "execution/sql/index_iterator.h"
#include "execution/sql/table_vector_iterator.h"
#include "execution/sql_test.h"

namespace terrier::execution::sql::test {
class CTEScanTest : public SqlBasedTest {
  void SetUp() override {
    // Create the test tables
    SqlBasedTest::SetUp();
    exec_ctx_ = MakeExecCtx();
    GenerateTestTables(exec_ctx_.get());
  }

 protected:
  /**
   * Execution context to use for the test
   */
  std::unique_ptr<exec::ExecutionContext> exec_ctx_;
};

TEST_F(CTEScanTest, DISABLED_CTEInitTest) {
  // Check the mapping of col_oids to the col_ids in the constructed table

  uint32_t cte_table_col_type[4] = {5, 4, 3, 9};  // {BIGINT, INTEGER, SMALLINT, VARCHAR}

  auto cte_scan = new terrier::execution::sql::CteScanIterator(exec_ctx_.get(), cte_table_col_type, 4);

  auto cte_table = cte_scan->GetTable();

  std::unordered_map<catalog::col_oid_t, uint16_t> oid_to_iid;
  std::vector<catalog::col_oid_t> col_oids2;
  for (int i = 1; i <= 4; i++) {
    col_oids2.push_back(static_cast<catalog::col_oid_t>(i));
  }
  oid_to_iid[static_cast<catalog::col_oid_t>(1)] = 1;
  oid_to_iid[static_cast<catalog::col_oid_t>(2)] = 2;
  oid_to_iid[static_cast<catalog::col_oid_t>(3)] = 3;
  oid_to_iid[static_cast<catalog::col_oid_t>(4)] = 0;
  /* Expected Result:
   * 1 = 1
   * 2 = 2
   * 3 = 3
   * 4 = 0
   * */
  auto proj_map = cte_table->ProjectionMapForOids(col_oids2);
  auto map_iterator = proj_map.begin();
  while (map_iterator != proj_map.end()) {
    EXPECT_EQ(map_iterator->second, oid_to_iid[map_iterator->first]);
    map_iterator++;
  }
}

TEST_F(CTEScanTest, CTEInsertTest) {
  // INSERT INTO cte_table SELECT colA FROM test_1 WHERE colA BETWEEN 495 and 505.

  // initialize the test_1 and the index on the table
  auto table_oid1 = exec_ctx_->GetAccessor()->GetTableOid(NSOid(), "test_1");
  auto index_oid1 = exec_ctx_->GetAccessor()->GetIndexOid(NSOid(), "index_1");

  // Select colA only
  std::array<uint32_t, 1> col_oids{1};

  // The index iterator gives us the slots to update.
  IndexIterator index_iter1{
      exec_ctx_.get(), 1, !table_oid1, !index_oid1, col_oids.data(), static_cast<uint32_t>(col_oids.size())};
  index_iter1.Init();

  // Create cte_table
  uint32_t cte_table_col_type[1] = {4};  // {INTEGER}

  auto cte_scan = new terrier::execution::sql::CteScanIterator(exec_ctx_.get(), cte_table_col_type, 1);

  auto cte_table = cte_scan->GetTable();

  // Find the rows with colA BETWEEN 495 AND 505. SELECT query
  int32_t lo_match = 495;
  int32_t hi_match = 505;
  auto *const lo_pr(index_iter1.LoPR());
  auto *const hi_pr(index_iter1.HiPR());
  lo_pr->Set<int32_t, false>(0, lo_match, false);
  hi_pr->Set<int32_t, false>(0, hi_match, false);
  index_iter1.ScanAscending(storage::index::ScanType::Closed, 0);
  std::vector<uint32_t> inserted_vals;
  while (index_iter1.Advance()) {
    // Get tuple at the current slot
    auto *const table_pr(index_iter1.TablePR());
    auto *val_a = table_pr->Get<int32_t, false>(0, nullptr);
    inserted_vals.emplace_back(*val_a);
    // Insert into table
    auto *const insert_pr(cte_scan->GetInsertTempTablePR());
    insert_pr->Set<int32_t, false>(0, *val_a, false);
    cte_scan->TableInsert();
  }

  // Try to fetch the inserted values.
  // TODO(Gautam): Create our own TableVectorIterator that does not check in the catalog
  TableVectorIterator table_iter(exec_ctx_.get(), !static_cast<catalog::table_oid_t>(999), col_oids.data(),
                                 static_cast<uint32_t>(col_oids.size()));
  table_iter.InitTempTable(common::ManagedPointer(cte_table));
  ProjectedColumnsIterator *pci = table_iter.GetProjectedColumnsIterator();
  uint32_t num_tuples = 0;
  while (table_iter.Advance()) {
    for (; pci->HasNext(); pci->Advance()) {
      auto *val_a = pci->Get<int32_t, false>(0, nullptr);
      ASSERT_EQ(*val_a, inserted_vals[num_tuples]);
      num_tuples++;
    }
    pci->Reset();
  }
  EXPECT_EQ(num_tuples, (hi_match - lo_match) + 1);
  delete cte_scan;
}

TEST_F(CTEScanTest, CTEInsertScanTest) {
  // INSERT INTO cte_table SELECT colA FROM test_1 WHERE colA BETWEEN 495 and 505.

  // initialize the test_1 and the index on the table
  auto table_oid1 = exec_ctx_->GetAccessor()->GetTableOid(NSOid(), "test_1");
  auto index_oid1 = exec_ctx_->GetAccessor()->GetIndexOid(NSOid(), "index_1");

  // Select colA only
  std::array<uint32_t, 1> col_oids{1};

  // The index iterator gives us the slots to update.
  IndexIterator index_iter1{
      exec_ctx_.get(), 1, !table_oid1, !index_oid1, col_oids.data(), static_cast<uint32_t>(col_oids.size())};
  index_iter1.Init();

  // Create cte_table
  uint32_t cte_table_col_type[1] = {4};  // {INTEGER}

  auto cte_scan = new terrier::execution::sql::CteScanIterator(exec_ctx_.get(), cte_table_col_type, 1);

  auto cte_table = cte_scan->GetTable();

  // Find the rows with colA BETWEEN 495 AND 505. SELECT query
  int32_t lo_match = 495;
  int32_t hi_match = 505;
  auto *const lo_pr(index_iter1.LoPR());
  auto *const hi_pr(index_iter1.HiPR());
  lo_pr->Set<int32_t, false>(0, lo_match, false);
  hi_pr->Set<int32_t, false>(0, hi_match, false);
  index_iter1.ScanAscending(storage::index::ScanType::Closed, 0);
  std::vector<uint32_t> inserted_vals;
  while (index_iter1.Advance()) {
    // Get tuple at the current slot
    auto *const table_pr(index_iter1.TablePR());
    auto *val_a = table_pr->Get<int32_t, false>(0, nullptr);
    inserted_vals.emplace_back(*val_a);
    // Insert into table
    auto *const insert_pr(cte_scan->GetInsertTempTablePR());
    insert_pr->Set<int32_t, false>(0, *val_a, false);
    cte_scan->TableInsert();
  }

  // Try to fetch the inserted values.
  // TODO(Gautam): Create our own TableVectorIterator that does not check in the catalog
  auto table_iter = new TableVectorIterator(exec_ctx_.get(), !(cte_scan->GetTableOid()), col_oids.data(),
                                            static_cast<uint32_t>(col_oids.size()));
  table_iter->InitTempTable(common::ManagedPointer(cte_table));
  ProjectedColumnsIterator *pci = table_iter->GetProjectedColumnsIterator();
  uint32_t num_tuples = 0;
  while (table_iter->Advance()) {
    for (; pci->HasNext(); pci->Advance()) {
      auto *val_a = pci->Get<int32_t, false>(0, nullptr);
      ASSERT_EQ(*val_a, inserted_vals[num_tuples]);
      num_tuples++;
    }
    pci->Reset();
  }
  delete table_iter;
  EXPECT_EQ(num_tuples, (hi_match - lo_match) + 1);
  delete cte_scan;
}

}  // namespace terrier::execution::sql::test
