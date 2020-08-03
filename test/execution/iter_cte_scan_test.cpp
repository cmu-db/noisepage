#include <array>
#include <memory>
#include <vector>

#include "catalog/catalog_defs.h"
#include "execution/sql/index_iterator.h"
#include "execution/sql/iter_cte_scan_iterator.h"
#include "execution/sql/storage_interface.h"
#include "execution/sql/table_vector_iterator.h"
#include "execution/sql_test.h"
#include "execution/util/timer.h"

namespace terrier::execution::sql::test {

class IterCTEScanTest : public SqlBasedTest {
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

TEST_F(IterCTEScanTest, IterCTEEmptyAccumulateTest) {
  // Test that Accumulate() returns false on empty

  // Create cte_table
  std::array<uint32_t, 1> col_oids{1};
  uint32_t cte_table_col_type[1] = {4};  // {INTEGER}

  // auto cte_scan = new terrier::execution::sql::IterCteScanIterator(exec_ctx_.get(), cte_table_col_type, 1);
  terrier::execution::sql::IterCteScanIterator cte_scan{exec_ctx_.get(), cte_table_col_type, 1};
  EXPECT_FALSE(cte_scan.Accumulate());

  TableVectorIterator seq_iter{exec_ctx_.get(), !static_cast<catalog::table_oid_t>(999), col_oids.data(),
                               static_cast<uint32_t>(col_oids.size())};
  seq_iter.InitTempTable(common::ManagedPointer(cte_scan.GetReadTable()));
  auto *pci = seq_iter.GetProjectedColumnsIterator();
  auto count = 0; // The number of records found

  while (seq_iter.Advance()) {
    for (; pci->HasNext(); pci->Advance()) {
      // Increment counter
      count++;
    }
    pci->Reset();
  }
  EXPECT_EQ(count, 0);
}

TEST_F(IterCTEScanTest, IterCTESingleInsertTest) {
  // Simple insert into cte_table
  // INSERT INTO cte_table SELECT colA FROM test_1 WHERE colA BETWEEN 1 and 20.

  // Initialize test table + index
  auto table_oid = exec_ctx_->GetAccessor()->GetTableOid(NSOid(), "test_1");
  auto index_oid = exec_ctx_->GetAccessor()->GetIndexOid(NSOid(), "index_1");

  // Just one column
  std::array<uint32_t, 1> col_oids{1};

  // The index iterator gives us the slots to update.
  IndexIterator index_iter{
      exec_ctx_.get(), 1, !table_oid, !index_oid, col_oids.data(), static_cast<uint32_t>(col_oids.size())};
  index_iter.Init();

  // Create cte_table
  uint32_t cte_table_col_type[1] = {4};  // {INTEGER}

  // auto cte_scan = new terrier::execution::sql::IterCteScanIterator(exec_ctx_.get(), cte_table_col_type, 1);
  terrier::execution::sql::IterCteScanIterator cte_scan{exec_ctx_.get(), cte_table_col_type, 1};

  // Find the rows with colA BETWEEN 1 AND 20. SELECT query
  int32_t lo_match = 1;
  int32_t hi_match = 20;
  auto *const lo_pr(index_iter.LoPR());
  auto *const hi_pr(index_iter.HiPR());
  lo_pr->Set<int32_t, false>(0, lo_match, false);
  hi_pr->Set<int32_t, false>(0, hi_match, false);
  index_iter.ScanAscending(storage::index::ScanType::Closed, 0);
  std::vector<uint32_t> inserted_vals;

  while (index_iter.Advance()) {
    // Get one item from test table
    auto *const cur_pr = index_iter.PR();
    auto *cur_val = cur_pr->Get<int32_t, false>(0, nullptr);
    inserted_vals.push_back(*cur_val);

    // Insert into cte_table
    auto *const insert_pr = cte_scan.GetInsertTempTablePR();
    insert_pr->Set<int32_t, false>(0, *cur_val, false);
    cte_scan.TableInsert();
  }
  EXPECT_TRUE(cte_scan.Accumulate());

  TableVectorIterator seq_iter{exec_ctx_.get(), !static_cast<catalog::table_oid_t>(999), col_oids.data(),
                               static_cast<uint32_t>(col_oids.size())};
  seq_iter.InitTempTable(common::ManagedPointer(cte_scan.GetReadTable()));
  auto *pci = seq_iter.GetProjectedColumnsIterator();
  auto count = 0;

  while (seq_iter.Advance()) {
    for (; pci->HasNext(); pci->Advance()) {
      // Get one element from cte_table
      auto *cur_val = pci->Get<int32_t, false>(0, nullptr);
      EXPECT_EQ(*cur_val, inserted_vals[count]);
      count++;
    }
    pci->Reset();
  }
  EXPECT_EQ(count, inserted_vals.size());
}
TEST_F(IterCTEScanTest, IterCTEWriteTableTest) {
  // Simple insert into cte_table
  // INSERT INTO cte_table SELECT colA FROM test_1 WHERE colA BETWEEN 1 and 20.

  // Initialize test table + index
  auto table_oid = exec_ctx_->GetAccessor()->GetTableOid(NSOid(), "test_1");
  auto index_oid = exec_ctx_->GetAccessor()->GetIndexOid(NSOid(), "index_1");

  // Just one column
  std::array<uint32_t, 1> col_oids{1};

  // The index iterator gives us the slots to update.
  IndexIterator index_iter{
      exec_ctx_.get(), 1, !table_oid, !index_oid, col_oids.data(), static_cast<uint32_t>(col_oids.size())};
  index_iter.Init();

  // Create cte_table
  uint32_t cte_table_col_type[1] = {4};  // {INTEGER}

  // auto cte_scan = new terrier::execution::sql::IterCteScanIterator(exec_ctx_.get(), cte_table_col_type, 1);
  terrier::execution::sql::IterCteScanIterator cte_scan{exec_ctx_.get(), cte_table_col_type, 1};

  // Find the rows with colA BETWEEN 1 AND 20. SELECT query
  int32_t lo_match = 1;
  int32_t hi_match = 20;
  auto *const lo_pr(index_iter.LoPR());
  auto *const hi_pr(index_iter.HiPR());
  lo_pr->Set<int32_t, false>(0, lo_match, false);
  hi_pr->Set<int32_t, false>(0, hi_match, false);
  index_iter.ScanAscending(storage::index::ScanType::Closed, 0);
  std::vector<uint32_t> inserted_vals;

  while (index_iter.Advance()) {
    // Get one item from test table
    auto *const cur_pr = index_iter.PR();
    auto *cur_val = cur_pr->Get<int32_t, false>(0, nullptr);
    inserted_vals.push_back(*cur_val);

    // Insert into cte_table
    auto *const insert_pr = cte_scan.GetInsertTempTablePR();
    insert_pr->Set<int32_t, false>(0, *cur_val, false);
    cte_scan.TableInsert();
  }

  TableVectorIterator seq_iter{exec_ctx_.get(), !static_cast<catalog::table_oid_t>(999), col_oids.data(),
                               static_cast<uint32_t>(col_oids.size())};
  seq_iter.InitTempTable(common::ManagedPointer(cte_scan.GetWriteTable()));
  auto *pci = seq_iter.GetProjectedColumnsIterator();
  auto count = 0;

  while (seq_iter.Advance()) {
    for (; pci->HasNext(); pci->Advance()) {
      // Get one element from cte_table
      auto *cur_val = pci->Get<int32_t, false>(0, nullptr);
      EXPECT_EQ(*cur_val, inserted_vals[count]);
      count++;
    }
    pci->Reset();
  }
  EXPECT_EQ(count, inserted_vals.size());
}

TEST_F(IterCTEScanTest, IterCTEDoubleAccumulateTest) {
  // Same insertion as the previous test, but accumulate TWICE instead of once.
  // Since there are no insertions between the two accumulates, tne second accumulate should do nothing.

  // Initialize test table + index
  auto table_oid = exec_ctx_->GetAccessor()->GetTableOid(NSOid(), "test_1");
  auto index_oid = exec_ctx_->GetAccessor()->GetIndexOid(NSOid(), "index_1");

  // Just one column
  std::array<uint32_t, 1> col_oids{1};

  // The index iterator gives us the slots to update.
  IndexIterator index_iter{
      exec_ctx_.get(), 1, !table_oid, !index_oid, col_oids.data(), static_cast<uint32_t>(col_oids.size())};
  index_iter.Init();

  // Create cte_table
  uint32_t cte_table_col_type[1] = {4};  // {INTEGER}

  // auto cte_scan = new terrier::execution::sql::IterCteScanIterator(exec_ctx_.get(), cte_table_col_type, 1);
  terrier::execution::sql::IterCteScanIterator cte_scan{exec_ctx_.get(), cte_table_col_type, 1};

  // Find the rows with colA BETWEEN 1 AND 20. SELECT query
  int32_t lo_match = 1;
  int32_t hi_match = 20;
  auto *const lo_pr(index_iter.LoPR());
  auto *const hi_pr(index_iter.HiPR());
  lo_pr->Set<int32_t, false>(0, lo_match, false);
  hi_pr->Set<int32_t, false>(0, hi_match, false);
  index_iter.ScanAscending(storage::index::ScanType::Closed, 0);
  std::vector<uint32_t> inserted_vals;

  while (index_iter.Advance()) {
    // Get one item from test table
    auto *const cur_pr = index_iter.PR();
    auto *cur_val = cur_pr->Get<int32_t, false>(0, nullptr);
    inserted_vals.push_back(*cur_val);

    // Insert into cte_table
    auto *const insert_pr = cte_scan.GetInsertTempTablePR();
    insert_pr->Set<int32_t, false>(0, *cur_val, false);
    cte_scan.TableInsert();
  }
  EXPECT_TRUE(cte_scan.Accumulate());
  EXPECT_FALSE(cte_scan.Accumulate());

  TableVectorIterator seq_iter{exec_ctx_.get(), !static_cast<catalog::table_oid_t>(999), col_oids.data(),
                               static_cast<uint32_t>(col_oids.size())};
  seq_iter.InitTempTable(common::ManagedPointer(cte_scan.GetReadTable()));
  auto *pci = seq_iter.GetProjectedColumnsIterator();
  auto count = 0;

  while (seq_iter.Advance()) {
    for (; pci->HasNext(); pci->Advance()) {
      // Get one element from cte_table
      auto *cur_val = pci->Get<int32_t, false>(0, nullptr);
      EXPECT_EQ(*cur_val, inserted_vals[count]);
      count++;
    }
    pci->Reset();
  }
  EXPECT_EQ(count, inserted_vals.size());
}

TEST_F(IterCTEScanTest, IterCTEMultipleInsertTest) {
  // Multiple iteration insert into cte_table
  //
  // SELECT colA FROM test_1 where colA BETWEEN 1 AND 20
  // Accumulate()
  // SELECT colA FROM test_1 where colA BETWEEN 21 AND 40
  //
  // cte_table should only contain values between 21 and 40

  // Initialize test table + index
  auto table_oid = exec_ctx_->GetAccessor()->GetTableOid(NSOid(), "test_1");
  auto index_oid = exec_ctx_->GetAccessor()->GetIndexOid(NSOid(), "index_1");

  // Just one column
  std::array<uint32_t, 1> col_oids{1};

  // The index iterator gives us the slots to update.
  IndexIterator index_iter{
      exec_ctx_.get(), 1, !table_oid, !index_oid, col_oids.data(), static_cast<uint32_t>(col_oids.size())};
  index_iter.Init();

  // Create cte_table
  uint32_t cte_table_col_type[1] = {4};  // {INTEGER}

  // auto cte_scan = new terrier::execution::sql::IterCteScanIterator(exec_ctx_.get(), cte_table_col_type, 1);
  terrier::execution::sql::IterCteScanIterator cte_scan{exec_ctx_.get(), cte_table_col_type, 1};

  // Find the rows with colA BETWEEN 1 AND 20. SELECT query
  int32_t first_lo_match = 1;
  int32_t first_hi_match = 20;
  auto *const first_lo_pr(index_iter.LoPR());
  auto *const first_hi_pr(index_iter.HiPR());
  first_lo_pr->Set<int32_t, false>(0, first_lo_match, false);
  first_hi_pr->Set<int32_t, false>(0, first_hi_match, false);
  index_iter.ScanAscending(storage::index::ScanType::Closed, 0);

  while (index_iter.Advance()) {
    // Get one item from test table
    auto *const cur_pr = index_iter.PR();
    auto *cur_val = cur_pr->Get<int32_t, false>(0, nullptr);

    // Insert into cte_table
    auto *const insert_pr = cte_scan.GetInsertTempTablePR();
    insert_pr->Set<int32_t, false>(0, *cur_val, false);
    cte_scan.TableInsert();
  }
  EXPECT_TRUE(cte_scan.Accumulate());

  // Find the rows with colA BETWEEN 1 AND 20. SELECT query
  int32_t lo_match = 21;
  int32_t hi_match = 40;
  auto *const lo_pr(index_iter.LoPR());
  auto *const hi_pr(index_iter.HiPR());
  lo_pr->Set<int32_t, false>(0, lo_match, false);
  hi_pr->Set<int32_t, false>(0, hi_match, false);
  index_iter.ScanAscending(storage::index::ScanType::Closed, 0);
  std::vector<uint32_t> inserted_vals;

  while (index_iter.Advance()) {
    // Get one item from test table
    auto *const cur_pr = index_iter.PR();
    auto *cur_val = cur_pr->Get<int32_t, false>(0, nullptr);
    inserted_vals.push_back(*cur_val);

    // Insert into cte_table
    auto *const insert_pr = cte_scan.GetInsertTempTablePR();
    insert_pr->Set<int32_t, false>(0, *cur_val, false);
    cte_scan.TableInsert();
  }
  EXPECT_TRUE(cte_scan.Accumulate());

  TableVectorIterator seq_iter{exec_ctx_.get(), !static_cast<catalog::table_oid_t>(999), col_oids.data(),
                               static_cast<uint32_t>(col_oids.size())};
  seq_iter.InitTempTable(common::ManagedPointer(cte_scan.GetReadTable()));
  auto *pci = seq_iter.GetProjectedColumnsIterator();
  auto count = 0;

  while (seq_iter.Advance()) {
    for (; pci->HasNext(); pci->Advance()) {
      // Get one element from cte_table
      auto *cur_val = pci->Get<int32_t, false>(0, nullptr);
      EXPECT_EQ(*cur_val, inserted_vals[count]);
      count++;
    }
    pci->Reset();
  }
  EXPECT_EQ(count, inserted_vals.size());
}

}  // namespace terrier::execution::sql::test
