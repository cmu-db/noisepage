#include "execution/sql/index_iterator.h"

#include <array>
#include <memory>

#include "catalog/catalog_defs.h"
#include "execution/sql/table_vector_iterator.h"
#include "execution/sql_test.h"
#include "execution/util/timer.h"

namespace noisepage::execution::sql::test {

class IndexIteratorTest : public SqlBasedTest {
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

// NOLINTNEXTLINE
TEST_F(IndexIteratorTest, SimpleIndexIteratorTest) {
  //
  // Access table data through the index
  //

  auto table_oid = exec_ctx_->GetAccessor()->GetTableOid(NSOid(), "test_1");
  auto sql_table = exec_ctx_->GetAccessor()->GetTable(table_oid);
  auto index_oid = exec_ctx_->GetAccessor()->GetIndexOid(NSOid(), "index_1");
  std::array<uint32_t, 1> col_oids{1};
  TableVectorIterator table_iter(exec_ctx_.get(), table_oid.UnderlyingValue(), col_oids.data(),
                                 static_cast<uint32_t>(col_oids.size()));
  IndexIterator index_iter{exec_ctx_.get(),
                           1,
                           table_oid.UnderlyingValue(),
                           index_oid.UnderlyingValue(),
                           col_oids.data(),
                           static_cast<uint32_t>(col_oids.size())};
  table_iter.Init();
  index_iter.Init();
  VectorProjectionIterator *vpi = table_iter.GetVectorProjectionIterator();

  // Iterate through the table.
  while (table_iter.Advance()) {
    for (; vpi->HasNext(); vpi->Advance()) {
      auto *key = vpi->GetValue<int32_t, false>(0, nullptr);
      // Check that the key can be recovered through the index
      auto *const index_pr(index_iter.PR());
      index_pr->Set<int32_t, false>(0, *key, false);
      index_iter.ScanKey();
      // One entry should be found
      ASSERT_TRUE(index_iter.Advance());
      // Get directly from iterator
      auto *const table_pr(index_iter.TablePR());
      auto *val = table_pr->Get<int32_t, false>(0, nullptr);
      ASSERT_EQ(*key, *val);
      val = nullptr;
      // Get indirectly from tuple slot
      storage::TupleSlot slot(index_iter.CurrentSlot());
      ASSERT_TRUE(sql_table->Select(exec_ctx_->GetTxn(), slot, index_iter.TablePR()));
      val = table_pr->Get<int32_t, false>(0, nullptr);
      ASSERT_EQ(*key, *val);

      // Check that there are no more entries.
      ASSERT_FALSE(index_iter.Advance());
    }
    vpi->Reset();
  }
}

// NOLINTNEXTLINE
TEST_F(IndexIteratorTest, SimpleAscendingScanTest) {
  //
  // Perform an ascending scan
  //

  auto table_oid = exec_ctx_->GetAccessor()->GetTableOid(NSOid(), "test_1");
  auto index_oid = exec_ctx_->GetAccessor()->GetIndexOid(NSOid(), "index_1");
  std::array<uint32_t, 1> col_oids{1};
  IndexIterator index_iter{exec_ctx_.get(),
                           1,
                           table_oid.UnderlyingValue(),
                           index_oid.UnderlyingValue(),
                           col_oids.data(),
                           static_cast<uint32_t>(col_oids.size())};
  index_iter.Init();
  auto *const lo_pr(index_iter.LoPR());
  auto *const hi_pr(index_iter.HiPR());
  lo_pr->Set<int32_t, false>(0, 495, false);
  hi_pr->Set<int32_t, false>(0, 505, false);
  index_iter.ScanAscending(storage::index::ScanType::Closed, 0);
  int32_t curr_match = 495;
  uint32_t num_matches = 0;
  while (index_iter.Advance()) {
    auto *const table_pr(index_iter.TablePR());
    auto *val = table_pr->Get<int32_t, false>(0, nullptr);
    EXPECT_EQ(*val, curr_match);
    curr_match++;
    num_matches++;
  }
  ASSERT_EQ(num_matches, 11);
}

// NOLINTNEXTLINE
TEST_F(IndexIteratorTest, SimpleLimitAscendingScanTest) {
  //
  // Perform an ascending scan
  //

  auto table_oid = exec_ctx_->GetAccessor()->GetTableOid(NSOid(), "test_1");
  auto index_oid = exec_ctx_->GetAccessor()->GetIndexOid(NSOid(), "index_1");
  std::array<uint32_t, 1> col_oids{1};
  IndexIterator index_iter{exec_ctx_.get(),
                           1,
                           table_oid.UnderlyingValue(),
                           index_oid.UnderlyingValue(),
                           col_oids.data(),
                           static_cast<uint32_t>(col_oids.size())};
  index_iter.Init();
  auto *const lo_pr(index_iter.LoPR());
  auto *const hi_pr(index_iter.HiPR());
  lo_pr->Set<int32_t, false>(0, 495, false);
  hi_pr->Set<int32_t, false>(0, 505, false);
  index_iter.ScanAscending(storage::index::ScanType::Closed, 5);
  int32_t curr_match = 495;
  uint32_t num_matches = 0;
  while (index_iter.Advance()) {
    auto *const table_pr(index_iter.TablePR());
    auto *val = table_pr->Get<int32_t, false>(0, nullptr);
    EXPECT_EQ(*val, curr_match);
    curr_match++;
    num_matches++;
  }
  ASSERT_EQ(num_matches, 5);
}

// NOLINTNEXTLINE
TEST_F(IndexIteratorTest, SimpleDescendingScanTest) {
  //
  // Perform an descending scan
  //

  auto table_oid = exec_ctx_->GetAccessor()->GetTableOid(NSOid(), "test_1");
  auto index_oid = exec_ctx_->GetAccessor()->GetIndexOid(NSOid(), "index_1");
  std::array<uint32_t, 1> col_oids{1};
  IndexIterator index_iter{exec_ctx_.get(),
                           1,
                           table_oid.UnderlyingValue(),
                           index_oid.UnderlyingValue(),
                           col_oids.data(),
                           static_cast<uint32_t>(col_oids.size())};
  index_iter.Init();

  // Iterate through the table.
  auto *const lo_pr(index_iter.LoPR());
  auto *const hi_pr(index_iter.HiPR());
  lo_pr->Set<int32_t, false>(0, 495, false);
  hi_pr->Set<int32_t, false>(0, 505, false);
  index_iter.ScanDescending();
  int32_t curr_match = 505;
  uint32_t num_matches = 0;
  while (index_iter.Advance()) {
    auto *const table_pr(index_iter.TablePR());
    auto *val = table_pr->Get<int32_t, false>(0, nullptr);
    ASSERT_EQ(*val, curr_match);
    curr_match--;
    num_matches++;
  }
  ASSERT_EQ(num_matches, 11);
}

// NOLINTNEXTLINE
TEST_F(IndexIteratorTest, SimpleLimitDescendingScanTest) {
  //
  // Perform an descending scan
  //

  auto table_oid = exec_ctx_->GetAccessor()->GetTableOid(NSOid(), "test_1");
  auto index_oid = exec_ctx_->GetAccessor()->GetIndexOid(NSOid(), "index_1");
  std::array<uint32_t, 1> col_oids{1};
  IndexIterator index_iter{exec_ctx_.get(),
                           1,
                           table_oid.UnderlyingValue(),
                           index_oid.UnderlyingValue(),
                           col_oids.data(),
                           static_cast<uint32_t>(col_oids.size())};
  index_iter.Init();

  // Iterate through the table.
  auto *const lo_pr(index_iter.LoPR());
  auto *const hi_pr(index_iter.HiPR());
  lo_pr->Set<int32_t, false>(0, 495, false);
  hi_pr->Set<int32_t, false>(0, 505, false);
  index_iter.ScanLimitDescending(5);
  int32_t curr_match = 505;
  uint32_t num_matches = 0;
  while (index_iter.Advance()) {
    auto *const table_pr(index_iter.TablePR());
    auto *val = table_pr->Get<int32_t, false>(0, nullptr);
    ASSERT_EQ(*val, curr_match);
    curr_match--;
    num_matches++;
  }
  ASSERT_EQ(num_matches, 5);
}

}  // namespace noisepage::execution::sql::test
