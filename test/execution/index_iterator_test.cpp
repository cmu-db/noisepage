#include <array>
#include <memory>

#include "execution/sql_test.h"

#include "catalog/catalog_defs.h"
#include "execution/sql/index_iterator.h"
#include "execution/sql/projected_row_wrapper.h"
#include "execution/sql/table_vector_iterator.h"
#include "execution/util/timer.h"

namespace terrier::execution::sql::test {

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
  TableVectorIterator table_iter(exec_ctx_.get(), !table_oid, col_oids.data(), static_cast<uint32_t>(col_oids.size()));
  IndexIterator index_iter{exec_ctx_.get(), !table_oid, !index_oid, col_oids.data(),
                           static_cast<uint32_t>(col_oids.size())};
  table_iter.Init();
  index_iter.Init();
  ProjectedColumnsIterator *pci = table_iter.GetProjectedColumnsIterator();

  // Iterate through the table.
  while (table_iter.Advance()) {
    for (; pci->HasNext(); pci->Advance()) {
      auto *key = pci->Get<int32_t, false>(0, nullptr);
      // Check that the key can be recovered through the index
      ProjectedRowWrapper index_pr(index_iter.PR());
      index_pr.Set<int32_t, false>(0, *key, false);
      index_iter.ScanKey();
      // One entry should be found
      ASSERT_TRUE(index_iter.Advance());
      // Get directly from iterator
      ProjectedRowWrapper table_pr(index_iter.TablePR());
      auto *val = table_pr.Get<int32_t, false>(0, nullptr);
      ASSERT_EQ(*key, *val);
      val = nullptr;
      // Get indirectly from tuple slot
      storage::TupleSlot slot(index_iter.CurrentSlot());
      ASSERT_TRUE(sql_table->Select(exec_ctx_->GetTxn(), slot, index_iter.TablePR()));
      val = table_pr.Get<int32_t, false>(0, nullptr);
      ASSERT_EQ(*key, *val);

      // Check that there are no more entries.
      ASSERT_FALSE(index_iter.Advance());
    }
    pci->Reset();
  }
}

}  // namespace terrier::execution::sql::test
