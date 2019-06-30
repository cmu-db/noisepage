#include <memory>

#include "execution/sql_test.h"  // NOLINT

#include "catalog/catalog_defs.h"
#include "execution/sql/table_vector_iterator.h"
#include "execution/util/timer.h"

namespace tpl::sql::test {

class TableVectorIteratorTest : public SqlBasedTest {
  void SetUp() override {
    // Create the test tables
    SqlBasedTest::SetUp();
    exec_ctx_ = MakeExecCtx();
    sql::TableGenerator table_generator{exec_ctx_.get()};
    table_generator.GenerateTestTables();
  }

 protected:
  /**
   * Execution context to use for the test
   */
  std::unique_ptr<exec::ExecutionContext> exec_ctx_;
};

// NOLINTNEXTLINE
TEST_F(TableVectorIteratorTest, EmptyIteratorTest) {
  //
  // Check to see that iteration doesn't begin without an input block
  //
  auto catalog_table = exec_ctx_->GetAccessor()->GetUserTable("empty_table");
  TableVectorIterator iter(!catalog_table->Oid(), exec_ctx_.get());
  iter.Init();
  while (iter.Advance()) {
    FAIL() << "Empty table should have no tuples";
  }
}

// NOLINTNEXTLINE
TEST_F(TableVectorIteratorTest, SimpleIteratorTest) {
  //
  // Simple test to ensure we iterate over the whole table
  //

  auto catalog_table = exec_ctx_->GetAccessor()->GetUserTable("test_1");
  TableVectorIterator iter(!catalog_table->Oid(), exec_ctx_.get());
  iter.Init();
  ProjectedColumnsIterator *pci = iter.projected_columns_iterator();

  u32 num_tuples = 0;
  while (iter.Advance()) {
    for (; pci->HasNext(); pci->Advance()) {
      num_tuples++;
    }
    pci->Reset();
  }
  EXPECT_EQ(sql::test1_size, num_tuples);
}

// NOLINTNEXTLINE
TEST_F(TableVectorIteratorTest, NullableTypesIteratorTest) {
  //
  // Ensure we iterate over the whole table even the types of the columns are
  // different
  //

  auto catalog_table = exec_ctx_->GetAccessor()->GetUserTable("test_2");
  TableVectorIterator iter(!catalog_table->Oid(), exec_ctx_.get());
  iter.Init();
  ProjectedColumnsIterator *pci = iter.projected_columns_iterator();

  u32 num_tuples = 0;
  while (iter.Advance()) {
    for (; pci->HasNext(); pci->Advance()) {
      num_tuples++;
    }
    pci->Reset();
  }
  EXPECT_EQ(sql::test2_size, num_tuples);
}

}  // namespace tpl::sql::test
