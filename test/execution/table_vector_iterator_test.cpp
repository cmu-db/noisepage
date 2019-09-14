#include <array>
#include <memory>
#include <vector>

#include "execution/sql_test.h"

#include "catalog/catalog_defs.h"
#include "execution/sql/table_vector_iterator.h"
#include "execution/util/timer.h"

namespace terrier::execution::sql::test {

class TableVectorIteratorTest : public SqlBasedTest {
  void SetUp() override {
    // Create the test tables
    SqlBasedTest::SetUp();
    exec_ctx_ = MakeExecCtx();
    GenerateTestTables(exec_ctx_.get());
  }

 public:
  parser::ConstantValueExpression DummyExpr() {
    return parser::ConstantValueExpression(type::TransientValueFactory::GetInteger(0));
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
  auto table_oid = exec_ctx_->GetAccessor()->GetTableOid(NSOid(), "empty_table");
  std::array<uint32_t, 1> col_oids{1};
  TableVectorIterator iter(exec_ctx_.get(), !table_oid, col_oids.data(), static_cast<uint32_t>(col_oids.size()));
  iter.Init();
  ASSERT_FALSE(iter.Advance());
}

// NOLINTNEXTLINE
TEST_F(TableVectorIteratorTest, SimpleIteratorTest) {
  //
  // Simple test to ensure we iterate over the whole table
  //

  auto table_oid = exec_ctx_->GetAccessor()->GetTableOid(NSOid(), "test_1");
  std::array<uint32_t, 1> col_oids{1};
  TableVectorIterator iter(exec_ctx_.get(), !table_oid, col_oids.data(), static_cast<uint32_t>(col_oids.size()));
  iter.Init();
  ProjectedColumnsIterator *pci = iter.GetProjectedColumnsIterator();

  uint32_t num_tuples = 0;
  int32_t prev_val{0};
  while (iter.Advance()) {
    for (; pci->HasNext(); pci->Advance()) {
      auto *val = pci->Get<int32_t, false>(0, nullptr);
      if (num_tuples > 0) {
        ASSERT_EQ(*val, prev_val + 1);
      }
      prev_val = *val;
      num_tuples++;
    }
    pci->Reset();
  }
  EXPECT_EQ(sql::TEST1_SIZE, num_tuples);
}

// NOLINTNEXTLINE
TEST_F(TableVectorIteratorTest, MultipleTypesIteratorTest) {
  //
  // Ensure we iterate over the whole table even the types of the columns are
  // different
  //

  auto table_oid = exec_ctx_->GetAccessor()->GetTableOid(NSOid(), "test_2");
  std::array<uint32_t, 4> col_oids{1, 2, 3, 4};
  TableVectorIterator iter(exec_ctx_.get(), !table_oid, col_oids.data(), static_cast<uint32_t>(col_oids.size()));
  iter.Init();
  ProjectedColumnsIterator *pci = iter.GetProjectedColumnsIterator();

  uint32_t num_tuples = 0;
  int16_t prev_val{0};
  while (iter.Advance()) {
    for (; pci->HasNext(); pci->Advance()) {
      // The serial column is the smallest one (SmallInt type), so it should be the last index in the storage layer.
      auto *val = pci->Get<int16_t, false>(3, nullptr);
      if (num_tuples > 0) {
        ASSERT_EQ(*val, prev_val + 1);
      }
      prev_val = *val;
      num_tuples++;
    }
    pci->Reset();
  }
  EXPECT_EQ(sql::TEST2_SIZE, num_tuples);
}

// NOLINTNEXTLINE
TEST_F(TableVectorIteratorTest, IteratorColOidsTest) {
  //
  // Ensure we only iterate over specified columns
  //

  auto table_oid = exec_ctx_->GetAccessor()->GetTableOid(NSOid(), "test_2");
  std::array<uint32_t, 1> col_oids{1};
  TableVectorIterator iter(exec_ctx_.get(), !table_oid, col_oids.data(), static_cast<uint32_t>(col_oids.size()));
  iter.Init();
  ProjectedColumnsIterator *pci = iter.GetProjectedColumnsIterator();

  uint32_t num_tuples = 0;
  int16_t prev_val{0};
  while (iter.Advance()) {
    for (; pci->HasNext(); pci->Advance()) {
      // Because we only specified one column, its index is 0 instead of three
      auto *val = pci->Get<int16_t, false>(0, nullptr);
      if (num_tuples > 0) {
        ASSERT_EQ(*val, prev_val + 1);
      }
      prev_val = *val;
      num_tuples++;
    }
    pci->Reset();
  }
  EXPECT_EQ(sql::TEST2_SIZE, num_tuples);
}

}  // namespace terrier::execution::sql::test
