#include <array>
#include <memory>
#include <vector>

#include "catalog/catalog_defs.h"
#include "execution/sql/table_vector_iterator.h"
#include "execution/sql_test.h"
#include "execution/util/timer.h"

namespace noisepage::execution::sql::test {

class TableVectorIteratorTest : public SqlBasedTest {
  void SetUp() override {
    // Create the test tables
    SqlBasedTest::SetUp();
    exec_ctx_ = MakeExecCtx();
    GenerateTestTables(exec_ctx_.get());
  }

 public:
  parser::ConstantValueExpression DummyExpr() {
    return parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(0));
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
  TableVectorIterator iter(exec_ctx_.get(), table_oid.UnderlyingValue(), col_oids.data(),
                           static_cast<uint32_t>(col_oids.size()));
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
  TableVectorIterator iter(exec_ctx_.get(), table_oid.UnderlyingValue(), col_oids.data(),
                           static_cast<uint32_t>(col_oids.size()));
  iter.Init();
  VectorProjectionIterator *vpi = iter.GetVectorProjectionIterator();

  uint32_t num_tuples = 0;
  int32_t prev_val{0};
  while (iter.Advance()) {
    for (; vpi->HasNext(); vpi->Advance()) {
      const auto *val = vpi->GetValue<int32_t, false>(0, nullptr);
      if (num_tuples > 0) {
        ASSERT_EQ(*val, prev_val + 1);
      }
      prev_val = *val;
      num_tuples++;
    }
    vpi->Reset();
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
  TableVectorIterator iter(exec_ctx_.get(), table_oid.UnderlyingValue(), col_oids.data(),
                           static_cast<uint32_t>(col_oids.size()));
  iter.Init();
  VectorProjectionIterator *vpi;

  uint32_t num_tuples = 0;
  int16_t prev_val{0};
  while (iter.Advance()) {
    for (vpi = iter.GetVectorProjectionIterator(); vpi->HasNext(); vpi->Advance()) {
      // The serial column is the first one
      const auto *val = vpi->GetValue<int16_t, false>(0, nullptr);
      if (num_tuples > 0) {
        ASSERT_EQ(*val, prev_val + 1);
      }
      prev_val = *val;
      num_tuples++;
    }
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
  TableVectorIterator iter(exec_ctx_.get(), table_oid.UnderlyingValue(), col_oids.data(),
                           static_cast<uint32_t>(col_oids.size()));
  iter.Init();
  VectorProjectionIterator *vpi = iter.GetVectorProjectionIterator();

  uint32_t num_tuples = 0;
  int16_t prev_val{0};
  while (iter.Advance()) {
    for (; vpi->HasNext(); vpi->Advance()) {
      // Because we only specified one column, its index is 0 instead of three
      auto *val = vpi->GetValue<int16_t, false>(0, nullptr);
      if (num_tuples > 0) {
        ASSERT_EQ(*val, prev_val + 1);
      }
      prev_val = *val;
      num_tuples++;
    }
    vpi->Reset();
  }
  EXPECT_EQ(sql::TEST2_SIZE, num_tuples);
}

// NOLINTNEXTLINE
TEST_F(TableVectorIteratorTest, ParallelScanTest) {
  //
  // Simple test to ensure we iterate over the whole table in parallel
  //

  struct Counter {
    uint32_t c_;
  };

  auto init_count = [](void *ctx, void *tls) { reinterpret_cast<Counter *>(tls)->c_ = 0; };

  // Scan function just counts all tuples it sees
  auto scanner = [](UNUSED_ATTRIBUTE void *state, void *tls, TableVectorIterator *tvi) {
    auto *counter = reinterpret_cast<Counter *>(tls);
    while (tvi->Advance()) {
      for (auto *vpi = tvi->GetVectorProjectionIterator(); vpi->HasNext(); vpi->Advance()) {
        counter->c_++;
      }
    }
  };

  // Setup thread states
  exec_ctx_->GetThreadStateContainer()->Reset(sizeof(Counter), init_count, nullptr, exec_ctx_.get());

  auto table_oid = exec_ctx_->GetAccessor()->GetTableOid(NSOid(), "test_1");
  std::array<uint32_t, 4> col_oids{1, 2, 3, 4};
  TableVectorIterator::ParallelScan(table_oid.UnderlyingValue(), col_oids.data(), col_oids.size(), nullptr,
                                    exec_ctx_.get(), scanner);

  // Count total aggregate tuple count seen by all threads
  uint32_t aggregate_tuple_count = 0;
  exec_ctx_->GetThreadStateContainer()->ForEach<Counter>(
      [&](Counter *counter) { aggregate_tuple_count += counter->c_; });
  EXPECT_EQ(sql::TEST1_SIZE, aggregate_tuple_count);
}

}  // namespace noisepage::execution::sql::test
