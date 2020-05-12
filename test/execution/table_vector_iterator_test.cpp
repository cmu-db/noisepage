#include "execution/sql/table_vector_iterator.h"

#include <array>
#include <memory>
#include <vector>

#include "catalog/catalog_defs.h"
#include "execution/sql_test.h"
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

TEST_F(TableVectorIteratorTest, ParallelScanTest) {
  //
  // Simple test to ensure we iterate over the whole table in parallel
  //

  struct Counter {
    uint32_t c_;
  };

  auto init_count = [](void *ctx, void *tls) { reinterpret_cast<Counter *>(tls)->c_ = 0; };

  // Scan function just counts all tuples it sees
  auto scanner = [](void *state, void *tls, TableVectorIterator *tvi) {
    auto *counter = reinterpret_cast<Counter *>(state);
    while (tvi->Advance()) {
      for (auto *pci = tvi->GetProjectedColumnsIterator(); pci->HasNext(); pci->Advance()) {
        counter->c_++;
      }
    }
  };

  uint32_t col_oids[] = {1};
  // Setup thread states
  ThreadStateContainer thread_state_container(common::ManagedPointer<MemoryPool>(exec_ctx_->GetMemoryPool()));

  unsigned int num_cores = std::thread::hardware_concurrency();
  if (num_cores == 0) {
    num_cores = 1;
  }

  // Parallel scan is tested using 1, 2, 4, 8 ... number of threads
  // until it reaches the number of cores on the machine
  for (uint32_t i = 1; i <= num_cores; i *= 2) {

    thread_state_container.Reset(sizeof(Counter),  // The type of each thread state structure
                                init_count,       // The thread state initialization function
                                nullptr,          // The thread state destruction function
                                nullptr);         // Context passed to init/destroy functions
    auto table_oid = exec_ctx_->GetAccessor()->GetTableOid(NSOid(), "test_1");
    TableVectorIterator::ParallelScan(static_cast<uint32_t>(table_oid),                           // ID of table to scan
                                      col_oids,                                                   // col oid array
                                      1,                                                          // Number of col oids
                                      thread_state_container.AccessThreadStateOfCurrentThread(),  // Thread states
                                      scanner,                                                    // Scan function
                                      exec_ctx_.get(),                                            // Execution context
                                      i);                                                         // Number of threads

    // Count total aggregate tuple count seen by all threads
    uint32_t aggregate_tuple_count = 0;
    thread_state_container.ForEach<Counter>([&](Counter *counter) { aggregate_tuple_count += counter->c_; });

    EXPECT_EQ(sql::TEST1_SIZE, aggregate_tuple_count);
  }
}
}  // namespace terrier::execution::sql::test
