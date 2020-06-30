#include <tuple>
#include <vector>

#include "catalog/schema.h"
#include "execution/exec/execution_context.h"
#include "execution/sql/filter_manager.h"
#include "execution/sql/table_vector_iterator.h"
#include "execution/sql/thread_state_container.h"
#include "execution/sql_test.h"
#include "execution/util/timer.h"

namespace terrier::execution::sql::test {

class TableVectorIteratorTest : public SqlBasedTest {};

TEST_F(TableVectorIteratorTest, InvalidBlockRangeIteratorTest) {
  auto table_id = TableIdToNum(TableId::Test1);
  auto *table = Catalog::Instance()->LookupTableById(table_id);

  const std::tuple<uint32_t, uint32_t, bool> test_cases[] = {
      {0, 10, true},
      {10, 0, false},
      {-10, 2, false},
      {0, table->GetBlockCount(), true},
      {10, table->GetBlockCount(), true},
      {10, table->GetBlockCount() + 1, false},
  };

  for (auto [start_idx, end_idx, valid] : test_cases) {
    TableVectorIterator iter(table_id, start_idx, end_idx);
    EXPECT_EQ(valid, iter.Init());
  }
}

TEST_F(TableVectorIteratorTest, EmptyIteratorTest) {
  //
  // Check to see that iteration doesn't begin without an input block
  //

  TableVectorIterator iter(TableIdToNum(TableId::EmptyTable));

  EXPECT_TRUE(iter.Init());

  while (iter.Advance()) {
    FAIL() << "Empty table should have no tuples";
  }
}

TEST_F(TableVectorIteratorTest, SimpleIteratorTest) {
  //
  // Simple test to ensure we iterate over the whole table
  //

  TableVectorIterator iter(TableIdToNum(TableId::Test1));

  EXPECT_TRUE(iter.Init());

  uint32_t num_tuples = 0;
  while (iter.Advance()) {
    VectorProjectionIterator *vpi = iter.GetVectorProjectionIterator();
    for (; vpi->HasNext(); vpi->Advance()) {
      num_tuples++;
    }
    vpi->Reset();
  }

  EXPECT_EQ(iter.GetTable()->GetTupleCount(), num_tuples);
}

TEST_F(TableVectorIteratorTest, ParallelScanTest) {
  //
  // Simple test to ensure we iterate over the whole table in parallel
  //

  struct Counter {
    uint32_t c;
  };

  auto init_count = [](UNUSED void *ctx, void *tls) { reinterpret_cast<Counter *>(tls)->c = 0; };

  // Scan function just counts all tuples it sees
  auto scanner = [](UNUSED void *state, void *tls, TableVectorIterator *tvi) {
    auto *counter = reinterpret_cast<Counter *>(tls);
    while (tvi->Advance()) {
      for (auto *vpi = tvi->GetVectorProjectionIterator(); vpi->HasNext(); vpi->Advance()) {
        counter->c++;
      }
    }
  };

  // Setup thread states
  MemoryPool memory(nullptr);
  ExecutionContext ctx(&memory);
  ThreadStateContainer thread_state_container(ctx.GetMemoryPool());
  thread_state_container.Reset(sizeof(Counter),  // The type of each thread state structure
                               init_count,       // The thread state initialization function
                               nullptr,          // The thread state destruction function
                               nullptr           // Context passed to init/destroy functions
  );

  const auto table_id = TableIdToNum(TableId::Test1);
  TableVectorIterator::ParallelScan(table_id,                 // ID of table to scan
                                    nullptr,                  // Query state to pass to scan threads
                                    &thread_state_container,  // Container for thread states
                                    scanner                   // Scan function
  );

  // Count total aggregate tuple count seen by all threads
  uint32_t aggregate_tuple_count = 0;
  thread_state_container.ForEach<Counter>([&](Counter *counter) { aggregate_tuple_count += counter->c; });

  EXPECT_EQ(Catalog::Instance()->LookupTableById(table_id)->GetTupleCount(), aggregate_tuple_count);
}

}  // namespace terrier::execution::sql::test
