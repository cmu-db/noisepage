#include "common/stat_registry.h"
#include <algorithm>
#include <functional>
#include <memory>
#include <random>
#include <string>
#include <vector>
#include "common/json.h"
#include "common/macros.h"
#include "gtest/gtest.h"
#include "storage/data_table.h"
#include "storage/record_buffer.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_context.h"

namespace terrier {

// clang-format off
/**
 * A simple dummy cache object with four differently typed attributes:
 *   uint64_t NumInsert
 *   uint32_t NumHit
 *   uint16_t NumFailure
 *   uint8_t NumUser
 */
#define CACHE_MEMBERS(f) \
  f(uint64_t, NumInsert) \
  f(uint32_t, NumHit) \
  f(uint16_t, NumFailure) \
  f(uint8_t, NumUser)
// clang-format on

DEFINE_PERFORMANCE_CLASS(CacheCounter, CACHE_MEMBERS)

/**
 * A simple dummy network object
 *   uint64_t NumRequest
 */
#define NETWORK_MEMBERS(f) f(uint64_t, NumRequest)

DEFINE_PERFORMANCE_CLASS(NetworkCounter, NETWORK_MEMBERS)

// Test being able to register/deregister a performance counter to the registry
// NOLINTNEXTLINE
TEST(StatRegistryTest, GTEST_DEBUG_ONLY(SimpleCorrectnessTest)) {
  terrier::common::StatisticsRegistry reg;
  CacheCounter cc;
  NetworkCounter nc;
  std::vector<std::string> current;
  std::vector<std::string> expected;

  // register under root
  reg.Register({}, &cc, this);
  EXPECT_EQ(reg.GetRegistrant({}, "CacheCounter"), this);
  expected = {"CacheCounter"};
  current = reg.GetRegistryListing();
  EXPECT_TRUE(current == expected);
  // get our counter back
  terrier::common::PerformanceCounter *pc = reg.GetPerformanceCounter({}, "CacheCounter");
  EXPECT_EQ(pc, &cc);
  // delete from root
  reg.Deregister({}, "CacheCounter", false);
  expected = {};
  current = reg.GetRegistryListing();
  EXPECT_TRUE(current == expected);

  // register under cache submodule
  reg.Register({"Cache"}, &cc, this);
  expected = {"Cache"};
  current = reg.GetRegistryListing();
  EXPECT_TRUE(current == expected);
  // check that we exist under cache submodule
  expected = {"CacheCounter"};
  current = reg.GetRegistryListing({"Cache"});
  EXPECT_TRUE(current == expected);
  // add something else to cache submodule
  reg.Register({"Cache"}, &nc, this);
  expected = {"CacheCounter", "NetworkCounter"};
  current = reg.GetRegistryListing({"Cache"});
  std::sort(current.begin(), current.end());
  EXPECT_TRUE(current == expected);
  // try deleting one thing from cache submodule
  reg.Deregister({"Cache"}, "CacheCounter", false);
  expected = {"NetworkCounter"};
  current = reg.GetRegistryListing({"Cache"});
  EXPECT_TRUE(current == expected);
  // try deleting another thing from cache submodule
  // since that becomes empty, should delete cache submodule itself
  reg.Deregister({"Cache"}, "NetworkCounter", false);
  expected = {};
  current = reg.GetRegistryListing();
  EXPECT_TRUE(current == expected);

  reg.Shutdown(false);
}

// Test registering multiple performance counters with the same name
// NOLINTNEXTLINE
TEST(StatRegistryTest, GTEST_DEBUG_ONLY(MultipleNameTest)) {
  terrier::common::StatisticsRegistry reg;
  CacheCounter cc;
  CacheCounter cc2;
  std::vector<std::string> current;
  std::vector<std::string> expected;

  reg.Register({}, &cc, this);
  reg.Register({}, &cc2, this);
  expected = {"CacheCounter", "CacheCounter1"};
  current = reg.GetRegistryListing();
  std::sort(current.begin(), current.end());
  EXPECT_TRUE(current == expected);
  reg.Deregister({}, "CacheCounter", false);
  expected = {"CacheCounter1"};
  current = reg.GetRegistryListing();
  std::sort(current.begin(), current.end());
  EXPECT_TRUE(current == expected);

  reg.Shutdown(false);
}

// Test dumping statistics
// NOLINTNEXTLINE
TEST(StatRegistryTest, GTEST_DEBUG_ONLY(DumpTest)) {
  terrier::common::StatisticsRegistry reg;
  CacheCounter cc;
  CacheCounter cc2;
  std::vector<std::string> current;
  std::vector<std::string> expected;

  reg.Register({}, &cc, this);
  reg.Register({"Cache"}, &cc2, this);

  cc.IncrementNumFailure(1);
  cc2.IncrementNumHit(1);

  terrier::common::json json = terrier::common::json::parse(reg.DumpStats());
  EXPECT_EQ(json["CacheCounter"]["Counters"]["NumFailure"], 1);
  EXPECT_EQ(json["Cache"]["CacheCounter"]["Counters"]["NumHit"], 1);

  reg.Shutdown(false);
}

// Test freeing the performance counters stored inside
// NOLINTNEXTLINE
TEST(StatRegistryTest, GTEST_DEBUG_ONLY(FreeTest)) {
  {
    terrier::common::StatisticsRegistry reg;
    auto *cc = new CacheCounter;
    reg.Register({}, cc, this);
    reg.Shutdown(true);
  }
  {
    terrier::common::StatisticsRegistry reg;
    auto *cc = new CacheCounter;
    reg.Register({}, cc, this);
    reg.Deregister({}, cc->GetName(), true);
    reg.Shutdown(true);
  }
}

// A basic test, registering a DataTable counter
// NOLINTNEXTLINE
TEST(StatRegistryTest, GTEST_DEBUG_ONLY(DataTableStatTest)) {
  terrier::storage::RecordBufferSegmentPool buffer_pool{100000, 10000};
  terrier::storage::BlockStore block_store{1000, 100};
  terrier::storage::BlockLayout block_layout({8, 8, 8});
  const std::vector<terrier::storage::col_id_t> col_ids = {terrier::storage::col_id_t{1},
                                                           terrier::storage::col_id_t{2}};
  terrier::storage::DataTable data_table(&block_store, block_layout, terrier::storage::layout_version_t{0});
  terrier::transaction::timestamp_t timestamp(0);
  auto *txn = new terrier::transaction::TransactionContext(timestamp, timestamp, &buffer_pool, DISABLED);
  auto init = terrier::storage::ProjectedRowInitializer::Create(block_layout, col_ids);
  auto *redo_buffer = terrier::common::AllocationUtil::AllocateAligned(init.ProjectedRowSize());
  auto *redo = init.InitializeRow(redo_buffer);

  data_table.Insert(txn, *redo);

  // initialize stat registry
  auto test_stat_reg = std::make_shared<terrier::common::StatisticsRegistry>();
  test_stat_reg->Register({"Storage"}, data_table.GetDataTableCounter(), &data_table);
  delete[] redo_buffer;
  delete txn;

  test_stat_reg->Shutdown(false);
}

}  // namespace terrier
