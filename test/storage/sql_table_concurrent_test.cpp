#include <algorithm>
#include <cstring>
#include <random>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "storage/garbage_collector.h"
#include "storage/sql_table.h"
#include "transaction/transaction_manager.h"
#include "type/type_id.h"
#include "util/catalog_test_util.h"
#include "util/test_harness.h"
#include "util/transaction_test_util.h"
namespace terrier {
struct SqlTableConcurrentTests : public TerrierTest {
  SqlTableConcurrentTests() : gc_(&txn_manager_) {}

  void SetUp() override { TerrierTest::SetUp(); }

  void TearDown() override { TerrierTest::TearDown(); }

  std::vector<catalog::Schema::Column> GenerateColumnsVector(storage::layout_version_t v) {
    std::vector<catalog::Schema::Column> cols;

    cols.emplace_back("version", type::TypeId::INTEGER, false, catalog::col_oid_t(100));
    cols.emplace_back("sentinel", type::TypeId::INTEGER, false, catalog::col_oid_t(1000));
    if ((!v) < 1) cols.emplace_back("bigint", type::TypeId::BIGINT, false, catalog::col_oid_t(1001));
    if ((!v) < 2) cols.emplace_back("integer", type::TypeId::INTEGER, false, catalog::col_oid_t(1002));
    if ((!v) < 3) cols.emplace_back("smallint", type::TypeId::SMALLINT, false, catalog::col_oid_t(1003));
    if ((!v) < 4) cols.emplace_back("tinyint", type::TypeId::TINYINT, false, catalog::col_oid_t(1004));
    if ((!v) >= 5) cols.emplace_back("tinyint", type::TypeId::TINYINT, false, catalog::col_oid_t(1005));
    if ((!v) >= 6) cols.emplace_back("smallint", type::TypeId::SMALLINT, false, catalog::col_oid_t(1006));
    if ((!v) >= 7) cols.emplace_back("integer", type::TypeId::INTEGER, false, catalog::col_oid_t(1007));
    if ((!v) >= 8) cols.emplace_back("bigint", type::TypeId::BIGINT, false, catalog::col_oid_t(1008));

    auto *col_oids = new std::vector<catalog::col_oid_t>;
    for (const auto &c : cols) {
      col_oids->emplace_back(c.GetOid());
    }
    versioned_col_oids.emplace_back(col_oids);

    return cols;
  }

  void PopulateProjectedRow(storage::layout_version_t v, int32_t base_val, storage::ProjectedRow *pr,
                            storage::ProjectionMap *pr_map) {
    EXPECT_NE(pr_map->find(catalog::col_oid_t(100)), pr_map->end());
    uint32_t *version = reinterpret_cast<uint32_t *>(pr->AccessForceNotNull(pr_map->at(catalog::col_oid_t(100))));
    *version = static_cast<uint32_t>(v);

    EXPECT_NE(pr_map->find(catalog::col_oid_t(1000)), pr_map->end());
    int32_t *sentinel = reinterpret_cast<int32_t *>(pr->AccessForceNotNull(pr_map->at(catalog::col_oid_t(1000))));
    *sentinel = base_val;

    if ((!v) < 1) {
      EXPECT_NE(pr_map->find(catalog::col_oid_t(1001)), pr_map->end());
      int64_t *bigint = reinterpret_cast<int64_t *>(pr->AccessForceNotNull(pr_map->at(catalog::col_oid_t(1001))));
      *bigint = base_val << 31;
    }

    if ((!v) < 2) {
      EXPECT_NE(pr_map->find(catalog::col_oid_t(1002)), pr_map->end());
      int32_t *integer = reinterpret_cast<int32_t *>(pr->AccessForceNotNull(pr_map->at(catalog::col_oid_t(1002))));
      *integer = base_val;
    }

    if ((!v) < 3) {
      EXPECT_NE(pr_map->find(catalog::col_oid_t(1003)), pr_map->end());
      int16_t *smallint = reinterpret_cast<int16_t *>(pr->AccessForceNotNull(pr_map->at(catalog::col_oid_t(1003))));
      *smallint = static_cast<int16_t>(base_val % 1 << 15);
    }

    if ((!v) < 4) {
      EXPECT_NE(pr_map->find(catalog::col_oid_t(1004)), pr_map->end());
      int8_t *tinyint = reinterpret_cast<int8_t *>(pr->AccessForceNotNull(pr_map->at(catalog::col_oid_t(1004))));
      *tinyint = static_cast<int8_t>(base_val % 1 << 7);
    }

    if ((!v) >= 5) {
      EXPECT_NE(pr_map->find(catalog::col_oid_t(1005)), pr_map->end());
      int8_t *tinyint = reinterpret_cast<int8_t *>(pr->AccessForceNotNull(pr_map->at(catalog::col_oid_t(1005))));
      *tinyint = static_cast<int8_t>(base_val % 1 << 7);
    }

    if ((!v) >= 6) {
      EXPECT_NE(pr_map->find(catalog::col_oid_t(1006)), pr_map->end());
      int16_t *smallint = reinterpret_cast<int16_t *>(pr->AccessForceNotNull(pr_map->at(catalog::col_oid_t(1006))));
      *smallint = static_cast<int16_t>(base_val % 1 << 15);
    }

    if ((!v) >= 7) {
      EXPECT_NE(pr_map->find(catalog::col_oid_t(1007)), pr_map->end());
      int32_t *integer = reinterpret_cast<int32_t *>(pr->AccessForceNotNull(pr_map->at(catalog::col_oid_t(1007))));
      *integer = base_val;
    }

    if ((!v) >= 8) {
      EXPECT_NE(pr_map->find(catalog::col_oid_t(1008)), pr_map->end());
      int64_t *bigint = reinterpret_cast<int64_t *>(pr->AccessForceNotNull(pr_map->at(catalog::col_oid_t(1008))));
      *bigint = base_val << 31;
    }
  }

  template<class RowType>
  void ValidateTuple(RowType *pr, storage::ProjectionMap *pr_map, storage::layout_version_t v,
                     int base_val) {
    EXPECT_NE(pr_map->find(catalog::col_oid_t(100)), pr_map->end());
    uint32_t *version = reinterpret_cast<uint32_t *>(pr->AccessWithNullCheck(pr_map->at(catalog::col_oid_t(100))));
    EXPECT_NE(version, nullptr);
    // LOG_INFO("Reading tuple at {} from {}", *version, !v);
    ASSERT_TRUE(*version <= (!v));

    EXPECT_NE(pr_map->find(catalog::col_oid_t(1000)), pr_map->end());
    int32_t *sentinel = reinterpret_cast<int32_t *>(pr->AccessWithNullCheck(pr_map->at(catalog::col_oid_t(1000))));
    EXPECT_EQ(*sentinel, base_val);

    if ((!v) < 1) {
      EXPECT_NE(pr_map->find(catalog::col_oid_t(1001)), pr_map->end());
      int64_t *bigint = reinterpret_cast<int64_t *>(pr->AccessWithNullCheck(pr_map->at(catalog::col_oid_t(1001))));
      EXPECT_EQ(*bigint, base_val << 31);
    }

    if ((!v) < 2) {
      EXPECT_NE(pr_map->find(catalog::col_oid_t(1002)), pr_map->end());
      int32_t *integer = reinterpret_cast<int32_t *>(pr->AccessWithNullCheck(pr_map->at(catalog::col_oid_t(1002))));
      EXPECT_EQ(*integer, base_val);
    }

    if ((!v) < 3) {
      EXPECT_NE(pr_map->find(catalog::col_oid_t(1003)), pr_map->end());
      int16_t *smallint = reinterpret_cast<int16_t *>(pr->AccessWithNullCheck(pr_map->at(catalog::col_oid_t(1003))));
      EXPECT_EQ(*smallint, static_cast<int16_t>(base_val % 1 << 15));
    }

    if ((!v) < 4) {
      EXPECT_NE(pr_map->find(catalog::col_oid_t(1004)), pr_map->end());
      int8_t *tinyint = reinterpret_cast<int8_t *>(pr->AccessWithNullCheck(pr_map->at(catalog::col_oid_t(1004))));
      EXPECT_EQ(*tinyint, static_cast<int8_t>(base_val % 1 << 7));
    }

    if ((!v) >= 5) {
      EXPECT_NE(pr_map->find(catalog::col_oid_t(1005)), pr_map->end());
      int8_t *tinyint = reinterpret_cast<int8_t *>(pr->AccessWithNullCheck(pr_map->at(catalog::col_oid_t(1005))));
      if (*version < 5)
        EXPECT_EQ(tinyint, nullptr);
      else
        EXPECT_EQ(*tinyint, static_cast<int8_t>(base_val % 1 << 7));
    }

    if ((!v) >= 6) {
      EXPECT_NE(pr_map->find(catalog::col_oid_t(1006)), pr_map->end());
      int16_t *smallint = reinterpret_cast<int16_t *>(pr->AccessWithNullCheck(pr_map->at(catalog::col_oid_t(1006))));
      if (*version < 6)
        EXPECT_EQ(smallint, nullptr);
      else
        EXPECT_EQ(*smallint, static_cast<int16_t>(base_val % 1 << 15));
    }

    if ((!v) >= 7) {
      EXPECT_NE(pr_map->find(catalog::col_oid_t(1007)), pr_map->end());
      int32_t *integer = reinterpret_cast<int32_t *>(pr->AccessWithNullCheck(pr_map->at(catalog::col_oid_t(1007))));
      if (*version < 7)
        EXPECT_EQ(integer, nullptr);
      else
        EXPECT_EQ(*integer, base_val);
    }

    if ((!v) >= 8) {
      EXPECT_NE(pr_map->find(catalog::col_oid_t(1008)), pr_map->end());
      int64_t *bigint = reinterpret_cast<int64_t *>(pr->AccessWithNullCheck(pr_map->at(catalog::col_oid_t(1008))));
      if (*version < 8)
        EXPECT_EQ(bigint, nullptr);
      else
        EXPECT_EQ(*bigint, base_val << 31);
    }
  }

  void ValidateTable(storage::SqlTable &table) {
    auto txn = txn_manager_.BeginTransaction();

    auto row_pair = table.InitializerForProjectedColumns(*versioned_col_oids[!schema_version_], 100, schema_version_);
    auto pci = new storage::ProjectedColumnsInitializer(std::get<0>(row_pair));
    auto pc_map = new storage::ProjectionMap(std::get<1>(row_pair));
    byte *buffer = common::AllocationUtil::AllocateAligned(pci->ProjectedColumnsSize());
    auto table_iter = table.begin(schema_version_);

    while (table_iter != table.end())  {
      // LOG_INFO("  Iter at ({}, {})", reinterpret_cast<uint64_t>(table_iter->GetBlock()), (table_iter->GetOffset()));
      auto pc = pci->Initialize(buffer);
      table.Scan(txn, &table_iter, pc, *pc_map, schema_version_);
      // LOG_INFO("  Iter at ({}, {})", reinterpret_cast<uint64_t>(table_iter->GetBlock()), (table_iter->GetOffset()));

      for (uint i : {0u, pc->NumTuples() - 1u}) {
        auto pr = pc->InterpretAsRow(i);
        EXPECT_NE(pc_map->find(catalog::col_oid_t(1000)), pc_map->end());
        int32_t *base_val = reinterpret_cast<int32_t *>(pr.AccessWithNullCheck(pc_map->at(catalog::col_oid_t(1000))));
        ValidateTuple<storage::ProjectedColumns::RowView>(&pr, pc_map, schema_version_, *base_val);
      }
    }

    delete[] buffer;
    delete pci;
    delete pc_map;
  }

  storage::RecordBufferSegmentPool buffer_pool_{100000, 100000};
  transaction::TransactionManager txn_manager_ = {&buffer_pool_, true, LOGGING_DISABLED};

  std::default_random_engine generator_;

  catalog::table_oid_t table_oid_ = catalog::table_oid_t(42);
  // catalog::col_oid_t next_col_oid_ = catalog::col_oid_t(1000);
  storage::layout_version_t schema_version_ = storage::layout_version_t(0);
  storage::BlockStore block_store_{100, 100};
  std::vector<catalog::Schema::Column> cols_;
  std::vector<std::vector<catalog::col_oid_t> *> versioned_col_oids;
  storage::GarbageCollector gc_;

 private:
};

// Execute a large number of inserts concurrently with schema changes and validate
// all of the data is consistent with what we expect in the end by embedding an id
// and insertion time version into the tuple.
// NOLINTNEXTLINE
TEST_F(SqlTableConcurrentTests, ConcurrentInsertsWithDifferentVersions) {
  const uint32_t num_iterations = 100;
  const uint32_t txns_per_thread = 10;
  const uint32_t num_threads = MultiThreadTestUtil::HardwareConcurrency();
  common::WorkerPool thread_pool(num_threads, {});

  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    // LOG_INFO("iteration {}", iteration);
    schema_version_ = storage::layout_version_t(0);
    catalog::Schema schema(GenerateColumnsVector(schema_version_), schema_version_);
    storage::SqlTable table(&block_store_, schema, table_oid_);

    // Begin concurrent section
    auto workload = [&](uint32_t id) {
      for (uint32_t t = 0; t < txns_per_thread; t++) {
        storage::layout_version_t working_version = schema_version_;
        transaction::TransactionContext *txn;
        if (id == 0) {
          if (t >= 8) break;  // No more schema updates
          txn = txn_manager_.BeginTransaction();
          // LOG_INFO("  Adding schema version {}", (!working_version)+1);
          catalog::Schema schema(GenerateColumnsVector(working_version + 1), working_version + 1);
          table.UpdateSchema(schema);
          // Update schema
        } else {
          txn = txn_manager_.BeginTransaction();
          // LOG_INFO("    Thread {} in version {} with transaction {}", id, !working_version, t);
          auto row_pair = table.InitializerForProjectedRow(*versioned_col_oids[!working_version], working_version);
          auto pri = new storage::ProjectedRowInitializer(std::get<0>(row_pair));
          auto pr_map = new storage::ProjectionMap(std::get<1>(row_pair));
          int32_t base_val = id * txns_per_thread + t;
          byte *buffer = common::AllocationUtil::AllocateAligned(pri->ProjectedRowSize());
          auto pr = pri->InitializeRow(buffer);

          PopulateProjectedRow(working_version, base_val, pr, pr_map);

          table.Insert(txn, *pr, working_version);
          pr = nullptr;
          delete[] buffer;
          delete pri;
          delete pr_map;
        }
        txn_manager_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
        if (id == 0) schema_version_++;
      }
    };

    MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads, workload);

    // LOG_INFO("Validating table...");
    ValidateTable(table);

    for (auto &version : versioned_col_oids) delete version;
    versioned_col_oids.clear();
    // End concurrent section
    // delete init_txn;

    gc_.PerformGarbageCollection();
    gc_.PerformGarbageCollection();
    gc_.PerformGarbageCollection();
  }
}

// Initialize the table with predetermined tuples and then concurrently read
// (via Select) the tuples and verify that the data they contain is consistent
// with the write and read versions.
// NOLINTNEXTLINE
TEST_F(SqlTableConcurrentTests, ConcurrentSelectsWithDifferentVersions) {
  const uint32_t num_iterations = 100;
  const uint32_t txns_per_thread = 10;
  const uint32_t num_threads = MultiThreadTestUtil::HardwareConcurrency();
  common::WorkerPool thread_pool(num_threads, {});

  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    // LOG_INFO("iteration {}", iteration);
    schema_version_ = storage::layout_version_t(0);
    catalog::Schema schema(GenerateColumnsVector(schema_version_), schema_version_);
    storage::SqlTable table(&block_store_, schema, table_oid_);
    std::vector<storage::TupleSlot> tuples;

    // Setup table
    transaction::TransactionContext *init_txn = txn_manager_.BeginTransaction();

    auto row_pair = table.InitializerForProjectedRow(*versioned_col_oids[!schema_version_], schema_version_);
    auto pri = new storage::ProjectedRowInitializer(std::get<0>(row_pair));
    auto pr_map = new storage::ProjectionMap(std::get<1>(row_pair));

    for (uint32_t i = 0; i < txns_per_thread * num_threads; i++) {
      byte *buffer = common::AllocationUtil::AllocateAligned(pri->ProjectedRowSize());
      auto pr = pri->InitializeRow(buffer);

      PopulateProjectedRow(schema_version_, i, pr, pr_map);

      tuples.emplace_back(table.Insert(init_txn, *pr, schema_version_));
      pr = nullptr;
      delete[] buffer;
    }
    txn_manager_.Commit(init_txn, TestCallbacks::EmptyCallback, nullptr);


    delete pri;
    delete pr_map;

    // Begin concurrent section
    auto workload = [&](uint32_t id) {
      for (uint32_t t = 0; t < txns_per_thread; t++) {
        storage::layout_version_t working_version = schema_version_;
        transaction::TransactionContext *txn;
        if (id == 0) {
          if (t >= 8) break;  // No more schema updates
          txn = txn_manager_.BeginTransaction();
          // LOG_INFO("  Adding schema version {}", (!working_version)+1);
          catalog::Schema schema(GenerateColumnsVector(working_version + 1), working_version + 1);
          table.UpdateSchema(schema);
          // Update schema
        } else {
          txn = txn_manager_.BeginTransaction();
          // LOG_INFO("    Thread {} in version {} with transaction {}", id, !working_version, t);
          auto row_pair = table.InitializerForProjectedRow(*versioned_col_oids[!working_version], working_version);
          auto pri = new storage::ProjectedRowInitializer(std::get<0>(row_pair));
          auto pr_map = new storage::ProjectionMap(std::get<1>(row_pair));
          int32_t base_val = id * txns_per_thread + t;
          byte *buffer = common::AllocationUtil::AllocateAligned(pri->ProjectedRowSize());
          auto pr = pri->InitializeRow(buffer);

          table.Select(txn, tuples[base_val], pr, *pr_map, working_version);
          ValidateTuple<storage::ProjectedRow>(pr, pr_map, working_version, base_val);

          pr = nullptr;
          delete[] buffer;
          delete pri;
          delete pr_map;
        }
        txn_manager_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
        if (id == 0) schema_version_++;
      }
    };

    MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads, workload);
    for (auto &version : versioned_col_oids) delete version;
    versioned_col_oids.clear();
    // End concurrent section
    // delete init_txn;

    gc_.PerformGarbageCollection();
    gc_.PerformGarbageCollection();
    gc_.PerformGarbageCollection();
  }
}

// Initialize the table with tuples and then update them to a checkable
// state that forces migration on later schemas and validate consistency.
// NOLINTNEXTLINE
TEST_F(SqlTableConcurrentTests, ConcurrentQueriesWithSchemaChange) {
  const uint32_t num_iterations = 100;
  const uint32_t txns_per_thread = 10;
  const uint32_t num_threads = MultiThreadTestUtil::HardwareConcurrency();
  common::WorkerPool thread_pool(num_threads, {});

  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    // LOG_INFO("iteration {}", iteration);
    schema_version_ = storage::layout_version_t(0);
    catalog::Schema schema(GenerateColumnsVector(schema_version_), schema_version_);
    storage::SqlTable table(&block_store_, schema, table_oid_);
    std::vector<storage::TupleSlot> tuples;

    // Setup table
    transaction::TransactionContext *init_txn = txn_manager_.BeginTransaction();

    auto row_pair = table.InitializerForProjectedRow(*versioned_col_oids[!schema_version_], schema_version_);
    auto pri = new storage::ProjectedRowInitializer(std::get<0>(row_pair));
    auto pr_map = new storage::ProjectionMap(std::get<1>(row_pair));

    for (uint32_t i = 0; i < txns_per_thread * num_threads; i++) {
      byte *buffer = common::AllocationUtil::AllocateAligned(pri->ProjectedRowSize());
      auto pr = pri->InitializeRow(buffer);

      PopulateProjectedRow(schema_version_, i, pr, pr_map);

      tuples.emplace_back(table.Insert(init_txn, *pr, schema_version_));
      pr = nullptr;
      delete[] buffer;
    }
    txn_manager_.Commit(init_txn, TestCallbacks::EmptyCallback, nullptr);


    delete pri;
    delete pr_map;

    // Begin concurrent section
    auto workload = [&](uint32_t id) {
      for (uint32_t t = 0; t < txns_per_thread; t++) {
        storage::layout_version_t working_version = schema_version_;
        transaction::TransactionContext *txn;
        if (id == 0) {
          if (t >= 8) break;  // No more schema updates
          txn = txn_manager_.BeginTransaction();
          // LOG_INFO("  Adding schema version {}", (!working_version)+1);
          catalog::Schema schema(GenerateColumnsVector(working_version + 1), working_version + 1);
          table.UpdateSchema(schema);
          // Update schema
        } else {
          txn = txn_manager_.BeginTransaction();
          // LOG_INFO("    Thread {} in version {} with transaction {}", id, !working_version, t);
          auto row_pair = table.InitializerForProjectedRow(*versioned_col_oids[!working_version], working_version);
          auto pri = new storage::ProjectedRowInitializer(std::get<0>(row_pair));
          auto pr_map = new storage::ProjectionMap(std::get<1>(row_pair));
          int32_t base_val = id * txns_per_thread + t;
          byte *buffer = common::AllocationUtil::AllocateAligned(pri->ProjectedRowSize());
          auto pr = pri->InitializeRow(buffer);

          table.Select(txn, tuples[base_val], pr, *pr_map, working_version);
          ValidateTuple<storage::ProjectedRow>(pr, pr_map, working_version, base_val);

          EXPECT_NE(pr_map->find(catalog::col_oid_t(100)), pr_map->end());
          uint32_t *version = reinterpret_cast<uint32_t *>(pr->AccessWithNullCheck(pr_map->at(catalog::col_oid_t(100))));

          PopulateProjectedRow(working_version, base_val, pr, pr_map);
          auto result = table.Update(txn, tuples[base_val], *pr, *pr_map, working_version);

          EXPECT_TRUE(result.first);
          ASSERT_LE(*version, !working_version);
          if (*version != (!working_version) && (!working_version) >= 5) EXPECT_NE(tuples[base_val], result.second);
          else EXPECT_EQ(tuples[base_val], result.second);

          tuples[base_val] = result.second;

          pr = nullptr;
          delete[] buffer;
          delete pri;
          delete pr_map;
        }
        txn_manager_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
        if (id == 0) schema_version_++;
      }
    };

    MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads, workload);

    ValidateTable(table);

    for (auto &version : versioned_col_oids) delete version;
    versioned_col_oids.clear();
    // End concurrent section
    // delete init_txn;

    gc_.PerformGarbageCollection();
    gc_.PerformGarbageCollection();
    gc_.PerformGarbageCollection();
  }
}

// NOLINTNEXTLINE
TEST_F(SqlTableConcurrentTests, ConcurrentCrossVersionUpdates) {
  // const uint32_t num_iterations = 5;
  // const uint32_t num_changes = 1000;
  // const uint16_t max_columns = 20;
  // const uint32_t num_threads = MultiThreadTestUtil::HardwareConcurrency();
  // common::WorkerPool thread_pool(num_threads, {});

  // for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
  //   LOG_INFO("iteration {}", iteration);
  //   storage::SqlTable table = ConstructTable();

  //   { // Begin concurrent section
  //     auto workload = [&](uint32_t id) {
  //       transaction::TransactionContext txn = txn_manager_.BeginTransaction();
  //       // TODO: test workload logic here
  //       txn_manager_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
  //       delete txn;
  //     };

  //     MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads, workload);
  //   }// End concurrent section
  // }
}
}  // namespace terrier
