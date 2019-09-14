#include <algorithm>
#include <memory>
#include <random>
#include <vector>

#include "execution/sql_test.h"

#include "catalog/catalog.h"
#include "execution/sql/join_hash_table.h"
#include "execution/sql/join_hash_table_vector_probe.h"
#include "execution/sql/projected_columns_iterator.h"
#include "execution/util/hash.h"
#include "storage/projected_columns.h"
#include "transaction/transaction_defs.h"
#include "type/type_id.h"

namespace terrier::execution::sql::test {

/// This is the tuple we insert into the hash table
template <uint8_t N>
struct Tuple {
  uint32_t build_key_;
  uint32_t aux_[N];
};

class JoinHashTableVectorProbeTest : public SqlBasedTest {
 public:
  JoinHashTableVectorProbeTest() : memory_(nullptr) {}

  void SetUp() override {
    SqlBasedTest::SetUp();
    exec_ctx_ = MakeExecCtx();
    InitializeColumns();
  }

  void InitializeColumns() {
    // TODO(Amadou): Come up with an easier way to create ProjectedColumns.
    // This should be done after the catalog PR is merged in.
    // Create column metadata for every column.
    catalog::Schema::Column col_a("col_a", type::TypeId::INTEGER, false, DummyCVE());
    catalog::Schema::Column col_b("col_b", type::TypeId::INTEGER, false, DummyCVE());

    // Create the table in the catalog.
    catalog::Schema tmp_schema({col_a, col_b});
    auto table_oid = exec_ctx_->GetAccessor()->CreateTable(NSOid(), "hash_join_test_table", tmp_schema);
    auto schema = exec_ctx_->GetAccessor()->GetSchema(table_oid);
    auto sql_table = new storage::SqlTable(BlockStore(), schema);
    exec_ctx_->GetAccessor()->SetTablePointer(table_oid, sql_table);

    // Create a ProjectedColumns
    std::vector<catalog::col_oid_t> col_oids;
    for (const auto &col : schema.GetColumns()) {
      col_oids.emplace_back(col.Oid());
    }

    auto pc_init = sql_table->InitializerForProjectedColumns(col_oids, common::Constants::K_DEFAULT_VECTOR_SIZE);
    buffer_ = common::AllocationUtil::AllocateAligned(pc_init.ProjectedColumnsSize());
    projected_columns_ = pc_init.Initialize(buffer_);
    projected_columns_->SetNumTuples(common::Constants::K_DEFAULT_VECTOR_SIZE);
  }

  // Delete allocated objects and remove the created table.
  ~JoinHashTableVectorProbeTest() override { delete[] buffer_; }

  MemoryPool *Memory() { return &memory_; }

  storage::ProjectedColumns *GetProjectedColumns() { return projected_columns_; }

 protected:
  template <uint8_t N, typename F>
  std::unique_ptr<const JoinHashTable> InsertAndBuild(bool concise, uint32_t num_tuples, F &&key_gen) {
    auto jht = std::make_unique<JoinHashTable>(Memory(), sizeof(Tuple<N>), concise);

    // Insert
    for (uint32_t i = 0; i < num_tuples; i++) {
      auto key = key_gen();
      auto hash = util::Hasher::Hash(reinterpret_cast<const uint8_t *>(&key), sizeof(key));
      auto *tuple = reinterpret_cast<Tuple<N> *>(jht->AllocInputTuple(hash));
      tuple->build_key_ = key;
    }

    // Build
    jht->Build();

    // Finish
    return jht;
  }

  template <uint8_t N>
  static hash_t HashTupleInPCI(ProjectedColumnsIterator *pci) noexcept {
    const auto *key_ptr = pci->Get<uint32_t, false>(0, nullptr);
    return util::Hasher::Hash(reinterpret_cast<const uint8_t *>(key_ptr), sizeof(Tuple<N>::build_key_));
  }

  /**
   * The function to determine whether two tuples have equivalent keys
   */
  template <uint8_t N>
  static bool CmpTupleInPCI(const void *table_tuple, ProjectedColumnsIterator *pci) noexcept {
    auto lhs_key = reinterpret_cast<const Tuple<N> *>(table_tuple)->build_key_;
    auto rhs_key = *pci->Get<uint32_t, false>(0, nullptr);
    return lhs_key == rhs_key;
  }

 private:
  MemoryPool memory_;
  storage::ProjectedColumns *projected_columns_{nullptr};

  byte *buffer_{nullptr};
  std::unique_ptr<exec::ExecutionContext> exec_ctx_;
};

// Sequential number functor
struct Seq {
  uint32_t c_;
  explicit Seq(uint32_t cc) : c_(cc) {}
  uint32_t operator()() noexcept { return c_++; }
};

struct Range {
  std::random_device random_;
  std::uniform_int_distribution<uint32_t> dist_;
  Range(uint32_t min, uint32_t max) : dist_(min, max) {}
  uint32_t operator()() noexcept { return dist_(random_); }
};

// Random number functor
struct Rand {
  std::random_device random_;
  Rand() = default;
  uint32_t operator()() noexcept { return random_(); }
};

// NOLINTNEXTLINE
TEST_F(JoinHashTableVectorProbeTest, SimpleGenericLookupTest) {
  constexpr const uint8_t n = 1;
  constexpr const uint32_t num_build = 1000;
  constexpr const uint32_t num_probe = num_build * 10;

  // Create test JHT
  auto jht = InsertAndBuild<n>(/*concise*/ false, num_build, Seq(0));

  // Create test probe input
  auto probe_keys = std::vector<uint32_t>(num_probe);
  std::generate(probe_keys.begin(), probe_keys.end(), Range(0, num_build - 1));

  auto *projected_columns = GetProjectedColumns();
  ProjectedColumnsIterator pci(projected_columns);

  // Lookup
  JoinHashTableVectorProbe lookup(*jht);

  // Loop over all matches
  uint32_t count = 0;
  for (uint32_t i = 0; i < num_probe; i += projected_columns->MaxTuples()) {
    uint32_t size = std::min(projected_columns->MaxTuples(), num_probe - i);

    // Setup Projected Column
    projected_columns->SetNumTuples(size);
    std::memcpy(projected_columns->ColumnStart(0), &probe_keys[i], size * sizeof(uint32_t));
    pci.SetProjectedColumn(projected_columns);

    // Lookup
    lookup.Prepare(&pci, HashTupleInPCI<n>);

    // Iterate all
    while (const auto *entry = lookup.GetNextOutput(&pci, CmpTupleInPCI<n>)) {
      count++;
      auto ht_key = entry->PayloadAs<Tuple<n>>()->build_key_;
      // NOTE: this would break if the columns had different sizes_ since the
      // storage layer might reorder them.
      auto probe_key = *pci.Get<uint32_t, false>(0, nullptr);
      EXPECT_EQ(ht_key, probe_key);
    }
  }

  EXPECT_EQ(num_probe, count);
}

// NOLINTNEXTLINE
TEST_F(JoinHashTableVectorProbeTest, DISABLED_PerfLookupTest) {
  auto bench = [this](bool concise) {
    constexpr const uint8_t n = 1;
    constexpr const uint32_t num_build = 5000000;
    constexpr const uint32_t num_probe = num_build * 10;

    // Create test JHT
    auto jht = InsertAndBuild<n>(concise, num_build, Seq(0));

    // Create test probe input
    auto probe_keys = std::vector<uint32_t>(num_probe);
    std::generate(probe_keys.begin(), probe_keys.end(), Range(0, num_build - 1));

    auto *projected_columns = GetProjectedColumns();
    ProjectedColumnsIterator pci(projected_columns);

    // Lookup
    JoinHashTableVectorProbe lookup(*jht);

    util::Timer<std::milli> timer;
    timer.Start();

    // Loop over all matches
    uint32_t count = 0;
    for (uint32_t i = 0; i < num_probe; i += common::Constants::K_DEFAULT_VECTOR_SIZE) {
      uint32_t size = std::min(common::Constants::K_DEFAULT_VECTOR_SIZE, num_probe - i);

      // Setup Projected Column
      projected_columns->SetNumTuples(size);
      std::memcpy(projected_columns->ColumnStart(0), &probe_keys[i], size * sizeof(uint32_t));
      pci.SetProjectedColumn(projected_columns);

      // Lookup
      lookup.Prepare(&pci, HashTupleInPCI<n>);

      // Iterate all
      while (const auto *entry = lookup.GetNextOutput(&pci, CmpTupleInPCI<n>)) {
        (void)entry;
        count++;
      }
    }

    timer.Stop();
    auto mtps = (num_probe / timer.Elapsed()) / 1000.0;
    EXECUTION_LOG_INFO("========== {} ==========", concise ? "Concise" : "Generic");
    EXECUTION_LOG_INFO("# Probes    : {}", num_probe)
    EXECUTION_LOG_INFO("Probe Time  : {} ms ({:.2f} Mtps)", timer.Elapsed(), mtps);
  };

  bench(false);
  bench(true);
}

}  // namespace terrier::execution::sql::test
