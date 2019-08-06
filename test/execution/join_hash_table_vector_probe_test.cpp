#include <algorithm>
#include <memory>
#include <random>
#include <vector>

#include "execution/sql_test.h"  // NOLINT

#include "catalog/catalog.h"
#include "execution/sql/join_hash_table.h"
#include "execution/sql/join_hash_table_vector_probe.h"
#include "execution/sql/projected_columns_iterator.h"
#include "execution/util/hash.h"
#include "storage/projected_columns.h"
#include "transaction/transaction_defs.h"
#include "type/type_id.h"

namespace terrier::sql::test {

/// This is the tuple we insert into the hash table
template <u8 N>
struct Tuple {
  u32 build_key;
  u32 aux[N];
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
    terrier::catalog::Schema::Column col_a("col_a", terrier::type::TypeId::INTEGER, false, DummyCVE());
    terrier::catalog::Schema::Column col_b("col_b", terrier::type::TypeId::INTEGER, false, DummyCVE());

    // Create the table in the catalog.
    terrier::catalog::Schema tmp_schema({col_a, col_b});
    auto table_oid = exec_ctx_->GetAccessor()->CreateTable(NSOid(), "hash_join_test_table", tmp_schema);
    auto schema = exec_ctx_->GetAccessor()->GetSchema(table_oid);
    auto sql_table = new terrier::storage::SqlTable(BlockStore(), schema);
    exec_ctx_->GetAccessor()->SetTablePointer(table_oid, sql_table);

    // Create a ProjectedColumns
    std::vector<terrier::catalog::col_oid_t> col_oids;
    for (const auto &col : schema.GetColumns()) {
      col_oids.emplace_back(col.Oid());
    }
    auto initializer_map = sql_table->InitializerForProjectedColumns(col_oids, kDefaultVectorSize);

    buffer_ = terrier::common::AllocationUtil::AllocateAligned(initializer_map.first.ProjectedColumnsSize());
    projected_columns_ = initializer_map.first.Initialize(buffer_);
    projected_columns_->SetNumTuples(kDefaultVectorSize);
  }

  // Delete allocated objects and remove the created table.
  ~JoinHashTableVectorProbeTest() override {
    delete[] buffer_;
  }

  MemoryPool *memory() { return &memory_; }

  terrier::storage::ProjectedColumns *GetProjectedColumns() { return projected_columns_; }

 protected:
  template <u8 N, typename F>
  std::unique_ptr<const JoinHashTable> InsertAndBuild(bool concise, u32 num_tuples, F &&key_gen) {
    auto jht = std::make_unique<JoinHashTable>(memory(), sizeof(Tuple<N>), concise);

    // Insert
    for (u32 i = 0; i < num_tuples; i++) {
      auto key = key_gen();
      auto hash = util::Hasher::Hash(reinterpret_cast<const u8 *>(&key), sizeof(key));
      auto *tuple = reinterpret_cast<Tuple<N> *>(jht->AllocInputTuple(hash));
      tuple->build_key = key;
    }

    // Build
    jht->Build();

    // Finish
    return jht;
  }

  template <u8 N>
  static hash_t HashTupleInPCI(ProjectedColumnsIterator *pci) noexcept {
    const auto *key_ptr = pci->Get<u32, false>(0, nullptr);
    return util::Hasher::Hash(reinterpret_cast<const u8 *>(key_ptr), sizeof(Tuple<N>::build_key));
  }

  /**
   * The function to determine whether two tuples have equivalent keys
   */
  template <u8 N>
  static bool CmpTupleInPCI(const void *table_tuple, ProjectedColumnsIterator *pci) noexcept {
    auto lhs_key = reinterpret_cast<const Tuple<N> *>(table_tuple)->build_key;
    auto rhs_key = *pci->Get<u32, false>(0, nullptr);
    return lhs_key == rhs_key;
  }

 private:
  MemoryPool memory_;
  terrier::storage::ProjectedColumns *projected_columns_{nullptr};

  byte *buffer_{nullptr};
  std::unique_ptr<exec::ExecutionContext> exec_ctx_;
};

// Sequential number functor
struct Seq {
  u32 c;
  explicit Seq(u32 cc) : c(cc) {}
  u32 operator()() noexcept { return c++; }
};

struct Range {
  std::random_device random;
  std::uniform_int_distribution<u32> dist;
  Range(u32 min, u32 max) : dist(min, max) {}
  u32 operator()() noexcept { return dist(random); }
};

// Random number functor
struct Rand {
  std::random_device random;
  Rand() = default;
  u32 operator()() noexcept { return random(); }
};

// NOLINTNEXTLINE
TEST_F(JoinHashTableVectorProbeTest, SimpleGenericLookupTest) {
  constexpr const u8 N = 1;
  constexpr const u32 num_build = 1000;
  constexpr const u32 num_probe = num_build * 10;

  // Create test JHT
  auto jht = InsertAndBuild<N>(/*concise*/ false, num_build, Seq(0));

  // Create test probe input
  auto probe_keys = std::vector<u32>(num_probe);
  std::generate(probe_keys.begin(), probe_keys.end(), Range(0, num_build - 1));

  auto *projected_columns = GetProjectedColumns();
  ProjectedColumnsIterator pci(projected_columns);

  // Lookup
  JoinHashTableVectorProbe lookup(*jht);

  // Loop over all matches
  u32 count = 0;
  for (u32 i = 0; i < num_probe; i += projected_columns->MaxTuples()) {
    u32 size = std::min(projected_columns->MaxTuples(), num_probe - i);

    // Setup Projected Column
    projected_columns->SetNumTuples(size);
    std::memcpy(projected_columns->ColumnStart(0), &probe_keys[i], size * sizeof(u32));
    pci.SetProjectedColumn(projected_columns);

    // Lookup
    lookup.Prepare(&pci, HashTupleInPCI<N>);

    // Iterate all
    while (const auto *entry = lookup.GetNextOutput(&pci, CmpTupleInPCI<N>)) {
      count++;
      auto ht_key = entry->PayloadAs<Tuple<N>>()->build_key;
      // NOTE: this would break if the columns had different sizes since the
      // storage layer might reorder them.
      auto probe_key = *pci.Get<u32, false>(0, nullptr);
      EXPECT_EQ(ht_key, probe_key);
    }
  }

  EXPECT_EQ(num_probe, count);
}

// NOLINTNEXTLINE
TEST_F(JoinHashTableVectorProbeTest, DISABLED_PerfLookupTest) {
  auto bench = [this](bool concise) {
    constexpr const u8 N = 1;
    constexpr const u32 num_build = 5000000;
    constexpr const u32 num_probe = num_build * 10;

    // Create test JHT
    auto jht = InsertAndBuild<N>(concise, num_build, Seq(0));

    // Create test probe input
    auto probe_keys = std::vector<u32>(num_probe);
    std::generate(probe_keys.begin(), probe_keys.end(), Range(0, num_build - 1));

    auto *projected_columns = GetProjectedColumns();
    ProjectedColumnsIterator pci(projected_columns);

    // Lookup
    JoinHashTableVectorProbe lookup(*jht);

    util::Timer<std::milli> timer;
    timer.Start();

    // Loop over all matches
    u32 count = 0;
    for (u32 i = 0; i < num_probe; i += kDefaultVectorSize) {
      u32 size = std::min(kDefaultVectorSize, num_probe - i);

      // Setup Projected Column
      projected_columns->SetNumTuples(size);
      std::memcpy(projected_columns->ColumnStart(0), &probe_keys[i], size * sizeof(u32));
      pci.SetProjectedColumn(projected_columns);

      // Lookup
      lookup.Prepare(&pci, HashTupleInPCI<N>);

      // Iterate all
      while (const auto *entry = lookup.GetNextOutput(&pci, CmpTupleInPCI<N>)) {
        (void)entry;
        count++;
      }
    }

    timer.Stop();
    auto mtps = (num_probe / timer.elapsed()) / 1000.0;
    EXECUTION_LOG_INFO("========== {} ==========", concise ? "Concise" : "Generic");
    EXECUTION_LOG_INFO("# Probes    : {}", num_probe)
    EXECUTION_LOG_INFO("Probe Time  : {} ms ({:.2f} Mtps)", timer.elapsed(), mtps);
  };

  bench(false);
  bench(true);
}

}  // namespace terrier::sql::test
