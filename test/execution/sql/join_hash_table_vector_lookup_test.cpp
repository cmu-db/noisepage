#include <algorithm>
#include <memory>
#include <random>
#include <vector>

#include "execution/tpl_test.h"  // NOLINT

#include "execution/catalog/catalog.h"
#include "execution/sql/execution_structures.h"
#include "execution/sql/join_hash_table.h"
#include "execution/sql/join_hash_table_vector_lookup.h"
#include "execution/sql/projected_columns_iterator.h"
#include "execution/storage/projected_columns.h"
#include "execution/util/hash.h"

namespace tpl::sql::test {
using namespace terrier;
/// This is the tuple we insert into the hash table
template <u8 N>
struct Tuple {
  u32 build_key;
  u32 aux[N];
};

template <u8 N>
static inline hash_t HashTupleInPCI(ProjectedColumnsIterator *pci) noexcept {
  const auto *key_ptr = pci->Get<u32, false>(0, nullptr);
  return util::Hasher::Hash(reinterpret_cast<const u8 *>(key_ptr), sizeof(Tuple<N>::build_key));
}

/// The function to determine whether two tuples have equivalent keys
template <u8 N>
static inline bool CmpTupleInPCI(const byte *table_tuple, ProjectedColumnsIterator *pci) noexcept {
  auto lhs_key = reinterpret_cast<const Tuple<N> *>(table_tuple)->build_key;
  auto rhs_key = *pci->Get<u32, false>(0, nullptr);
  return lhs_key == rhs_key;
}

class JoinHashTableVectorLookupTest : public TplTest {
 public:
  JoinHashTableVectorLookupTest() : region_(GetTestName()) { InitializeColumns(); }

  void InitializeColumns() {
    auto *exec = sql::ExecutionStructures::Instance();
    auto *catalog = exec->GetCatalog();
    auto *txn_manager = exec->GetTxnManager();
    txn_ = txn_manager->BeginTransaction();
    // TODO(Amadou): Come up with an easier way to create ProjectedColumns.
    // This should be done after the perso_catalog PR is merged in.
    // Create column metadata for every column.
    catalog::col_oid_t col_oid_a(catalog->GetNextOid());
    catalog::col_oid_t col_oid_b(catalog->GetNextOid());
    catalog::Schema::Column col_a = catalog::Schema::Column("col_a", type::TypeId::INTEGER, false, col_oid_a);
    catalog::Schema::Column col_b = catalog::Schema::Column("col_b", type::TypeId::INTEGER, false, col_oid_b);

    // Create the table in the catalog.
    catalog::Schema schema({col_a, col_b});
    auto table_oid = catalog->CreateTable(txn_, catalog::DEFAULT_DATABASE_OID, "hash_join_test_table", schema);

    // Get the table's information.
    catalog_table_ = catalog->GetCatalogTable(catalog::DEFAULT_DATABASE_OID, table_oid);
    auto sql_table = catalog_table_->GetSqlTable();

    // Create a ProjectedColumns
    std::vector<catalog::col_oid_t> col_oids;
    for (const auto &col : sql_table->GetSchema().GetColumns()) {
      col_oids.emplace_back(col.GetOid());
    }
    auto initializer_map = sql_table->InitializerForProjectedColumns(col_oids, kDefaultVectorSize);

    buffer_ = common::AllocationUtil::AllocateAligned(initializer_map.first.ProjectedColumnsSize());
    projected_columns_ = initializer_map.first.Initialize(buffer_);
    projected_columns_->SetNumTuples(kDefaultVectorSize);
  }

  // Delete allocated objects and remove the created table.
  ~JoinHashTableVectorLookupTest() {
    auto *exec = sql::ExecutionStructures::Instance();
    auto *catalog = exec->GetCatalog();
    auto *txn_manager = exec->GetTxnManager();
    txn_manager->Commit(txn_, [](void *) { return; }, nullptr);
    catalog->DeleteTable(txn_, catalog::DEFAULT_DATABASE_OID, catalog_table_->Oid());
    delete txn_;
    delete[] buffer_;
  }

  util::Region *region() { return &region_; }

  storage::ProjectedColumns *GetProjectedColumns() { return projected_columns_; }

 private:
  storage::ProjectedColumns *projected_columns_ = nullptr;
  util::Region region_;

  byte *buffer_ = nullptr;
  std::shared_ptr<catalog::SqlTableRW> catalog_table_ = nullptr;
  transaction::TransactionContext *txn_ = nullptr;
};

template <u8 N, typename F>
std::unique_ptr<const JoinHashTable> InsertAndBuild(util::Region *region, bool concise, u32 num_tuples, F &&key_gen) {
  auto jht = std::make_unique<JoinHashTable>(region, sizeof(Tuple<N>), concise);

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

TEST_F(JoinHashTableVectorLookupTest, SimpleGenericLookupTest) {
  constexpr const u8 N = 1;
  constexpr const u32 num_build = 1000;
  constexpr const u32 num_probe = num_build * 10;

  // Create test JHT
  auto jht = InsertAndBuild<N>(region(), /*concise*/ false, num_build, Seq(0));

  // Create test probe input
  auto probe_keys = std::vector<u32>(num_probe);
  std::generate(probe_keys.begin(), probe_keys.end(), Range(0, num_build - 1));

  auto *projected_columns = GetProjectedColumns();
  ProjectedColumnsIterator pci(projected_columns);

  // Lookup
  JoinHashTableVectorLookup lookup(*jht);

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

TEST_F(JoinHashTableVectorLookupTest, DISABLED_PerfLookupTest) {
  auto bench = [this](bool concise) {
    constexpr const u8 N = 1;
    constexpr const u32 num_build = 5000000;
    constexpr const u32 num_probe = num_build * 10;

    // Create test JHT
    auto jht = InsertAndBuild<N>(region(), concise, num_build, Seq(0));

    // Create test probe input
    auto probe_keys = std::vector<u32>(num_probe);
    std::generate(probe_keys.begin(), probe_keys.end(), Range(0, num_build - 1));

    auto *projected_columns = GetProjectedColumns();
    ProjectedColumnsIterator pci(projected_columns);

    // Lookup
    JoinHashTableVectorLookup lookup(*jht);

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

}  // namespace tpl::sql::test
