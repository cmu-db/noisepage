#include <vector>
#include <algorithm>
#include <random>

#include "benchmark/benchmark.h"
#include "execution/sql/join_hash_table.h"
#include "execution/sql/vector.h"
#include "execution/sql/vector_operations/vector_operations.h"
#include "execution/exec/execution_settings.h"
#include "execution/exec/execution_context.h"
#include "execution/sql_test.h"
#include "common/hash_util.h"
#include "benchmark_util/sfc_gen.h"

namespace noisepage::execution {

template <uint64_t N>
struct Tuple {
  uint64_t key;
  uint64_t vals[N];
  Tuple() = default;
  Tuple(uint64_t key) : key(key) {}
  hash_t Hash() const { return common::HashUtil::Hash(key); }
};

namespace {

class DatabaseObject {
 public:
  DatabaseObject() {
    noisepage::LoggersUtil::Initialize();
    db_main_ = noisepage::DBMain::Builder().SetUseGC(true).SetUseGCThread(true).SetUseCatalog(true).Build();
    metrics_manager_ = db_main_->GetMetricsManager();

    block_store_ = db_main_->GetStorageLayer()->GetBlockStore();
    catalog_ = db_main_->GetCatalogLayer()->GetCatalog();
    txn_manager_ = db_main_->GetTransactionLayer()->GetTransactionManager();

    test_txn_ = txn_manager_->BeginTransaction();

    // Create catalog and test namespace
    test_db_oid_ = catalog_->CreateDatabase(common::ManagedPointer(test_txn_), "test_db", true);
    accessor_ = catalog_->GetAccessor(common::ManagedPointer(test_txn_), test_db_oid_, DISABLED);
    test_ns_oid_ = accessor_->GetDefaultNamespace();

    exec_settings_ = std::make_unique<exec::ExecutionSettings>();
  }

  ~DatabaseObject() { txn_manager_->Commit(test_txn_, transaction::TransactionUtil::EmptyCallback, nullptr); }

  catalog::namespace_oid_t NSOid() { return test_ns_oid_; }

  common::ManagedPointer<storage::BlockStore> BlockStore() { return block_store_; }

  std::unique_ptr<exec::ExecutionContext> MakeExecCtx(exec::OutputCallback *callback = nullptr,
                                                      const planner::OutputSchema *schema = nullptr) {
    exec::OutputCallback empty = nullptr;
    const auto &callback_ref = (callback == nullptr) ? empty : *callback;
    return std::make_unique<exec::ExecutionContext>(test_db_oid_, common::ManagedPointer(test_txn_), callback_ref,
                                                    schema, common::ManagedPointer(accessor_), *exec_settings_,
                                                    metrics_manager_);
  }

  void GenerateTestTables(exec::ExecutionContext *exec_ctx) {
    sql::TableGenerator table_generator{exec_ctx, block_store_, test_ns_oid_};
    table_generator.GenerateTestTables();
  }

  parser::ConstantValueExpression DummyCVE() {
    return parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(0));
  }

  std::unique_ptr<noisepage::catalog::CatalogAccessor> MakeAccessor() {
    return catalog_->GetAccessor(common::ManagedPointer(test_txn_), test_db_oid_, DISABLED);
  }

 private:
  std::unique_ptr<DBMain> db_main_;
  common::ManagedPointer<metrics::MetricsManager> metrics_manager_;
  common::ManagedPointer<storage::BlockStore> block_store_;
  common::ManagedPointer<catalog::Catalog> catalog_;
  common::ManagedPointer<transaction::TransactionManager> txn_manager_;
  catalog::db_oid_t test_db_oid_{0};
  catalog::namespace_oid_t test_ns_oid_;
  transaction::TransactionContext *test_txn_;
  std::unique_ptr<catalog::CatalogAccessor> accessor_;
  std::unique_ptr<exec::ExecutionSettings> exec_settings_;
};


template <uint64_t N>
std::unique_ptr<sql::JoinHashTable> CreateAndPopulateJoinHashTable(exec::ExecutionContext *exec_context,
                                                                   uint64_t num_tuples,
                                                                   bool concise) {
  std::unique_ptr<sql::JoinHashTable> jht;
  exec::ExecutionSettings exec_settings{};
  jht = std::make_unique<sql::JoinHashTable>(exec_settings, exec_context, sizeof(Tuple<N>), concise);

  SFC64 gen(std::random_device{}());
  std::uniform_int_distribution<uint64_t> dist(0, num_tuples);
  for (uint32_t i = 0; i < num_tuples; i++) {
    Tuple<N> tuple(dist(gen));
    *reinterpret_cast<Tuple<N> *>(jht->AllocInputTuple(tuple.Hash())) = tuple;
  }

  return jht;
}

}  // namespace

template <uint64_t N>
static void BM_Base(benchmark::State &state) {
  const uint64_t num_tuples = state.range(0);
  DatabaseObject database_object;

  for (auto _ : state) {
    auto exec_context = database_object.MakeExecCtx();
    auto jht = CreateAndPopulateJoinHashTable<N>(exec_context.get(), num_tuples, false);
  }
}

template <uint64_t N, bool Concise>
static void BM_Build(benchmark::State &state) {
  const uint64_t num_tuples = state.range(0);
  DatabaseObject database_object;

  // Make build tuples.
  SFC64 gen(std::random_device{}());
  std::uniform_int_distribution<uint64_t> dist(0, num_tuples);
  std::vector<Tuple<N>> probe_tuples(num_tuples * 50);
  std::generate(probe_tuples.begin(), probe_tuples.end(), [&]() { return dist(gen);});

  for (auto _ : state) {
    auto exec_context = database_object.MakeExecCtx();
    // Populate.
    auto jht = CreateAndPopulateJoinHashTable<N>(exec_context.get(), num_tuples, Concise);
    // Build.
    jht->Build();
    // Probe.
    uint64_t count = 0;
    for (const auto &probe : probe_tuples) {
      count += (jht->template Lookup<Concise>(probe.Hash()).HasNext());
      //jht->template Lookup<Concise>(probe.Hash()).HasNext();
      //jht->template Lookup<Concise>(probe.Hash());
    }
    //benchmark::DoNotOptimize(count);
    state.counters["Probes"] = count;
    //printf("Allocated memory: %ld\n", exec_context->GetMemoryPool()->GetTracker()->GetAllocatedSize());
  }
}

// ---------------------------------------------------------
//
// Benchmark Configs
//
// ---------------------------------------------------------

static void CustomArguments(benchmark::internal::Benchmark *b) {
  for (int64_t i = 18; i < 21; i++) {
    b->Arg(1 << i);
  }
}

// ---------------------------------------------------------
// Tuple Size = 16
BENCHMARK_TEMPLATE(BM_Base, 1)->Apply(CustomArguments)->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_Build, 1, false)->Apply(CustomArguments)->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_Build, 1, true)->Apply(CustomArguments)->Unit(benchmark::kMillisecond);

// ---------------------------------------------------------
// Tuple Size = 24
BENCHMARK_TEMPLATE(BM_Base, 2)->Apply(CustomArguments)->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_Build, 2, false)->Apply(CustomArguments)->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_Build, 2, true)->Apply(CustomArguments)->Unit(benchmark::kMillisecond);

// ---------------------------------------------------------
// Tuple Size = 40
BENCHMARK_TEMPLATE(BM_Base, 4)->Apply(CustomArguments)->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_Build, 4, false)->Apply(CustomArguments)->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_Build, 4, true)->Apply(CustomArguments)->Unit(benchmark::kMillisecond);

// ---------------------------------------------------------
// Tuple Size = 72
BENCHMARK_TEMPLATE(BM_Base, 8)->Apply(CustomArguments)->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_Build, 8, false)->Apply(CustomArguments)->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_Build, 8, true)->Apply(CustomArguments)->Unit(benchmark::kMillisecond);

// ---------------------------------------------------------
// Tuple Size = 136
BENCHMARK_TEMPLATE(BM_Base, 16)->Apply(CustomArguments)->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_Build, 16, false)->Apply(CustomArguments)->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_Build, 16, true)->Apply(CustomArguments)->Unit(benchmark::kMillisecond);

}  // namespace noisepage::execution
