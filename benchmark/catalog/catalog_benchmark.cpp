#include <utility>
#include <vector>

#include "benchmark/benchmark.h"
#include "catalog/catalog.h"
#include "catalog/catalog_defs.h"
#include "common/scoped_timer.h"
#include "transaction/transaction_manager.h"
#include "util/transaction_benchmark_util.h"

namespace terrier {

class CatalogBenchmark : public benchmark::Fixture {
 public:
  void SetUp(const benchmark::State &state) final {
    txn_manager_ = new transaction::TransactionManager(&buffer_pool_, true, LOGGING_DISABLED);

    catalog_ = new catalog::Catalog(txn_manager_);
  }

  void TearDown(const benchmark::State &state) final {
    delete catalog_;  // need to delete catalog_first
    delete txn_manager_;
  }

  // transaction manager
  transaction::TransactionManager *txn_manager_;
  storage::RecordBufferSegmentPool buffer_pool_{100, 100};

  transaction::TransactionContext *txn_;

  catalog::Catalog *catalog_;

  const int num_lookups = 100000;
};

// NOLINTNEXTLINE
/*BENCHMARK_DEFINE_F(CatalogBenchmark, DatabaseLookupTime)(benchmark::State &state) {
  std::vector<type::Value> search_vec, ret_row;
  // setup search vector, lookup the default database
  search_vec.push_back(type::ValueFactory::GetNullValue(type::TypeId::INTEGER));
  search_vec.push_back(type::ValueFactory::GetVarcharValue("terrier"));

  catalog::DatabaseHandle db_handle = catalog_->GetDatabaseHandle();
  txn_ = txn_manager_->BeginTransaction();

  // NOLINTNEXTLINE
  for (auto _ : state) {
    for (int32_t iter = 0; iter < num_lookups; iter++) {
      // TODO(pakhtar): replace with GetDatabaseEntry(from name);
      // ret_row = db_handle.pg_database_rw_->FindRow(txn_, search_vec);
      auto entry = db_handle.GetDatabaseEntry(txn_, "terrier");
    }
  }
  state.SetItemsProcessed(state.iterations() * num_lookups);
  txn_manager_->Commit(txn_, TestCallbacks::EmptyCallback, nullptr);
  delete txn_;
}

BENCHMARK_REGISTER_F(CatalogBenchmark, DatabaseLookupTime)->Unit(benchmark::kMillisecond)->MinTime(2);*/

}  // namespace terrier
