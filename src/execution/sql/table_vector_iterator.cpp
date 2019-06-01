#include <memory>
#include <vector>

#include "execution/exec/execution_context.h"
#include "execution/sql/execution_structures.h"
#include "execution/sql/table_vector_iterator.h"
#include "execution/util/timer.h"
#include "tbb/blocked_range.h"
#include "tbb/parallel_for.h"
#include "tbb/task_scheduler_init.h"

namespace tpl::sql {

using terrier::catalog::col_oid_t;
using terrier::catalog::Schema;
using terrier::common::AllocationUtil;
using terrier::storage::DataTable;
using terrier::transaction::TransactionContext;

TableVectorIterator::TableVectorIterator(u32 db_oid, u32 ns_oid, u32 table_oid, TransactionContext *txn)
    : db_oid_(db_oid), ns_oid_(ns_oid), table_oid_(table_oid), txn_(txn) {}

TableVectorIterator::~TableVectorIterator() { delete[] buffer_; }

bool TableVectorIterator::Init() {
  // Find the table
  auto *exec = ExecutionStructures::Instance();
  catalog_table_ = exec->GetCatalog()->GetUserTable(txn_, db_oid_, ns_oid_, table_oid_);
  if (catalog_table_ == nullptr) return false;

  // Initialize the projected column
  const auto &schema = catalog_table_->GetSqlTable()->GetSchema();
  std::vector<col_oid_t> col_oids{};
  const std::vector<Schema::Column> &columns = schema.GetColumns();
  col_oids.reserve(columns.size());
  for (const auto &col : columns) {
    col_oids.emplace_back(col.GetOid());
  }
  auto pri_map = catalog_table_->GetSqlTable()->InitializerForProjectedColumns(col_oids, kDefaultVectorSize);
  buffer_ = AllocationUtil::AllocateAligned(pri_map.first.ProjectedColumnsSize());
  projected_columns_ = pri_map.first.Initialize(buffer_);
  initialized = true;

  // Begin iterating
  iter_ = std::make_unique<DataTable::SlotIterator>(catalog_table_->GetSqlTable()->begin());
  return true;
}

bool TableVectorIterator::Advance() {
  if (!initialized) return false;
  // First check if the iterator ended.
  if (*iter_ == catalog_table_->GetSqlTable()->end()) {
    return false;
  }
  // Scan the table a set the projected column.
  catalog_table_->GetSqlTable()->Scan(txn_, iter_.get(), projected_columns_);
  pci_.SetProjectedColumn(projected_columns_);
  return true;
}

/*
namespace {

class ScanTask {
 public:
  ScanTask(u32 db_oid, u32 table_oid, exec::ExecutionContext *const exec_ctx,
           ThreadStateContainer *const thread_state_container,
           TableVectorIterator::ScanFn scanner)
      : db_oid_(db_oid),
        table_oid_(table_oid),
        exec_ctx_(exec_ctx),
        thread_state_container_(thread_state_container),
        scanner_(scanner) {}

  void operator()(const tbb::blocked_range<u32> &block_range) const {
    // Create the iterator over the specified block range
    TableVectorIterator iter(db_oid_, table_oid_, block_range.begin(), block_range.end());

    // Initialize it
    if (!iter.Init()) {
      return;
    }

    // Pull out the thread-local state
    byte *const thread_state =
        thread_state_container_->AccessThreadStateOfCurrentThread();

    // Call scanning function
    scanner_(exec_ctx_, thread_state, &iter);
  }

 private:
  u32 db_oid_;
  u32 table_oid_;
  exec::ExecutionContext *const exec_ctx_;
  ThreadStateContainer *const thread_state_container_;
  TableVectorIterator::ScanFn scanner_;
};

}  // namespace
*/

bool TableVectorIterator::ParallelScan(u32 db_oid, u32 table_oid, void *const query_state,
                                       ThreadStateContainer *const thread_states, const ScanFn scan_fn,
                                       const u32 min_grain_size) {
  // Lookup table
  /*const Table *table = Catalog::Instance()->LookupTableById(TableId(table_id));
  if (table == nullptr) {
    return false;
  }

  // Time
  util::Timer<> timer;
  timer.Start();

  // Execute parallel scan
  tbb::task_scheduler_init scan_scheduler;
  tbb::blocked_range<u32> block_range(0, table->num_blocks(), min_grain_size);
  tbb::parallel_for(block_range, ScanTask(table_id, exec_ctx,
                                          thread_state_container, scanner));

  timer.Stop();
  double tps = table->num_tuples() / timer.elapsed();
  LOG_INFO("Scanned {} blocks ({} tuples) blocks in {} ms ({:.2f} tps)",
           table->num_blocks(), table->num_tuples(), timer.elapsed(), tps);
  */
  return false;
}

}  // namespace tpl::sql
