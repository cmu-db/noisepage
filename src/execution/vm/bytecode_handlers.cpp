#include "execution/vm/bytecode_handlers.h"
#include "execution/sql/projected_columns_iterator.h"

#include "catalog/catalog_defs.h"
#include "execution/exec/execution_context.h"

extern "C" {

// ---------------------------------------------------------
// Thread State Container
// ---------------------------------------------------------

void OpThreadStateContainerInit(terrier::execution::sql::ThreadStateContainer *const thread_state_container,
                                terrier::execution::sql::MemoryPool *const memory) {
  new (thread_state_container) terrier::execution::sql::ThreadStateContainer(memory);
}

void OpThreadStateContainerFree(terrier::execution::sql::ThreadStateContainer *const thread_state_container) {
  thread_state_container->~ThreadStateContainer();
}

// ---------------------------------------------------------
// Table Vector Iterator
// ---------------------------------------------------------

void OpTableVectorIteratorInit(terrier::execution::sql::TableVectorIterator *iter,
                               terrier::execution::exec::ExecutionContext *exec_ctx, uint32_t table_oid,
                               uint32_t *col_oids, uint32_t num_oids) {
  TERRIER_ASSERT(iter != nullptr, "Null iterator to initialize");
  new (iter) terrier::execution::sql::TableVectorIterator(exec_ctx, table_oid, col_oids, num_oids);
}

void OpTableVectorIteratorPerformInit(terrier::execution::sql::TableVectorIterator *iter) { iter->Init(); }

void OpTableVectorIteratorReset(terrier::execution::sql::TableVectorIterator *iter) {
  TERRIER_ASSERT(iter != nullptr, "NULL iterator given to reset");
  iter->Reset();
}

void OpTableVectorIteratorFree(terrier::execution::sql::TableVectorIterator *iter) {
  TERRIER_ASSERT(iter != nullptr, "NULL iterator given to close");
  iter->~TableVectorIterator();
}

void OpPCIFilterEqual(uint64_t *size, terrier::execution::sql::ProjectedColumnsIterator *iter, uint32_t col_idx,
                      int8_t type, int64_t val) {
  auto sql_type = static_cast<terrier::type::TypeId>(type);
  auto v = iter->MakeFilterVal(val, sql_type);
  *size = iter->FilterColByVal<std::equal_to>(col_idx, sql_type, v);
}

void OpPCIFilterGreaterThan(uint64_t *size, terrier::execution::sql::ProjectedColumnsIterator *iter, uint32_t col_idx,
                            int8_t type, int64_t val) {
  auto sql_type = static_cast<terrier::type::TypeId>(type);
  auto v = iter->MakeFilterVal(val, sql_type);
  *size = iter->FilterColByVal<std::greater>(col_idx, sql_type, v);
}

void OpPCIFilterGreaterThanEqual(uint64_t *size, terrier::execution::sql::ProjectedColumnsIterator *iter,
                                 uint32_t col_idx, int8_t type, int64_t val) {
  auto sql_type = static_cast<terrier::type::TypeId>(type);
  auto v = iter->MakeFilterVal(val, sql_type);
  *size = iter->FilterColByVal<std::greater_equal>(col_idx, sql_type, v);
}

void OpPCIFilterLessThan(uint64_t *size, terrier::execution::sql::ProjectedColumnsIterator *iter, uint32_t col_idx,
                         int8_t type, int64_t val) {
  auto sql_type = static_cast<terrier::type::TypeId>(type);
  auto v = iter->MakeFilterVal(val, sql_type);
  *size = iter->FilterColByVal<std::less>(col_idx, sql_type, v);
}

void OpPCIFilterLessThanEqual(uint64_t *size, terrier::execution::sql::ProjectedColumnsIterator *iter, uint32_t col_idx,
                              int8_t type, int64_t val) {
  auto sql_type = static_cast<terrier::type::TypeId>(type);
  auto v = iter->MakeFilterVal(val, sql_type);
  *size = iter->FilterColByVal<std::less_equal>(col_idx, sql_type, v);
}

void OpPCIFilterNotEqual(uint64_t *size, terrier::execution::sql::ProjectedColumnsIterator *iter, uint32_t col_idx,
                         int8_t type, int64_t val) {
  auto sql_type = static_cast<terrier::type::TypeId>(type);
  auto v = iter->MakeFilterVal(val, sql_type);
  *size = iter->FilterColByVal<std::not_equal_to>(col_idx, sql_type, v);
}

// ---------------------------------------------------------
// Filter Manager
// ---------------------------------------------------------

void OpFilterManagerInit(terrier::execution::sql::FilterManager *filter_manager) {
  new (filter_manager) terrier::execution::sql::FilterManager();
}

void OpFilterManagerStartNewClause(terrier::execution::sql::FilterManager *filter_manager) {
  filter_manager->StartNewClause();
}

void OpFilterManagerInsertFlavor(terrier::execution::sql::FilterManager *filter_manager,
                                 terrier::execution::sql::FilterManager::MatchFn flavor) {
  filter_manager->InsertClauseFlavor(flavor);
}

void OpFilterManagerFinalize(terrier::execution::sql::FilterManager *filter_manager) { filter_manager->Finalize(); }

void OpFilterManagerRunFilters(terrier::execution::sql::FilterManager *filter_manager,
                               terrier::execution::sql::ProjectedColumnsIterator *pci) {
  filter_manager->RunFilters(pci);
}

void OpFilterManagerFree(terrier::execution::sql::FilterManager *filter_manager) { filter_manager->~FilterManager(); }

// ---------------------------------------------------------
// Join Hash Table
// ---------------------------------------------------------

void OpJoinHashTableInit(terrier::execution::sql::JoinHashTable *join_hash_table,
                         terrier::execution::sql::MemoryPool *memory, uint32_t tuple_size) {
  new (join_hash_table) terrier::execution::sql::JoinHashTable(memory, tuple_size);
}

void OpJoinHashTableBuild(terrier::execution::sql::JoinHashTable *join_hash_table) { join_hash_table->Build(); }

void OpJoinHashTableBuildParallel(terrier::execution::sql::JoinHashTable *join_hash_table,
                                  terrier::execution::sql::ThreadStateContainer *thread_state_container,
                                  uint32_t jht_offset) {
  join_hash_table->MergeParallel(thread_state_container, jht_offset);
}

void OpJoinHashTableFree(terrier::execution::sql::JoinHashTable *join_hash_table) { join_hash_table->~JoinHashTable(); }

// ---------------------------------------------------------
// Aggregation Hash Table
// ---------------------------------------------------------

void OpAggregationHashTableInit(terrier::execution::sql::AggregationHashTable *const agg_hash_table,
                                terrier::execution::sql::MemoryPool *const memory, const uint32_t payload_size) {
  new (agg_hash_table) terrier::execution::sql::AggregationHashTable(memory, payload_size);
}

void OpAggregationHashTableFree(terrier::execution::sql::AggregationHashTable *const agg_hash_table) {
  agg_hash_table->~AggregationHashTable();
}

void OpAggregationHashTableIteratorInit(terrier::execution::sql::AggregationHashTableIterator *iter,
                                        terrier::execution::sql::AggregationHashTable *agg_hash_table) {
  TERRIER_ASSERT(agg_hash_table != nullptr, "Null hash table");
  new (iter) terrier::execution::sql::AggregationHashTableIterator(*agg_hash_table);
}

void OpAggregationHashTableIteratorFree(terrier::execution::sql::AggregationHashTableIterator *iter) {
  iter->~AggregationHashTableIterator();
}

// ---------------------------------------------------------
// Sorters
// ---------------------------------------------------------

void OpSorterInit(terrier::execution::sql::Sorter *const sorter, terrier::execution::sql::MemoryPool *const memory,
                  const terrier::execution::sql::Sorter::ComparisonFunction cmp_fn, const uint32_t tuple_size) {
  new (sorter) terrier::execution::sql::Sorter(memory, cmp_fn, tuple_size);
}

void OpSorterSort(terrier::execution::sql::Sorter *sorter) { sorter->Sort(); }

void OpSorterSortParallel(terrier::execution::sql::Sorter *sorter,
                          terrier::execution::sql::ThreadStateContainer *thread_state_container,
                          uint32_t sorter_offset) {
  sorter->SortParallel(thread_state_container, sorter_offset);
}

void OpSorterSortTopKParallel(terrier::execution::sql::Sorter *sorter,
                              terrier::execution::sql::ThreadStateContainer *thread_state_container,
                              uint32_t sorter_offset, uint64_t top_k) {
  sorter->SortTopKParallel(thread_state_container, sorter_offset, top_k);
}

void OpSorterFree(terrier::execution::sql::Sorter *sorter) { sorter->~Sorter(); }

void OpSorterIteratorInit(terrier::execution::sql::SorterIterator *iter, terrier::execution::sql::Sorter *sorter) {
  new (iter) terrier::execution::sql::SorterIterator(sorter);
}

void OpSorterIteratorFree(terrier::execution::sql::SorterIterator *iter) { iter->~SorterIterator(); }
// -------------------------------------------------------------
// Output
// ------------------------------------------------------------
void OpOutputAlloc(terrier::execution::exec::ExecutionContext *exec_ctx, terrier::byte **result) {
  *result = exec_ctx->GetOutputBuffer()->AllocOutputSlot();
}

void OpOutputFinalize(terrier::execution::exec::ExecutionContext *exec_ctx) { exec_ctx->GetOutputBuffer()->Finalize(); }

// -------------------------------------------------------------------
// Index Iterator
// -------------------------------------------------------------------
void OpIndexIteratorInit(terrier::execution::sql::IndexIterator *iter,
                         terrier::execution::exec::ExecutionContext *exec_ctx, uint32_t table_oid, uint32_t index_oid,
                         uint32_t *col_oids, uint32_t num_oids) {
  new (iter) terrier::execution::sql::IndexIterator(exec_ctx, table_oid, index_oid, col_oids, num_oids);
}

void OpIndexIteratorPerformInit(terrier::execution::sql::IndexIterator *iter) { iter->Init(); }

void OpIndexIteratorFree(terrier::execution::sql::IndexIterator *iter) { iter->~IndexIterator(); }

}  //
