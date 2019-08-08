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

void OpTableVectorIteratorConstruct(terrier::execution::sql::TableVectorIterator *iter, u32 table_oid,
                                    terrier::execution::exec::ExecutionContext *exec_ctx) {
  TPL_ASSERT(iter != nullptr, "Null iterator to initialize");
  new (iter) terrier::execution::sql::TableVectorIterator(table_oid, exec_ctx);
}

void OpTableVectorIteratorPerformInit(terrier::execution::sql::TableVectorIterator *iter) { iter->Init(); }

void OpTableVectorIteratorFree(terrier::execution::sql::TableVectorIterator *iter) {
  TPL_ASSERT(iter != nullptr, "NULL iterator given to close");
  iter->~TableVectorIterator();
}

void OpPCIFilterEqual(u64 *size, terrier::execution::sql::ProjectedColumnsIterator *iter, u32 col_idx, i8 type,
                      i64 val) {
  auto sql_type = static_cast<terrier::type::TypeId>(type);
  auto v = iter->MakeFilterVal(val, sql_type);
  *size = iter->FilterColByVal<std::equal_to>(col_idx, sql_type, v);
}

void OpPCIFilterGreaterThan(u64 *size, terrier::execution::sql::ProjectedColumnsIterator *iter, u32 col_idx, i8 type,
                            i64 val) {
  auto sql_type = static_cast<terrier::type::TypeId>(type);
  auto v = iter->MakeFilterVal(val, sql_type);
  *size = iter->FilterColByVal<std::greater>(col_idx, sql_type, v);
}

void OpPCIFilterGreaterThanEqual(u64 *size, terrier::execution::sql::ProjectedColumnsIterator *iter, u32 col_idx,
                                 i8 type, i64 val) {
  auto sql_type = static_cast<terrier::type::TypeId>(type);
  auto v = iter->MakeFilterVal(val, sql_type);
  *size = iter->FilterColByVal<std::greater_equal>(col_idx, sql_type, v);
}

void OpPCIFilterLessThan(u64 *size, terrier::execution::sql::ProjectedColumnsIterator *iter, u32 col_idx, i8 type,
                         i64 val) {
  auto sql_type = static_cast<terrier::type::TypeId>(type);
  auto v = iter->MakeFilterVal(val, sql_type);
  *size = iter->FilterColByVal<std::less>(col_idx, sql_type, v);
}

void OpPCIFilterLessThanEqual(u64 *size, terrier::execution::sql::ProjectedColumnsIterator *iter, u32 col_idx, i8 type,
                              i64 val) {
  auto sql_type = static_cast<terrier::type::TypeId>(type);
  auto v = iter->MakeFilterVal(val, sql_type);
  *size = iter->FilterColByVal<std::less_equal>(col_idx, sql_type, v);
}

void OpPCIFilterNotEqual(u64 *size, terrier::execution::sql::ProjectedColumnsIterator *iter, u32 col_idx, i8 type,
                         i64 val) {
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
                         terrier::execution::sql::MemoryPool *memory, u32 tuple_size) {
  new (join_hash_table) terrier::execution::sql::JoinHashTable(memory, tuple_size);
}

void OpJoinHashTableBuild(terrier::execution::sql::JoinHashTable *join_hash_table) { join_hash_table->Build(); }

void OpJoinHashTableBuildParallel(terrier::execution::sql::JoinHashTable *join_hash_table,
                                  terrier::execution::sql::ThreadStateContainer *thread_state_container,
                                  u32 jht_offset) {
  join_hash_table->MergeParallel(thread_state_container, jht_offset);
}

void OpJoinHashTableFree(terrier::execution::sql::JoinHashTable *join_hash_table) { join_hash_table->~JoinHashTable(); }

// ---------------------------------------------------------
// Aggregation Hash Table
// ---------------------------------------------------------

void OpAggregationHashTableInit(terrier::execution::sql::AggregationHashTable *const agg_hash_table,
                                terrier::execution::sql::MemoryPool *const memory, const u32 payload_size) {
  new (agg_hash_table) terrier::execution::sql::AggregationHashTable(memory, payload_size);
}

void OpAggregationHashTableFree(terrier::execution::sql::AggregationHashTable *const agg_hash_table) {
  agg_hash_table->~AggregationHashTable();
}

void OpAggregationHashTableIteratorInit(terrier::execution::sql::AggregationHashTableIterator *iter,
                                        terrier::execution::sql::AggregationHashTable *agg_hash_table) {
  TPL_ASSERT(agg_hash_table != nullptr, "Null hash table");
  new (iter) terrier::execution::sql::AggregationHashTableIterator(*agg_hash_table);
}

void OpAggregationHashTableIteratorFree(terrier::execution::sql::AggregationHashTableIterator *iter) {
  iter->~AggregationHashTableIterator();
}

// ---------------------------------------------------------
// Sorters
// ---------------------------------------------------------

void OpSorterInit(terrier::execution::sql::Sorter *const sorter, terrier::execution::sql::MemoryPool *const memory,
                  const terrier::execution::sql::Sorter::ComparisonFunction cmp_fn, const u32 tuple_size) {
  new (sorter) terrier::execution::sql::Sorter(memory, cmp_fn, tuple_size);
}

void OpSorterSort(terrier::execution::sql::Sorter *sorter) { sorter->Sort(); }

void OpSorterSortParallel(terrier::execution::sql::Sorter *sorter,
                          terrier::execution::sql::ThreadStateContainer *thread_state_container, u32 sorter_offset) {
  sorter->SortParallel(thread_state_container, sorter_offset);
}

void OpSorterSortTopKParallel(terrier::execution::sql::Sorter *sorter,
                              terrier::execution::sql::ThreadStateContainer *thread_state_container, u32 sorter_offset,
                              u64 top_k) {
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
void OpOutputAlloc(terrier::execution::exec::ExecutionContext *exec_ctx, byte **result) {
  *result = exec_ctx->GetOutputBuffer()->AllocOutputSlot();
}

void OpOutputAdvance(terrier::execution::exec::ExecutionContext *exec_ctx) { exec_ctx->GetOutputBuffer()->Advance(); }

void OpOutputSetNull(terrier::execution::exec::ExecutionContext *exec_ctx, u32 idx) {
  exec_ctx->GetOutputBuffer()->SetNull(static_cast<u16>(idx), true);
}

void OpOutputFinalize(terrier::execution::exec::ExecutionContext *exec_ctx) { exec_ctx->GetOutputBuffer()->Finalize(); }

// -------------------------------------------------------------
// Insert
// ------------------------------------------------------------
void OpInsert(terrier::execution::exec::ExecutionContext *exec_ctx, u32 table_oid, byte *values_ptr) {
  // TODO(Amadou): Implement me once a builtin ProjectedRow is implemented.
}

// -------------------------------------------------------------------
// Index Iterator
// -------------------------------------------------------------------
void OpIndexIteratorConstruct(terrier::execution::sql::IndexIterator *iter, uint32_t table_oid, uint32_t index_oid,
                              terrier::execution::exec::ExecutionContext *exec_ctx) {
  new (iter) terrier::execution::sql::IndexIterator(table_oid, index_oid, exec_ctx);
}

void OpIndexIteratorPerformInit(terrier::execution::sql::IndexIterator *iter) { iter->Init(); }

void OpIndexIteratorFree(terrier::execution::sql::IndexIterator *iter) { iter->~IndexIterator(); }

}  //
