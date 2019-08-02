#include "execution/vm/bytecode_handlers.h"
#include "execution/sql/projected_columns_iterator.h"

#include "catalog/catalog_defs.h"
#include "execution/exec/execution_context.h"

extern "C" {

// ---------------------------------------------------------
// Thread State Container
// ---------------------------------------------------------

void OpThreadStateContainerInit(tpl::sql::ThreadStateContainer *const thread_state_container,
                                tpl::sql::MemoryPool *const memory) {
  new (thread_state_container) tpl::sql::ThreadStateContainer(memory);
}

void OpThreadStateContainerFree(tpl::sql::ThreadStateContainer *const thread_state_container) {
  thread_state_container->~ThreadStateContainer();
}

// ---------------------------------------------------------
// Table Vector Iterator
// ---------------------------------------------------------

void OpTableVectorIteratorConstruct(tpl::sql::TableVectorIterator *iter, u32 table_oid,
                               tpl::exec::ExecutionContext *exec_ctx) {
  TPL_ASSERT(iter != nullptr, "Null iterator to initialize");
  new (iter) tpl::sql::TableVectorIterator(table_oid, exec_ctx);
}



void OpTableVectorIteratorPerformInit(tpl::sql::TableVectorIterator *iter) { iter->Init(); }

void OpTableVectorIteratorFree(tpl::sql::TableVectorIterator *iter) {
  TPL_ASSERT(iter != nullptr, "NULL iterator given to close");
  iter->~TableVectorIterator();
}

void OpPCIFilterEqual(u64 *size, tpl::sql::ProjectedColumnsIterator *iter, u32 col_idx, i8 type, i64 val) {
  auto sql_type = static_cast<terrier::type::TypeId>(type);
  auto v = iter->MakeFilterVal(val, sql_type);
  *size = iter->FilterColByVal<std::equal_to>(col_idx, sql_type, v);
}

void OpPCIFilterGreaterThan(u64 *size, tpl::sql::ProjectedColumnsIterator *iter, u32 col_idx, i8 type, i64 val) {
  auto sql_type = static_cast<terrier::type::TypeId>(type);
  auto v = iter->MakeFilterVal(val, sql_type);
  *size = iter->FilterColByVal<std::greater>(col_idx, sql_type, v);
}

void OpPCIFilterGreaterThanEqual(u64 *size, tpl::sql::ProjectedColumnsIterator *iter, u32 col_idx, i8 type, i64 val) {
  auto sql_type = static_cast<terrier::type::TypeId>(type);
  auto v = iter->MakeFilterVal(val, sql_type);
  *size = iter->FilterColByVal<std::greater_equal>(col_idx, sql_type, v);
}

void OpPCIFilterLessThan(u64 *size, tpl::sql::ProjectedColumnsIterator *iter, u32 col_idx, i8 type, i64 val) {
  auto sql_type = static_cast<terrier::type::TypeId>(type);
  auto v = iter->MakeFilterVal(val, sql_type);
  *size = iter->FilterColByVal<std::less>(col_idx, sql_type, v);
}

void OpPCIFilterLessThanEqual(u64 *size, tpl::sql::ProjectedColumnsIterator *iter, u32 col_idx, i8 type, i64 val) {
  auto sql_type = static_cast<terrier::type::TypeId>(type);
  auto v = iter->MakeFilterVal(val, sql_type);
  *size = iter->FilterColByVal<std::less_equal>(col_idx, sql_type, v);
}

void OpPCIFilterNotEqual(u64 *size, tpl::sql::ProjectedColumnsIterator *iter, u32 col_idx, i8 type, i64 val) {
  auto sql_type = static_cast<terrier::type::TypeId>(type);
  auto v = iter->MakeFilterVal(val, sql_type);
  *size = iter->FilterColByVal<std::not_equal_to>(col_idx, sql_type, v);
}

// ---------------------------------------------------------
// Filter Manager
// ---------------------------------------------------------

void OpFilterManagerInit(tpl::sql::FilterManager *filter_manager) { new (filter_manager) tpl::sql::FilterManager(); }

void OpFilterManagerStartNewClause(tpl::sql::FilterManager *filter_manager) { filter_manager->StartNewClause(); }

void OpFilterManagerInsertFlavor(tpl::sql::FilterManager *filter_manager, tpl::sql::FilterManager::MatchFn flavor) {
  filter_manager->InsertClauseFlavor(flavor);
}

void OpFilterManagerFinalize(tpl::sql::FilterManager *filter_manager) { filter_manager->Finalize(); }

void OpFilterManagerRunFilters(tpl::sql::FilterManager *filter_manager, tpl::sql::ProjectedColumnsIterator *pci) {
  filter_manager->RunFilters(pci);
}

void OpFilterManagerFree(tpl::sql::FilterManager *filter_manager) { filter_manager->~FilterManager(); }

// ---------------------------------------------------------
// Join Hash Table
// ---------------------------------------------------------

void OpJoinHashTableInit(tpl::sql::JoinHashTable *join_hash_table, tpl::sql::MemoryPool *memory, u32 tuple_size) {
  new (join_hash_table) tpl::sql::JoinHashTable(memory, tuple_size);
}

void OpJoinHashTableBuild(tpl::sql::JoinHashTable *join_hash_table) { join_hash_table->Build(); }

void OpJoinHashTableBuildParallel(tpl::sql::JoinHashTable *join_hash_table,
                                  tpl::sql::ThreadStateContainer *thread_state_container, u32 jht_offset) {
  join_hash_table->MergeParallel(thread_state_container, jht_offset);
}

void OpJoinHashTableFree(tpl::sql::JoinHashTable *join_hash_table) { join_hash_table->~JoinHashTable(); }

// ---------------------------------------------------------
// Aggregation Hash Table
// ---------------------------------------------------------

void OpAggregationHashTableInit(tpl::sql::AggregationHashTable *const agg_hash_table,
                                tpl::sql::MemoryPool *const memory, const u32 payload_size) {
  new (agg_hash_table) tpl::sql::AggregationHashTable(memory, payload_size);
}

void OpAggregationHashTableFree(tpl::sql::AggregationHashTable *const agg_hash_table) {
  agg_hash_table->~AggregationHashTable();
}

void OpAggregationHashTableIteratorInit(tpl::sql::AggregationHashTableIterator *iter,
                                        tpl::sql::AggregationHashTable *agg_hash_table) {
  TPL_ASSERT(agg_hash_table != nullptr, "Null hash table");
  new (iter) tpl::sql::AggregationHashTableIterator(*agg_hash_table);
}

void OpAggregationHashTableIteratorFree(tpl::sql::AggregationHashTableIterator *iter) {
  iter->~AggregationHashTableIterator();
}

// ---------------------------------------------------------
// Sorters
// ---------------------------------------------------------

void OpSorterInit(tpl::sql::Sorter *const sorter, tpl::sql::MemoryPool *const memory,
                  const tpl::sql::Sorter::ComparisonFunction cmp_fn, const u32 tuple_size) {
  new (sorter) tpl::sql::Sorter(memory, cmp_fn, tuple_size);
}

void OpSorterSort(tpl::sql::Sorter *sorter) { sorter->Sort(); }

void OpSorterSortParallel(tpl::sql::Sorter *sorter, tpl::sql::ThreadStateContainer *thread_state_container,
                          u32 sorter_offset) {
  sorter->SortParallel(thread_state_container, sorter_offset);
}

void OpSorterSortTopKParallel(tpl::sql::Sorter *sorter, tpl::sql::ThreadStateContainer *thread_state_container,
                              u32 sorter_offset, u64 top_k) {
  sorter->SortTopKParallel(thread_state_container, sorter_offset, top_k);
}

void OpSorterFree(tpl::sql::Sorter *sorter) { sorter->~Sorter(); }

void OpSorterIteratorInit(tpl::sql::SorterIterator *iter, tpl::sql::Sorter *sorter) {
  new (iter) tpl::sql::SorterIterator(sorter);
}

void OpSorterIteratorFree(tpl::sql::SorterIterator *iter) { iter->~SorterIterator(); }
// -------------------------------------------------------------
// Output
// ------------------------------------------------------------
void OpOutputAlloc(tpl::exec::ExecutionContext *exec_ctx, byte **result) {
  *result = exec_ctx->GetOutputBuffer()->AllocOutputSlot();
}

void OpOutputAdvance(tpl::exec::ExecutionContext *exec_ctx) { exec_ctx->GetOutputBuffer()->Advance(); }

void OpOutputSetNull(tpl::exec::ExecutionContext *exec_ctx, u32 idx) {
  exec_ctx->GetOutputBuffer()->SetNull(static_cast<u16>(idx), true);
}

void OpOutputFinalize(tpl::exec::ExecutionContext *exec_ctx) { exec_ctx->GetOutputBuffer()->Finalize(); }

// -------------------------------------------------------------
// Insert
// ------------------------------------------------------------
void OpInsert(tpl::exec::ExecutionContext *exec_ctx, u32 table_oid, byte *values_ptr) {
  // TODO(Amadou): Implement me once a builtin ProjectedRow is implemented.
}

// -------------------------------------------------------------------
// Index Iterator
// -------------------------------------------------------------------
void OpIndexIteratorConstruct(tpl::sql::IndexIterator *iter, uint32_t table_oid, uint32_t index_oid,
                         tpl::exec::ExecutionContext *exec_ctx) {
  new (iter) tpl::sql::IndexIterator(table_oid, index_oid, exec_ctx);
}

void OpIndexIteratorPerformInit(tpl::sql::IndexIterator *iter) { iter->Init(); }


void OpIndexIteratorFree(tpl::sql::IndexIterator *iter) { iter->~IndexIterator(); }

}  //
