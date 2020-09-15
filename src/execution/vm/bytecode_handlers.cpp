#include "execution/vm/bytecode_handlers.h"

#include "catalog/catalog_defs.h"
#include "execution/exec/execution_context.h"
#include "execution/sql/index_iterator.h"
#include "execution/sql/storage_interface.h"
#include "execution/sql/vector_projection_iterator.h"

extern "C" {

// ---------------------------------------------------------
// Table Vector Iterator
// ---------------------------------------------------------

void OpTableVectorIteratorInit(terrier::execution::sql::TableVectorIterator *iter,
                               terrier::execution::exec::ExecutionContext *exec_ctx, uint32_t table_oid,
                               uint32_t *col_oids, uint32_t num_oids) {
  TERRIER_ASSERT(iter != nullptr, "Null iterator to initialize");
  new (iter) terrier::execution::sql::TableVectorIterator(exec_ctx, table_oid, col_oids, num_oids);
}

void OpTableVectorIteratorPerformInit(terrier::execution::sql::TableVectorIterator *iter) {
  TERRIER_ASSERT(iter != nullptr, "NULL iterator given to init");
  iter->Init();
}

void OpTableVectorIteratorFree(terrier::execution::sql::TableVectorIterator *iter) {
  TERRIER_ASSERT(iter != nullptr, "NULL iterator given to close");
  iter->~TableVectorIterator();
}

void OpVPIInit(terrier::execution::sql::VectorProjectionIterator *vpi, terrier::execution::sql::VectorProjection *vp) {
  new (vpi) terrier::execution::sql::VectorProjectionIterator(vp);
}

void OpVPIInitWithList(terrier::execution::sql::VectorProjectionIterator *vpi,
                       terrier::execution::sql::VectorProjection *vp, terrier::execution::sql::TupleIdList *tid_list) {
  new (vpi) terrier::execution::sql::VectorProjectionIterator(vp, tid_list);
}

void OpVPIFree(terrier::execution::sql::VectorProjectionIterator *vpi) { vpi->~VectorProjectionIterator(); }

// ---------------------------------------------------------
// Filter Manager
// ---------------------------------------------------------

void OpFilterManagerInit(terrier::execution::sql::FilterManager *filter_manager,
                         const terrier::execution::exec::ExecutionSettings &exec_settings) {
  new (filter_manager) terrier::execution::sql::FilterManager(exec_settings);
}

void OpFilterManagerStartNewClause(terrier::execution::sql::FilterManager *filter_manager) {
  filter_manager->StartNewClause();
}

void OpFilterManagerInsertFilter(terrier::execution::sql::FilterManager *filter_manager,
                                 terrier::execution::sql::FilterManager::MatchFn clause) {
  filter_manager->InsertClauseTerm(clause);
}

void OpFilterManagerRunFilters(terrier::execution::sql::FilterManager *filter_manager,
                               terrier::execution::sql::VectorProjectionIterator *vpi,
                               terrier::execution::exec::ExecutionContext *exec_ctx) {
  filter_manager->RunFilters(exec_ctx, vpi);
}

void OpFilterManagerFree(terrier::execution::sql::FilterManager *filter_manager) { filter_manager->~FilterManager(); }

// ---------------------------------------------------------
// Join Hash Table
// ---------------------------------------------------------

void OpJoinHashTableInit(terrier::execution::sql::JoinHashTable *join_hash_table,
                         terrier::execution::exec::ExecutionContext *exec_ctx,
                         terrier::execution::sql::MemoryPool *memory, uint32_t tuple_size) {
  new (join_hash_table) terrier::execution::sql::JoinHashTable(exec_ctx->GetExecutionSettings(), memory, tuple_size);
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
                                terrier::execution::exec::ExecutionContext *exec_ctx,
                                terrier::execution::sql::MemoryPool *const memory, const uint32_t payload_size) {
  new (agg_hash_table)
      terrier::execution::sql::AggregationHashTable(exec_ctx->GetExecutionSettings(), memory, payload_size);
}

void OpAggregationHashTableGetTupleCount(uint32_t *result,
                                         terrier::execution::sql::AggregationHashTable *const agg_hash_table) {
  *result = agg_hash_table->GetTupleCount();
}

void OpAggregationHashTableFree(terrier::execution::sql::AggregationHashTable *const agg_hash_table) {
  agg_hash_table->~AggregationHashTable();
}

void OpAggregationHashTableIteratorInit(terrier::execution::sql::AHTIterator *iter,
                                        terrier::execution::sql::AggregationHashTable *agg_hash_table) {
  TERRIER_ASSERT(agg_hash_table != nullptr, "Null hash table");
  new (iter) terrier::execution::sql::AHTIterator(*agg_hash_table);
}

void OpAggregationHashTableBuildAllHashTablePartitions(terrier::execution::sql::AggregationHashTable *agg_hash_table,
                                                       void *query_state) {
  agg_hash_table->BuildAllPartitions(query_state);
}

void OpAggregationHashTableRepartition(terrier::execution::sql::AggregationHashTable *agg_hash_table) {
  agg_hash_table->Repartition();
}

void OpAggregationHashTableMergePartitions(
    terrier::execution::sql::AggregationHashTable *agg_hash_table,
    terrier::execution::sql::AggregationHashTable *target_agg_hash_table, void *query_state,
    terrier::execution::sql::AggregationHashTable::MergePartitionFn merge_partition_fn) {
  agg_hash_table->MergePartitions(target_agg_hash_table, query_state, merge_partition_fn);
}

void OpAggregationHashTableIteratorFree(terrier::execution::sql::AHTIterator *iter) { iter->~AHTIterator(); }

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
  new (iter) terrier::execution::sql::SorterIterator(*sorter);
}

void OpSorterIteratorFree(terrier::execution::sql::SorterIterator *iter) { iter->~SorterIterator(); }

// ---------------------------------------------------------
// CSV Reader
// ---------------------------------------------------------
#if 0
void OpCSVReaderInit(terrier::execution::util::CSVReader *reader, const uint8_t *file_name, uint32_t len) {
  std::string_view fname(reinterpret_cast<const char *>(file_name), len);
  new (reader) terrier::execution::util::CSVReader(std::make_unique<terrier::execution::util::CSVFile>(fname));
}

void OpCSVReaderPerformInit(bool *result, terrier::execution::util::CSVReader *reader) {
  *result = reader->Initialize();
}

void OpCSVReaderClose(terrier::execution::util::CSVReader *reader) { std::destroy_at(reader); }
#endif
// -------------------------------------------------------------
// StorageInterface Calls
// -------------------------------------------------------------

void OpStorageInterfaceInit(terrier::execution::sql::StorageInterface *storage_interface,
                            terrier::execution::exec::ExecutionContext *exec_ctx, uint32_t table_oid,
                            uint32_t *col_oids, uint32_t num_oids, bool need_indexes) {
  new (storage_interface) terrier::execution::sql::StorageInterface(exec_ctx, terrier::catalog::table_oid_t(table_oid),
                                                                    col_oids, num_oids, need_indexes);
}

void OpStorageInterfaceGetTablePR(terrier::storage::ProjectedRow **pr_result,
                                  terrier::execution::sql::StorageInterface *storage_interface) {
  *pr_result = storage_interface->GetTablePR();
}

void OpStorageInterfaceTableUpdate(bool *result, terrier::execution::sql::StorageInterface *storage_interface,
                                   terrier::storage::TupleSlot *tuple_slot) {
  *result = storage_interface->TableUpdate(*tuple_slot);
}

void OpStorageInterfaceTableDelete(bool *result, terrier::execution::sql::StorageInterface *storage_interface,
                                   terrier::storage::TupleSlot *tuple_slot) {
  *result = storage_interface->TableDelete(*tuple_slot);
}

void OpStorageInterfaceTableInsert(terrier::storage::TupleSlot *tuple_slot,
                                   terrier::execution::sql::StorageInterface *storage_interface) {
  *tuple_slot = storage_interface->TableInsert();
}

void OpStorageInterfaceGetIndexPR(terrier::storage::ProjectedRow **pr_result,
                                  terrier::execution::sql::StorageInterface *storage_interface, uint32_t index_oid) {
  *pr_result = storage_interface->GetIndexPR(terrier::catalog::index_oid_t(index_oid));
}

void OpStorageInterfaceGetIndexHeapSize(uint32_t *size, terrier::execution::sql::StorageInterface *storage_interface) {
  *size = storage_interface->GetIndexHeapSize();
}

// TODO(WAN): this should be uint64_t, but see #1049
void OpStorageInterfaceIndexGetSize(uint32_t *result, terrier::execution::sql::StorageInterface *storage_interface) {
  *result = storage_interface->IndexGetSize();
}

void OpStorageInterfaceIndexInsert(bool *result, terrier::execution::sql::StorageInterface *storage_interface) {
  *result = storage_interface->IndexInsert();
}

void OpStorageInterfaceIndexInsertUnique(bool *result, terrier::execution::sql::StorageInterface *storage_interface) {
  *result = storage_interface->IndexInsertUnique();
}
void OpStorageInterfaceIndexInsertWithSlot(bool *result, terrier::execution::sql::StorageInterface *storage_interface,
                                           terrier::storage::TupleSlot *tuple_slot, bool unique) {
  *result = storage_interface->IndexInsertWithTuple(*tuple_slot, unique);
}
void OpStorageInterfaceIndexDelete(terrier::execution::sql::StorageInterface *storage_interface,
                                   terrier::storage::TupleSlot *tuple_slot) {
  storage_interface->IndexDelete(*tuple_slot);
}

void OpStorageInterfaceFree(terrier::execution::sql::StorageInterface *storage_interface) {
  storage_interface->~StorageInterface();
}

// -------------------------------------------------------------------
// Index Iterator
// -------------------------------------------------------------------
void OpIndexIteratorInit(terrier::execution::sql::IndexIterator *iter,
                         terrier::execution::exec::ExecutionContext *exec_ctx, uint32_t num_attrs, uint32_t table_oid,
                         uint32_t index_oid, uint32_t *col_oids, uint32_t num_oids) {
  new (iter) terrier::execution::sql::IndexIterator(exec_ctx, num_attrs, table_oid, index_oid, col_oids, num_oids);
}

// TODO(WAN): this should be uint64_t, but see #1049
void OpIndexIteratorGetSize(uint32_t *index_size, terrier::execution::sql::IndexIterator *iter) {
  *index_size = iter->GetIndexSize();
}

void OpIndexIteratorPerformInit(terrier::execution::sql::IndexIterator *iter) { iter->Init(); }

void OpIndexIteratorFree(terrier::execution::sql::IndexIterator *iter) { iter->~IndexIterator(); }

}  //
