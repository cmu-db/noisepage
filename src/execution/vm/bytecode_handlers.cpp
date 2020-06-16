#include "execution/vm/bytecode_handlers.h"

#include "catalog/catalog_defs.h"
#include "execution/exec/execution_context.h"
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
  TERRIER_ASSERT(iter != nullptr, "NULL iterator given to close");
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
                         terrier::execution::exec::ExecutionSettings *exec_settings) {
  new (filter_manager) terrier::execution::sql::FilterManager(terrier::common::ManagedPointer(exec_settings));
}

void OpFilterManagerStartNewClause(terrier::execution::sql::FilterManager *filter_manager) {
  filter_manager->StartNewClause();
}

void OpFilterManagerInsertFilter(terrier::execution::sql::FilterManager *filter_manager,
                                 terrier::execution::sql::FilterManager::MatchFn clause) {
  filter_manager->InsertClauseTerm(clause);
}

void OpFilterManagerRunFilters(terrier::execution::sql::FilterManager *filter_manager,
                               terrier::execution::sql::VectorProjectionIterator *vpi) {
  filter_manager->RunFilters(vpi);
}

void OpFilterManagerFree(terrier::execution::sql::FilterManager *filter_manager) { filter_manager->~FilterManager(); }

// ---------------------------------------------------------
// Join Hash Table
// ---------------------------------------------------------

void OpJoinHashTableInit(terrier::execution::sql::JoinHashTable *join_hash_table,
                         terrier::execution::exec::ExecutionSettings *exec_settings,
                         terrier::execution::sql::MemoryPool *memory, uint32_t tuple_size) {
  new (join_hash_table)
      terrier::execution::sql::JoinHashTable(terrier::common::ManagedPointer(exec_settings), memory, tuple_size);
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
                                terrier::execution::exec::ExecutionSettings *exec_settings,
                                terrier::execution::sql::MemoryPool *const memory, const uint32_t payload_size) {
  new (agg_hash_table) terrier::execution::sql::AggregationHashTable(terrier::common::ManagedPointer(exec_settings),
                                                                     memory, payload_size);
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

void OpCSVReaderInit(terrier::execution::util::CSVReader *reader, const uint8_t *file_name, uint32_t len) {
  std::string_view fname(reinterpret_cast<const char *>(file_name), len);
  new (reader) terrier::execution::util::CSVReader(std::make_unique<terrier::execution::util::CSVFile>(fname));
}

void OpCSVReaderPerformInit(bool *result, terrier::execution::util::CSVReader *reader) {
  *result = reader->Initialize();
}

void OpCSVReaderClose(terrier::execution::util::CSVReader *reader) { std::destroy_at(reader); }

}  //
