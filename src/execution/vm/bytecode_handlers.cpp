#include "execution/vm/bytecode_handlers.h"

#include "catalog/catalog_defs.h"
#include "execution/exec/execution_context.h"
#include "execution/sql/index_iterator.h"
#include "execution/sql/storage_interface.h"
#include "execution/sql/vector_projection_iterator.h"
#include "self_driving/modeling/operating_unit_defs.h"

extern "C" {

// ---------------------------------------------------------
// Table Vector Iterator
// ---------------------------------------------------------

void OpTableVectorIteratorInit(noisepage::execution::sql::TableVectorIterator *iter,
                               noisepage::execution::exec::ExecutionContext *exec_ctx, uint32_t table_oid,
                               uint32_t *col_oids, uint32_t num_oids) {
  NOISEPAGE_ASSERT(iter != nullptr, "Null iterator to initialize");
  new (iter) noisepage::execution::sql::TableVectorIterator(exec_ctx, table_oid, col_oids, num_oids);
}

void OpTableVectorIteratorPerformInit(noisepage::execution::sql::TableVectorIterator *iter) {
  NOISEPAGE_ASSERT(iter != nullptr, "NULL iterator given to init");
  iter->Init();
}

void OpTableVectorIteratorFree(noisepage::execution::sql::TableVectorIterator *iter) {
  NOISEPAGE_ASSERT(iter != nullptr, "NULL iterator given to close");
  iter->~TableVectorIterator();
}

void OpVPIInit(noisepage::execution::sql::VectorProjectionIterator *vpi,
               noisepage::execution::sql::VectorProjection *vp) {
  new (vpi) noisepage::execution::sql::VectorProjectionIterator(vp);
}

void OpVPIInitWithList(noisepage::execution::sql::VectorProjectionIterator *vpi,
                       noisepage::execution::sql::VectorProjection *vp,
                       noisepage::execution::sql::TupleIdList *tid_list) {
  new (vpi) noisepage::execution::sql::VectorProjectionIterator(vp, tid_list);
}

void OpVPIFree(noisepage::execution::sql::VectorProjectionIterator *vpi) { vpi->~VectorProjectionIterator(); }

// ---------------------------------------------------------
// Filter Manager
// ---------------------------------------------------------

void OpFilterManagerInit(noisepage::execution::sql::FilterManager *filter_manager,
                         const noisepage::execution::exec::ExecutionSettings &exec_settings) {
  new (filter_manager) noisepage::execution::sql::FilterManager(exec_settings);
}

void OpFilterManagerStartNewClause(noisepage::execution::sql::FilterManager *filter_manager) {
  filter_manager->StartNewClause();
}

void OpFilterManagerInsertFilter(noisepage::execution::sql::FilterManager *filter_manager,
                                 noisepage::execution::sql::FilterManager::MatchFn clause) {
  filter_manager->InsertClauseTerm(clause);
}

void OpFilterManagerRunFilters(noisepage::execution::sql::FilterManager *filter_manager,
                               noisepage::execution::sql::VectorProjectionIterator *vpi,
                               noisepage::execution::exec::ExecutionContext *exec_ctx) {
  filter_manager->RunFilters(exec_ctx, vpi);
}

void OpFilterManagerFree(noisepage::execution::sql::FilterManager *filter_manager) { filter_manager->~FilterManager(); }

// ---------------------------------------------------------
// Join Hash Table
// ---------------------------------------------------------

void OpJoinHashTableInit(noisepage::execution::sql::JoinHashTable *join_hash_table,
                         noisepage::execution::exec::ExecutionContext *exec_ctx, uint32_t tuple_size) {
  new (join_hash_table)
      noisepage::execution::sql::JoinHashTable(exec_ctx->GetExecutionSettings(), exec_ctx, tuple_size);
}

void OpJoinHashTableBuild(noisepage::execution::sql::JoinHashTable *join_hash_table) { join_hash_table->Build(); }

void OpJoinHashTableBuildParallel(noisepage::execution::sql::JoinHashTable *join_hash_table,
                                  noisepage::execution::sql::ThreadStateContainer *thread_state_container,
                                  uint32_t jht_offset) {
  join_hash_table->MergeParallel(thread_state_container, jht_offset);
}

void OpJoinHashTableFree(noisepage::execution::sql::JoinHashTable *join_hash_table) {
  join_hash_table->~JoinHashTable();
}

void OpJoinHashTableIteratorInit(noisepage::execution::sql::JoinHashTableIterator *iter,
                                 noisepage::execution::sql::JoinHashTable *join_hash_table) {
  NOISEPAGE_ASSERT(join_hash_table != nullptr, "Null hash table");
  new (iter) noisepage::execution::sql::JoinHashTableIterator(*join_hash_table);
}

void OpJoinHashTableIteratorFree(noisepage::execution::sql::JoinHashTableIterator *iter) {
  iter->~JoinHashTableIterator();
}

// ---------------------------------------------------------
// Aggregation Hash Table
// ---------------------------------------------------------

void OpAggregationHashTableInit(noisepage::execution::sql::AggregationHashTable *const agg_hash_table,
                                noisepage::execution::exec::ExecutionContext *exec_ctx, const uint32_t payload_size) {
  new (agg_hash_table)
      noisepage::execution::sql::AggregationHashTable(exec_ctx->GetExecutionSettings(), exec_ctx, payload_size);
}

void OpAggregationHashTableGetTupleCount(uint32_t *result,
                                         noisepage::execution::sql::AggregationHashTable *const agg_hash_table) {
  *result = agg_hash_table->GetTupleCount();
}

void OpAggregationHashTableGetInsertCount(uint32_t *result,
                                          noisepage::execution::sql::AggregationHashTable *const agg_hash_table) {
  *result = agg_hash_table->GetInsertCount();
}

void OpAggregationHashTableFree(noisepage::execution::sql::AggregationHashTable *const agg_hash_table) {
  agg_hash_table->~AggregationHashTable();
}

void OpAggregationHashTableIteratorInit(noisepage::execution::sql::AHTIterator *iter,
                                        noisepage::execution::sql::AggregationHashTable *agg_hash_table) {
  NOISEPAGE_ASSERT(agg_hash_table != nullptr, "Null hash table");
  new (iter) noisepage::execution::sql::AHTIterator(*agg_hash_table);
}

void OpAggregationHashTableBuildAllHashTablePartitions(noisepage::execution::sql::AggregationHashTable *agg_hash_table,
                                                       void *query_state) {
  agg_hash_table->BuildAllPartitions(query_state);
}

void OpAggregationHashTableRepartition(noisepage::execution::sql::AggregationHashTable *agg_hash_table) {
  agg_hash_table->Repartition();
}

void OpAggregationHashTableMergePartitions(
    noisepage::execution::sql::AggregationHashTable *agg_hash_table,
    noisepage::execution::sql::AggregationHashTable *target_agg_hash_table, void *query_state,
    noisepage::execution::sql::AggregationHashTable::MergePartitionFn merge_partition_fn) {
  agg_hash_table->MergePartitions(target_agg_hash_table, query_state, merge_partition_fn);
}

void OpAggregationHashTableIteratorFree(noisepage::execution::sql::AHTIterator *iter) { iter->~AHTIterator(); }

// ---------------------------------------------------------
// Sorters
// ---------------------------------------------------------

void OpSorterInit(noisepage::execution::sql::Sorter *const sorter,
                  noisepage::execution::exec::ExecutionContext *const exec_ctx,
                  const noisepage::execution::sql::Sorter::ComparisonFunction cmp_fn, const uint32_t tuple_size) {
  new (sorter) noisepage::execution::sql::Sorter(exec_ctx, cmp_fn, tuple_size);
}

void OpSorterSort(noisepage::execution::sql::Sorter *sorter) { sorter->Sort(); }

void OpSorterSortParallel(noisepage::execution::sql::Sorter *sorter,
                          noisepage::execution::sql::ThreadStateContainer *thread_state_container,
                          uint32_t sorter_offset) {
  sorter->SortParallel(thread_state_container, sorter_offset);
}

void OpSorterSortTopKParallel(noisepage::execution::sql::Sorter *sorter,
                              noisepage::execution::sql::ThreadStateContainer *thread_state_container,
                              uint32_t sorter_offset, uint64_t top_k) {
  sorter->SortTopKParallel(thread_state_container, sorter_offset, top_k);
}

void OpSorterFree(noisepage::execution::sql::Sorter *sorter) { sorter->~Sorter(); }

void OpSorterIteratorInit(noisepage::execution::sql::SorterIterator *iter, noisepage::execution::sql::Sorter *sorter) {
  new (iter) noisepage::execution::sql::SorterIterator(*sorter);
}

void OpSorterIteratorFree(noisepage::execution::sql::SorterIterator *iter) { iter->~SorterIterator(); }

// ---------------------------------------------------------
// CSV Reader
// ---------------------------------------------------------
#if 0
void OpCSVReaderInit(noisepage::execution::util::CSVReader *reader, const uint8_t *file_name, uint32_t len) {
  std::string_view fname(reinterpret_cast<const char *>(file_name), len);
  new (reader) noisepage::execution::util::CSVReader(std::make_unique<noisepage::execution::util::CSVFile>(fname));
}

void OpCSVReaderPerformInit(bool *result, noisepage::execution::util::CSVReader *reader) {
  *result = reader->Initialize();
}

void OpCSVReaderClose(noisepage::execution::util::CSVReader *reader) { std::destroy_at(reader); }
#endif
// -------------------------------------------------------------
// StorageInterface Calls
// -------------------------------------------------------------

void OpStorageInterfaceInit(noisepage::execution::sql::StorageInterface *storage_interface,
                            noisepage::execution::exec::ExecutionContext *exec_ctx, uint32_t table_oid,
                            uint32_t *col_oids, uint32_t num_oids, bool need_indexes) {
  new (storage_interface) noisepage::execution::sql::StorageInterface(
      exec_ctx, noisepage::catalog::table_oid_t(table_oid), col_oids, num_oids, need_indexes);
}

void OpStorageInterfaceGetTablePR(noisepage::storage::ProjectedRow **pr_result,
                                  noisepage::execution::sql::StorageInterface *storage_interface) {
  *pr_result = storage_interface->GetTablePR();
}

void OpStorageInterfaceTableUpdate(bool *result, noisepage::execution::sql::StorageInterface *storage_interface,
                                   noisepage::storage::TupleSlot *tuple_slot) {
  *result = storage_interface->TableUpdate(*tuple_slot);
}

void OpStorageInterfaceTableDelete(bool *result, noisepage::execution::sql::StorageInterface *storage_interface,
                                   noisepage::storage::TupleSlot *tuple_slot) {
  *result = storage_interface->TableDelete(*tuple_slot);
}

void OpStorageInterfaceTableInsert(noisepage::storage::TupleSlot *tuple_slot,
                                   noisepage::execution::sql::StorageInterface *storage_interface) {
  *tuple_slot = storage_interface->TableInsert();
}

void OpStorageInterfaceGetIndexPR(noisepage::storage::ProjectedRow **pr_result,
                                  noisepage::execution::sql::StorageInterface *storage_interface, uint32_t index_oid) {
  *pr_result = storage_interface->GetIndexPR(noisepage::catalog::index_oid_t(index_oid));
}

void OpStorageInterfaceGetIndexHeapSize(uint32_t *size,
                                        noisepage::execution::sql::StorageInterface *storage_interface) {
  *size = storage_interface->GetIndexHeapSize();
}

// TODO(WAN): this should be uint64_t, but see #1049
void OpStorageInterfaceIndexGetSize(uint32_t *result, noisepage::execution::sql::StorageInterface *storage_interface) {
  *result = storage_interface->IndexGetSize();
}

void OpStorageInterfaceIndexInsert(bool *result, noisepage::execution::sql::StorageInterface *storage_interface) {
  *result = storage_interface->IndexInsert();
}

void OpStorageInterfaceIndexInsertUnique(bool *result, noisepage::execution::sql::StorageInterface *storage_interface) {
  *result = storage_interface->IndexInsertUnique();
}
void OpStorageInterfaceIndexInsertWithSlot(bool *result, noisepage::execution::sql::StorageInterface *storage_interface,
                                           noisepage::storage::TupleSlot *tuple_slot, bool unique) {
  *result = storage_interface->IndexInsertWithTuple(*tuple_slot, unique);
}
void OpStorageInterfaceIndexDelete(noisepage::execution::sql::StorageInterface *storage_interface,
                                   noisepage::storage::TupleSlot *tuple_slot) {
  storage_interface->IndexDelete(*tuple_slot);
}

void OpStorageInterfaceFree(noisepage::execution::sql::StorageInterface *storage_interface) {
  storage_interface->~StorageInterface();
}

// -------------------------------------------------------------------
// Index Iterator
// -------------------------------------------------------------------
void OpIndexIteratorInit(noisepage::execution::sql::IndexIterator *iter,
                         noisepage::execution::exec::ExecutionContext *exec_ctx, uint32_t num_attrs, uint32_t table_oid,
                         uint32_t index_oid, uint32_t *col_oids, uint32_t num_oids) {
  new (iter) noisepage::execution::sql::IndexIterator(exec_ctx, num_attrs, table_oid, index_oid, col_oids, num_oids);
}

// TODO(WAN): this should be uint64_t, but see #1049
void OpIndexIteratorGetSize(uint32_t *index_size, noisepage::execution::sql::IndexIterator *iter) {
  *index_size = iter->GetIndexSize();
}

void OpIndexIteratorPerformInit(noisepage::execution::sql::IndexIterator *iter) { iter->Init(); }

void OpIndexIteratorFree(noisepage::execution::sql::IndexIterator *iter) { iter->~IndexIterator(); }

void OpExecutionContextRegisterHook(noisepage::execution::exec::ExecutionContext *exec_ctx, uint32_t hook_idx,
                                    noisepage::execution::exec::ExecutionContext::HookFn hook) {
  exec_ctx->RegisterHook(hook_idx, hook);
}

void OpExecutionContextClearHooks(noisepage::execution::exec::ExecutionContext *exec_ctx) { exec_ctx->ClearHooks(); }

void OpExecutionContextInitHooks(noisepage::execution::exec::ExecutionContext *exec_ctx, uint32_t num_hooks) {
  exec_ctx->InitHooks(num_hooks);
}

void OpExecutionContextStartPipelineTracker(noisepage::execution::exec::ExecutionContext *const exec_ctx,
                                            noisepage::execution::pipeline_id_t pipeline_id) {
  exec_ctx->StartPipelineTracker(pipeline_id);
}

void OpExecutionContextEndPipelineTracker(noisepage::execution::exec::ExecutionContext *const exec_ctx,
                                          noisepage::execution::query_id_t query_id,
                                          noisepage::execution::pipeline_id_t pipeline_id,
                                          noisepage::selfdriving::ExecOUFeatureVector *const ouvec) {
  exec_ctx->EndPipelineTracker(query_id, pipeline_id, ouvec);
}

void OpExecOUFeatureVectorRecordFeature(
    noisepage::selfdriving::ExecOUFeatureVector *ouvec, noisepage::execution::pipeline_id_t pipeline_id,
    noisepage::execution::feature_id_t feature_id,
    noisepage::selfdriving::ExecutionOperatingUnitFeatureAttribute feature_attribute,
    noisepage::selfdriving::ExecutionOperatingUnitFeatureUpdateMode mode, uint32_t value) {
  ouvec->UpdateFeature(pipeline_id, feature_id, feature_attribute, mode, value);
}

void OpExecOUFeatureVectorInitialize(noisepage::execution::exec::ExecutionContext *const exec_ctx,
                                     noisepage::selfdriving::ExecOUFeatureVector *const ouvec,
                                     noisepage::execution::pipeline_id_t pipeline_id, bool is_parallel) {
  if (is_parallel)
    exec_ctx->InitializeParallelOUFeatureVector(ouvec, pipeline_id);
  else
    exec_ctx->InitializeOUFeatureVector(ouvec, pipeline_id);
}

void OpExecOUFeatureVectorReset(noisepage::selfdriving::ExecOUFeatureVector *const ouvec) { ouvec->Reset(); }

void OpExecutionContextSetMemoryUseOverride(noisepage::execution::exec::ExecutionContext *const exec_ctx,
                                            uint32_t memory_use) {
  exec_ctx->SetMemoryUseOverride(memory_use);
}

void OpExecOUFeatureVectorFilter(noisepage::selfdriving::ExecOUFeatureVector *const ouvec,
                                 noisepage::selfdriving::ExecutionOperatingUnitType filter) {
  ouvec->pipeline_features_->erase(
      std::remove_if(ouvec->pipeline_features_->begin(), ouvec->pipeline_features_->end(),
                     [filter](const auto &feature) {
                       return (filter != noisepage::selfdriving::ExecutionOperatingUnitType::INVALID) &&
                              (filter != feature.GetExecutionOperatingUnitType());
                     }),
      ouvec->pipeline_features_->end());
}

void OpRegisterThreadWithMetricsManager(noisepage::execution::exec::ExecutionContext *exec_ctx) {
  exec_ctx->RegisterThreadWithMetricsManager();
}

void OpEnsureTrackersStopped(noisepage::execution::exec::ExecutionContext *exec_ctx) {
  exec_ctx->EnsureTrackersStopped();
}

void OpAggregateMetricsThread(noisepage::execution::exec::ExecutionContext *exec_ctx) {
  exec_ctx->AggregateMetricsThread();
}

}  //
