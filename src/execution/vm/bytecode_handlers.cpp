#include "execution/vm/bytecode_handlers.h"

#include "catalog/catalog_defs.h"
#include "execution/exec/execution_context.h"
#include "execution/sql/execution_structures.h"

extern "C" {

// ---------------------------------------------------------
// Region
// ---------------------------------------------------------

void OpRegionInit(tpl::util::Region *region) {
  new (region) tpl::util::Region("tmp");
}

void OpRegionFree(tpl::util::Region *region) { region->~Region(); }

// ---------------------------------------------------------
// Transactions
// ---------------------------------------------------------
void OpBeginTransaction(terrier::transaction::TransactionContext **txn) {
  auto *exec = tpl::sql::ExecutionStructures::Instance();
  *txn = exec->GetTxnManager()->BeginTransaction();
}

void OpCommitTransaction(terrier::transaction::TransactionContext **txn) {
  auto *exec = tpl::sql::ExecutionStructures::Instance();
  exec->GetTxnManager()->Commit(*txn, [](void *) { return; }, nullptr);
}

void OpAbortTransaction(terrier::transaction::TransactionContext **txn) {
  auto *exec = tpl::sql::ExecutionStructures::Instance();
  exec->GetTxnManager()->Abort(*txn);
}

// ---------------------------------------------------------
// Table Vector Iterator
// ---------------------------------------------------------

void OpTableVectorIteratorInit(tpl::sql::TableVectorIterator *iter, u32 db_oid,
                               u32 table_oid, uintptr_t exec_context_addr) {
  TPL_ASSERT(iter != nullptr, "Null iterator to initialize");
  auto *exec_context =
      reinterpret_cast<tpl::exec::ExecutionContext *>(exec_context_addr);
  new (iter)
      tpl::sql::TableVectorIterator(db_oid, table_oid, exec_context->GetTxn());
}

void OpTableVectorIteratorPerformInit(tpl::sql::TableVectorIterator *iter) {
  iter->Init();
}

void OpTableVectorIteratorFree(tpl::sql::TableVectorIterator *iter) {
  TPL_ASSERT(iter != nullptr, "NULL iterator given to close");
  iter->~TableVectorIterator();
}

void OpPCIFilterEqual(u32 *size, tpl::sql::ProjectedColumnsIterator *iter,
                      u32 col_id, i8 type, i64 val) {
  auto sql_type = static_cast<terrier::type::TypeId>(type);
  auto v = iter->MakeFilterVal(val, sql_type);
  *size = iter->FilterColByVal<std::equal_to>(col_id, sql_type, v);
}

void OpPCIFilterGreaterThan(u32 *size, tpl::sql::ProjectedColumnsIterator *iter,
                            u32 col_id, i8 type, i64 val) {
  auto sql_type = static_cast<terrier::type::TypeId>(type);
  auto v = iter->MakeFilterVal(val, sql_type);
  *size = iter->FilterColByVal<std::greater>(col_id, sql_type, v);
}

void OpPCIFilterGreaterThanEqual(u32 *size,
                                 tpl::sql::ProjectedColumnsIterator *iter,
                                 u32 col_id, i8 type, i64 val) {
  auto sql_type = static_cast<terrier::type::TypeId>(type);
  auto v = iter->MakeFilterVal(val, sql_type);
  *size = iter->FilterColByVal<std::greater_equal>(col_id, sql_type, v);
}

void OpPCIFilterLessThan(u32 *size, tpl::sql::ProjectedColumnsIterator *iter,
                         u32 col_id, i8 type, i64 val) {
  auto sql_type = static_cast<terrier::type::TypeId>(type);
  auto v = iter->MakeFilterVal(val, sql_type);
  *size = iter->FilterColByVal<std::less>(col_id, sql_type, v);
}

void OpPCIFilterLessThanEqual(u32 *size,
                              tpl::sql::ProjectedColumnsIterator *iter,
                              u32 col_id, i8 type, i64 val) {
  auto sql_type = static_cast<terrier::type::TypeId>(type);
  auto v = iter->MakeFilterVal(val, sql_type);
  *size = iter->FilterColByVal<std::less_equal>(col_id, sql_type, v);
}

void OpPCIFilterNotEqual(u32 *size, tpl::sql::ProjectedColumnsIterator *iter,
                         u32 col_id, i8 type, i64 val) {
  auto sql_type = static_cast<terrier::type::TypeId>(type);
  auto v = iter->MakeFilterVal(val, sql_type);
  *size = iter->FilterColByVal<std::not_equal_to>(col_id, sql_type, v);
}

// ---------------------------------------------------------
// Join Hash Table
// ---------------------------------------------------------

void OpJoinHashTableInit(tpl::sql::JoinHashTable *join_hash_table,
                         tpl::util::Region *region, u32 tuple_size) {
  new (join_hash_table) tpl::sql::JoinHashTable(region, tuple_size);
}

void OpJoinHashTableBuild(tpl::sql::JoinHashTable *join_hash_table) {
  join_hash_table->Build();
}

void OpJoinHashTableFree(tpl::sql::JoinHashTable *join_hash_table) {
  join_hash_table->~JoinHashTable();
}

// ---------------------------------------------------------
// Aggregation Hash Table
// ---------------------------------------------------------

void OpAggregationHashTableInit(tpl::sql::AggregationHashTable *agg_table,
                                tpl::util::Region *region, u32 entry_size) {
  new (agg_table) tpl::sql::AggregationHashTable(region, entry_size);
}

void OpAggregationHashTableFree(tpl::sql::AggregationHashTable *agg_table) {
  agg_table->~AggregationHashTable();
}

// ---------------------------------------------------------
// Sorters
// ---------------------------------------------------------

void OpSorterInit(tpl::sql::Sorter *sorter, tpl::util::Region *region,
                  tpl::sql::Sorter::ComparisonFunction cmp_fn, u32 tuple_size) {
  new (sorter) tpl::sql::Sorter(region, cmp_fn, tuple_size);
}

void OpSorterSort(tpl::sql::Sorter *sorter) { sorter->Sort(); }

void OpSorterFree(tpl::sql::Sorter *sorter) { sorter->~Sorter(); }

void OpSorterIteratorInit(tpl::sql::SorterIterator *iter,
                          tpl::sql::Sorter *sorter) {
  new (iter) tpl::sql::SorterIterator(sorter);
}

void OpSorterIteratorFree(tpl::sql::SorterIterator *iter) {
  iter->~SorterIterator();
}

// -------------------------------------------------------------
// Output
// ------------------------------------------------------------
void OpOutputAlloc(uintptr_t context_ptr, byte **result) {
  auto exec_context =
      reinterpret_cast<tpl::exec::ExecutionContext *>(context_ptr);
  *result = exec_context->GetOutputBuffer()->AllocOutputSlot();
}

void OpOutputAdvance(uintptr_t context_ptr) {
  auto exec_context =
      reinterpret_cast<tpl::exec::ExecutionContext *>(context_ptr);
  exec_context->GetOutputBuffer()->Advance();
}

void OpOutputSetNull(uintptr_t context_ptr, u32 idx) {
  auto exec_context =
      reinterpret_cast<tpl::exec::ExecutionContext *>(context_ptr);
  exec_context->GetOutputBuffer()->SetNull(static_cast<u16>(idx), true);
}

void OpOutputFinalize(uintptr_t context_ptr) {
  auto exec_context =
      reinterpret_cast<tpl::exec::ExecutionContext *>(context_ptr);
  exec_context->GetOutputBuffer()->Finalize();
}

// -------------------------------------------------------------
// Insert
// ------------------------------------------------------------
void OpInsert(uintptr_t context_ptr, u32 db_oid, u32 table_oid, byte *values_ptr) {
  auto exec_context =
      reinterpret_cast<tpl::exec::ExecutionContext *>(context_ptr);

  // find the table we want to insert to
  auto catalog = tpl::sql::ExecutionStructures::Instance()->GetCatalog();
  auto table = catalog->GetCatalogTable(static_cast<terrier::catalog::db_oid_t>(db_oid),
      static_cast<terrier::catalog::table_oid_t>(table_oid));
  auto sql_table = table->GetSqlTable();
  auto *const txn = exec_context->GetTxn();

  // create insertion buffer
  auto *pri = table->GetPRI();
  auto *insert_buffer =
      terrier::common::AllocationUtil::AllocateAligned(pri->ProjectedRowSize());
  auto *insert = pri->InitializeRow(insert_buffer);

  // copy data into insertion buffer
  u16 index = 0;
  u16 offset = 0;

  auto schema_cols = sql_table->GetSchema().GetColumns();
  for (const auto &col : schema_cols) {
    //TODO(tanujnay112): figure out nulls
    uint8_t current_size = col.GetAttrSize();
    byte *data = insert->AccessForceNotNull(index);
    std::memcpy(data, values_ptr + offset, current_size);
    index = static_cast<u16>(index + 1);
    offset = static_cast<u16>(offset + current_size);
  }

  sql_table->Insert(txn, *insert);
}

// -------------------------------------------------------------------
// Index Iterator
// -------------------------------------------------------------------
void OpIndexIteratorInit(tpl::sql::IndexIterator *iter, uint32_t index_oid,
                         uintptr_t context_ptr) {
  auto exec_context =
      reinterpret_cast<tpl::exec::ExecutionContext *>(context_ptr);
  new (iter) tpl::sql::IndexIterator(index_oid, exec_context->GetTxn());
}
void OpIndexIteratorScanKey(tpl::sql::IndexIterator *iter, byte *key) {
  iter->ScanKey(key);
}
void OpIndexIteratorFree(tpl::sql::IndexIterator *iter) {
  iter->~IndexIterator();
}

}  //
