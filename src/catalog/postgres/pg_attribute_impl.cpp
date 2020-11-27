#include "catalog/postgres/pg_attribute_impl.h"

#include "catalog/database_catalog.h"
#include "catalog/postgres/builder.h"
#include "catalog/postgres/pg_attribute.h"
#include "catalog/postgres/pg_namespace.h"
#include "common/json.h"
#include "storage/index/index.h"
#include "storage/sql_table.h"

namespace noisepage::catalog::postgres {

PgAttributeImpl::PgAttributeImpl(db_oid_t db_oid) : db_oid_(db_oid) {}

void PgAttributeImpl::BootstrapPRIs() {
  const std::vector<col_oid_t> pg_attribute_all_oids{PgAttribute::PG_ATTRIBUTE_ALL_COL_OIDS.cbegin(),
                                                     PgAttribute::PG_ATTRIBUTE_ALL_COL_OIDS.end()};
  pg_attribute_all_cols_pri_ = columns_->InitializerForProjectedRow(pg_attribute_all_oids);
  pg_attribute_all_cols_prm_ = columns_->ProjectionMapForOids(pg_attribute_all_oids);

  const std::vector<col_oid_t> get_columns_oids{PgAttribute::ATTNUM_COL_OID,     PgAttribute::ATTNAME_COL_OID,
                                                PgAttribute::ATTTYPID_COL_OID,   PgAttribute::ATTLEN_COL_OID,
                                                PgAttribute::ATTNOTNULL_COL_OID, PgAttribute::ADSRC_COL_OID};
  get_columns_pri_ = columns_->InitializerForProjectedRow(get_columns_oids);
  get_columns_prm_ = columns_->ProjectionMapForOids(get_columns_oids);

  const std::vector<col_oid_t> delete_columns_oids{PgAttribute::ATTNUM_COL_OID, PgAttribute::ATTNAME_COL_OID};
  delete_columns_pri_ = columns_->InitializerForProjectedRow(delete_columns_oids);
  delete_columns_prm_ = columns_->ProjectionMapForOids(delete_columns_oids);
}

void PgAttributeImpl::Bootstrap(const common::ManagedPointer<transaction::TransactionContext> txn,
                                const common::ManagedPointer<DatabaseCatalog> dbc) {
  UNUSED_ATTRIBUTE bool retval;

  retval = dbc->CreateTableEntry(txn, PgAttribute::COLUMN_TABLE_OID, postgres::NAMESPACE_CATALOG_NAMESPACE_OID,
                                 "pg_attribute", Builder::GetColumnTableSchema());
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = dbc->SetTablePointer(txn, PgAttribute::COLUMN_TABLE_OID, columns_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  retval = dbc->CreateIndexEntry(txn, postgres::NAMESPACE_CATALOG_NAMESPACE_OID, PgAttribute::COLUMN_TABLE_OID,
                                 PgAttribute::COLUMN_OID_INDEX_OID, "pg_attribute_oid_index",
                                 Builder::GetColumnOidIndexSchema(db_oid_));
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = dbc->SetIndexPointer(txn, PgAttribute::COLUMN_OID_INDEX_OID, columns_oid_index_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  retval = dbc->CreateIndexEntry(txn, postgres::NAMESPACE_CATALOG_NAMESPACE_OID, PgAttribute::COLUMN_TABLE_OID,
                                 PgAttribute::COLUMN_NAME_INDEX_OID, "pg_attribute_name_index",
                                 Builder::GetColumnNameIndexSchema(db_oid_));
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = dbc->SetIndexPointer(txn, PgAttribute::COLUMN_NAME_INDEX_OID, columns_name_index_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
}

template <typename Column, typename ClassOid, typename ColOid>
bool PgAttributeImpl::CreateColumn(const common::ManagedPointer<transaction::TransactionContext> txn,
                                   const ClassOid class_oid, const ColOid col_oid, const Column &col) {
  // Step 1: Insert into the table
  auto *const redo = txn->StageWrite(db_oid_, postgres::PgAttribute::COLUMN_TABLE_OID, pg_attribute_all_cols_pri_);
  // Write the attributes in the Redo Record
  auto oid_entry = reinterpret_cast<ColOid *>(
      redo->Delta()->AccessForceNotNull(pg_attribute_all_cols_prm_[postgres::PgAttribute::ATTNUM_COL_OID]));
  auto relid_entry = reinterpret_cast<ClassOid *>(
      redo->Delta()->AccessForceNotNull(pg_attribute_all_cols_prm_[postgres::PgAttribute::ATTRELID_COL_OID]));
  auto name_entry = reinterpret_cast<storage::VarlenEntry *>(
      redo->Delta()->AccessForceNotNull(pg_attribute_all_cols_prm_[postgres::PgAttribute::ATTNAME_COL_OID]));
  auto type_entry = reinterpret_cast<uint32_t *>(redo->Delta()->AccessForceNotNull(
      pg_attribute_all_cols_prm_[postgres::PgAttribute::ATTTYPID_COL_OID]));  // TypeId is a uint8_t enum class, but in
  // the schema it's INTEGER (32-bit)
  auto len_entry = reinterpret_cast<uint16_t *>(
      redo->Delta()->AccessForceNotNull(pg_attribute_all_cols_prm_[postgres::PgAttribute::ATTLEN_COL_OID]));
  auto notnull_entry = reinterpret_cast<bool *>(
      redo->Delta()->AccessForceNotNull(pg_attribute_all_cols_prm_[postgres::PgAttribute::ATTNOTNULL_COL_OID]));
  auto dsrc_entry = reinterpret_cast<storage::VarlenEntry *>(
      redo->Delta()->AccessForceNotNull(pg_attribute_all_cols_prm_[postgres::PgAttribute::ADSRC_COL_OID]));
  *oid_entry = col_oid;
  *relid_entry = class_oid;
  const auto name_varlen = storage::StorageUtil::CreateVarlen(col.Name());

  *name_entry = name_varlen;
  *type_entry = static_cast<uint32_t>(col.Type());
  // TODO(Amadou): Figure out what really goes here for varlen. Unclear if it's attribute size (16) or varlen length
  *len_entry = (col.Type() == type::TypeId::VARCHAR || col.Type() == type::TypeId::VARBINARY) ? col.MaxVarlenSize()
                                                                                              : col.AttrSize();
  *notnull_entry = !col.Nullable();
  storage::VarlenEntry dsrc_varlen = storage::StorageUtil::CreateVarlen(col.StoredExpression()->ToJson().dump());
  *dsrc_entry = dsrc_varlen;
  // Finally, insert into the table to get the tuple slot
  const auto tupleslot = columns_->Insert(txn, redo);

  // Step 2: Insert into name index
  const auto name_pri = columns_name_index_->GetProjectedRowInitializer();
  // Create a buffer large enough for all columns
  auto *const buffer = common::AllocationUtil::AllocateAligned(name_pri.ProjectedRowSize());
  auto *pr = name_pri.InitializeRow(buffer);
  // Write the attributes in the ProjectedRow. We know the offsets without the map because of the ordering of attribute
  // sizes
  *(reinterpret_cast<storage::VarlenEntry *>(pr->AccessForceNotNull(0))) = name_varlen;
  *(reinterpret_cast<ClassOid *>(pr->AccessForceNotNull(1))) = class_oid;

  if (!columns_name_index_->InsertUnique(txn, *pr, tupleslot)) {
    // There was a name conflict and we need to abort.  Free the buffer and return false to indicate failure
    delete[] buffer;

    // Clean up the varlen's buffer in the case it wasn't inlined.
    if (!name_varlen.IsInlined()) {
      delete[] name_varlen.Content();
    }

    return false;
  }

  // Step 3: Insert into oid index
  const auto oid_pri = columns_oid_index_->GetProjectedRowInitializer();
  auto oid_prm = columns_oid_index_->GetKeyOidToOffsetMap();
  pr = oid_pri.InitializeRow(buffer);
  // Write the attributes in the ProjectedRow. These hardcoded indexkeycol_oids come from
  // Builder::GetColumnOidIndexSchema()
  *(reinterpret_cast<ClassOid *>(pr->AccessForceNotNull(oid_prm[indexkeycol_oid_t(1)]))) = class_oid;
  *(reinterpret_cast<ColOid *>(pr->AccessForceNotNull(oid_prm[indexkeycol_oid_t(2)]))) = col_oid;

  bool UNUSED_ATTRIBUTE result = columns_oid_index_->InsertUnique(txn, *pr, tupleslot);
  NOISEPAGE_ASSERT(result, "Assigned OIDs failed to be unique.");

  // Finish
  delete[] buffer;
  return true;
}

template <typename Column, typename ClassOid, typename ColOid>
std::vector<Column> PgAttributeImpl::GetColumns(const common::ManagedPointer<transaction::TransactionContext> txn,
                                                ClassOid class_oid) {
  // Step 1: Read Index

  const auto oid_pri = columns_oid_index_->GetProjectedRowInitializer();
  auto oid_prm = columns_oid_index_->GetKeyOidToOffsetMap();

  // Buffer is large enough to hold all prs
  byte *const buffer = common::AllocationUtil::AllocateAligned(get_columns_pri_.ProjectedRowSize());
  byte *const key_buffer = common::AllocationUtil::AllocateAligned(oid_pri.ProjectedRowSize());
  // Scan the class index
  NOISEPAGE_ASSERT(get_columns_pri_.ProjectedRowSize() >= oid_pri.ProjectedRowSize(),
                   "Buffer must be large enough to fit largest PR");
  auto *pr = oid_pri.InitializeRow(buffer);
  auto *pr_high = oid_pri.InitializeRow(key_buffer);

  // Write the attributes in the ProjectedRow
  // Low key (class, INVALID_COLUMN_OID)
  *(reinterpret_cast<ClassOid *>(pr->AccessForceNotNull(oid_prm[indexkeycol_oid_t(1)]))) = class_oid;
  *(reinterpret_cast<ColOid *>(pr->AccessForceNotNull(oid_prm[indexkeycol_oid_t(2)]))) = ColOid(0);

  // High key (class + 1, INVALID_COLUMN_OID)
  *(reinterpret_cast<ClassOid *>(pr_high->AccessForceNotNull(oid_prm[indexkeycol_oid_t(1)]))) = ++class_oid;
  *(reinterpret_cast<ColOid *>(pr_high->AccessForceNotNull(oid_prm[indexkeycol_oid_t(2)]))) = ColOid(0);
  std::vector<storage::TupleSlot> index_results;
  columns_oid_index_->ScanAscending(*txn, storage::index::ScanType::Closed, 2, pr, pr_high, 0, &index_results);

  NOISEPAGE_ASSERT(!index_results.empty(),
                   "Incorrect number of results from index scan. empty() implies that function was called with an oid "
                   "that doesn't exist in the Catalog, but binding somehow succeeded. That doesn't make sense.");

  // Step 2: Scan the table to get the columns
  std::vector<Column> cols;
  pr = get_columns_pri_.InitializeRow(buffer);
  for (const auto &slot : index_results) {
    const auto UNUSED_ATTRIBUTE result = columns_->Select(txn, slot, pr);
    NOISEPAGE_ASSERT(result, "Index scan did a visibility check, so Select shouldn't fail at this point.");
    cols.emplace_back(MakeColumn<Column, ColOid>(pr, get_columns_prm_));
  }

  // TODO(Matt): do we have any way to assert that we got the number of attributes we expect? From another attribute in
  // another catalog table maybe?

  // Finish
  delete[] buffer;
  delete[] key_buffer;
  return cols;
}

// TODO(Matt): we need a DeleteColumn()

template <typename Column, typename ClassOid>
bool PgAttributeImpl::DeleteColumns(const common::ManagedPointer<transaction::TransactionContext> txn,
                                    const ClassOid class_oid) {
  // Step 1: Read Index
  const auto oid_pri = columns_oid_index_->GetProjectedRowInitializer();
  auto oid_prm = columns_oid_index_->GetKeyOidToOffsetMap();
  const auto name_pri = columns_name_index_->GetProjectedRowInitializer();

  // Buffer is large enough to hold all prs
  byte *const buffer = common::AllocationUtil::AllocateAligned(delete_columns_pri_.ProjectedRowSize());
  byte *const key_buffer = common::AllocationUtil::AllocateAligned(name_pri.ProjectedRowSize());
  // Scan the class index
  auto *pr = oid_pri.InitializeRow(buffer);
  auto *key_pr = oid_pri.InitializeRow(key_buffer);

  // Write the attributes in the ProjectedRow
  // Low key (class, INVALID_COLUMN_OID) [using uint32_t to avoid adding ColOid to template]
  *(reinterpret_cast<ClassOid *>(pr->AccessForceNotNull(oid_prm[indexkeycol_oid_t(1)]))) = class_oid;
  *(reinterpret_cast<uint32_t *>(pr->AccessForceNotNull(oid_prm[indexkeycol_oid_t(2)]))) = 0;

  auto next_oid = ClassOid(class_oid.UnderlyingValue() + 1);
  // High key (class + 1, INVALID_COLUMN_OID) [using uint32_t to avoid adding ColOid to template]
  *(reinterpret_cast<ClassOid *>(key_pr->AccessForceNotNull(oid_prm[indexkeycol_oid_t(1)]))) = next_oid;
  *(reinterpret_cast<uint32_t *>(key_pr->AccessForceNotNull(oid_prm[indexkeycol_oid_t(2)]))) = 0;
  std::vector<storage::TupleSlot> index_results;
  columns_oid_index_->ScanAscending(*txn, storage::index::ScanType::Closed, 2, pr, key_pr, 0, &index_results);

  NOISEPAGE_ASSERT(!index_results.empty(),
                   "Incorrect number of results from index scan. empty() implies that function was called with an oid "
                   "that doesn't exist in the Catalog, but binding somehow succeeded. That doesn't make sense.");

  // TODO(Matt): do we have any way to assert that we got the number of attributes we expect? From another attribute in
  // another catalog table maybe?

  // Step 2: Scan the table to get the columns
  pr = delete_columns_pri_.InitializeRow(buffer);
  for (const auto &slot : index_results) {
    // 1. Extract attributes from the tuple for the index deletions
    auto UNUSED_ATTRIBUTE result = columns_->Select(txn, slot, pr);
    NOISEPAGE_ASSERT(result, "Index scan did a visibility check, so Select shouldn't fail at this point.");
    const auto *const col_name = reinterpret_cast<const storage::VarlenEntry *const>(
        pr->AccessWithNullCheck(delete_columns_prm_[postgres::PgAttribute::ATTNAME_COL_OID]));
    NOISEPAGE_ASSERT(col_name != nullptr, "Name shouldn't be NULL.");
    const auto *const col_oid = reinterpret_cast<const uint32_t *const>(
        pr->AccessWithNullCheck(delete_columns_prm_[postgres::PgAttribute::ATTNUM_COL_OID]));
    NOISEPAGE_ASSERT(col_oid != nullptr, "OID shouldn't be NULL.");

    // 2. Delete from the table
    txn->StageDelete(db_oid_, postgres::PgAttribute::COLUMN_TABLE_OID, slot);
    result = columns_->Delete(txn, slot);
    if (!result) {
      // Failed to delete one of the columns, clean up and return false to indicate failure
      delete[] buffer;
      delete[] key_buffer;
      return false;
    }

    // 4. Delete from oid index
    key_pr = oid_pri.InitializeRow(key_buffer);
    // Write the attributes in the ProjectedRow. These hardcoded indexkeycol_oids come from
    // Builder::GetColumnOidIndexSchema()
    *(reinterpret_cast<ClassOid *>(key_pr->AccessForceNotNull(oid_prm[indexkeycol_oid_t(1)]))) = class_oid;
    *(reinterpret_cast<uint32_t *>(key_pr->AccessForceNotNull(oid_prm[indexkeycol_oid_t(2)]))) = *col_oid;
    columns_oid_index_->Delete(txn, *key_pr, slot);

    // 5. Delete from name index
    key_pr = name_pri.InitializeRow(key_buffer);
    // Write the attributes in the ProjectedRow. We know the offsets without the map because of the ordering of
    // attribute sizes
    *(reinterpret_cast<storage::VarlenEntry *>(key_pr->AccessForceNotNull(0))) = *col_name;
    *(reinterpret_cast<ClassOid *>(key_pr->AccessForceNotNull(1))) = class_oid;
    columns_name_index_->Delete(txn, *key_pr, slot);
  }
  delete[] buffer;
  delete[] key_buffer;
  return true;
}

template <typename Column, typename ColOid>
Column PgAttributeImpl::MakeColumn(storage::ProjectedRow *const pr, const storage::ProjectionMap &pr_map) {
  const auto col_oid =
      *reinterpret_cast<uint32_t *>(pr->AccessForceNotNull(pr_map.at(postgres::PgAttribute::ATTNUM_COL_OID)));
  const auto col_name = reinterpret_cast<storage::VarlenEntry *>(
      pr->AccessForceNotNull(pr_map.at(postgres::PgAttribute::ATTNAME_COL_OID)));
  const auto col_type = static_cast<type::TypeId>(*reinterpret_cast<uint32_t *>(pr->AccessForceNotNull(
      pr_map.at(postgres::PgAttribute::ATTTYPID_COL_OID))));  // TypeId is a uint8_t enum class, but in the schema it's
  // INTEGER (32-bit)
  const auto col_len =
      *reinterpret_cast<uint16_t *>(pr->AccessForceNotNull(pr_map.at(postgres::PgAttribute::ATTLEN_COL_OID)));
  const auto col_null =
      !(*reinterpret_cast<bool *>(pr->AccessForceNotNull(pr_map.at(postgres::PgAttribute::ATTNOTNULL_COL_OID))));
  const auto *const col_expr =
      reinterpret_cast<storage::VarlenEntry *>(pr->AccessForceNotNull(pr_map.at(postgres::PgAttribute::ADSRC_COL_OID)));

  // TODO(WAN): Why are we deserializing expressions to make a catalog column? This is potentially busted.
  // Our JSON library is also not the most performant.
  // I believe it is OK that the unique ptr goes out of scope because right now both Column constructors copy the expr.
  auto deserialized = parser::DeserializeExpression(nlohmann::json::parse(col_expr->StringView()));

  auto expr = std::move(deserialized.result_);
  NOISEPAGE_ASSERT(deserialized.non_owned_exprs_.empty(), "Congrats, you get to refactor the catalog API.");

  const std::string name(reinterpret_cast<const char *>(col_name->Content()), col_name->Size());
  Column col = (col_type == type::TypeId::VARCHAR || col_type == type::TypeId::VARBINARY)
                   ? Column(name, col_type, col_len, col_null, *expr)
                   : Column(name, col_type, col_null, *expr);

  col.SetOid(ColOid(col_oid));
  return col;
}

template bool PgAttributeImpl::CreateColumn<Schema::Column, table_oid_t>(
    const common::ManagedPointer<transaction::TransactionContext> txn, const table_oid_t class_oid,
    const col_oid_t col_oid, const Schema::Column &col);
template bool PgAttributeImpl::CreateColumn<IndexSchema::Column, index_oid_t>(
    const common::ManagedPointer<transaction::TransactionContext> txn, const index_oid_t class_oid,
    const indexkeycol_oid_t col_oid, const IndexSchema::Column &col);

template std::vector<Schema::Column> PgAttributeImpl::GetColumns<Schema::Column, table_oid_t, col_oid_t>(
    const common::ManagedPointer<transaction::TransactionContext> txn, const table_oid_t class_oid);

template std::vector<IndexSchema::Column>
PgAttributeImpl::GetColumns<IndexSchema::Column, index_oid_t, indexkeycol_oid_t>(
    const common::ManagedPointer<transaction::TransactionContext> txn, const index_oid_t class_oid);

template bool PgAttributeImpl::DeleteColumns<Schema::Column, table_oid_t>(
    const common::ManagedPointer<transaction::TransactionContext> txn, const table_oid_t class_oid);

template bool PgAttributeImpl::DeleteColumns<IndexSchema::Column, index_oid_t>(
    const common::ManagedPointer<transaction::TransactionContext> txn, const index_oid_t class_oid);

template Schema::Column PgAttributeImpl::MakeColumn<Schema::Column, col_oid_t>(storage::ProjectedRow *const pr,
                                                                               const storage::ProjectionMap &pr_map);

template IndexSchema::Column PgAttributeImpl::MakeColumn<IndexSchema::Column, indexkeycol_oid_t>(
    storage::ProjectedRow *const pr, const storage::ProjectionMap &pr_map);

}  // namespace noisepage::catalog::postgres
