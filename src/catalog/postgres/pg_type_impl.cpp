#include "catalog/postgres/pg_type_impl.h"

#include "catalog/database_catalog.h"
#include "catalog/index_schema.h"
#include "catalog/postgres/builder.h"
#include "catalog/postgres/pg_namespace.h"
#include "storage/index/index.h"
#include "storage/sql_table.h"

namespace noisepage::catalog::postgres {

PgTypeImpl::PgTypeImpl(db_oid_t db_oid) : db_oid_(db_oid) {}

void PgTypeImpl::BootstrapPRIs() {
  const std::vector<col_oid_t> pg_type_all_oids{PgType::PG_TYPE_ALL_COL_OIDS.cbegin(),
                                                PgType::PG_TYPE_ALL_COL_OIDS.cend()};
  pg_type_all_cols_pri_ = types_->InitializerForProjectedRow(pg_type_all_oids);
  pg_type_all_cols_prm_ = types_->ProjectionMapForOids(pg_type_all_oids);
}

void PgTypeImpl::Bootstrap(common::ManagedPointer<transaction::TransactionContext> txn,
                           common::ManagedPointer<DatabaseCatalog> dbc) {
  UNUSED_ATTRIBUTE bool retval;

  retval = dbc->CreateTableEntry(txn, PgType::TYPE_TABLE_OID, NAMESPACE_CATALOG_NAMESPACE_OID, "pg_type",
                                 Builder::GetTypeTableSchema());
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = dbc->SetTablePointer(txn, PgType::TYPE_TABLE_OID, types_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  retval =
      dbc->CreateIndexEntry(txn, NAMESPACE_CATALOG_NAMESPACE_OID, PgType::TYPE_TABLE_OID, PgType::TYPE_OID_INDEX_OID,
                            "pg_type_oid_index", Builder::GetTypeOidIndexSchema(db_oid_));
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = dbc->SetIndexPointer(txn, PgType::TYPE_OID_INDEX_OID, types_oid_index_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  retval =
      dbc->CreateIndexEntry(txn, NAMESPACE_CATALOG_NAMESPACE_OID, PgType::TYPE_TABLE_OID, PgType::TYPE_NAME_INDEX_OID,
                            "pg_type_name_index", Builder::GetTypeNameIndexSchema(db_oid_));
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = dbc->SetIndexPointer(txn, PgType::TYPE_NAME_INDEX_OID, types_name_index_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  retval = dbc->CreateIndexEntry(txn, NAMESPACE_CATALOG_NAMESPACE_OID, PgType::TYPE_TABLE_OID,
                                 PgType::TYPE_NAMESPACE_INDEX_OID, "pg_type_namespace_index",
                                 Builder::GetTypeNamespaceIndexSchema(db_oid_));
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = dbc->SetIndexPointer(txn, PgType::TYPE_NAMESPACE_INDEX_OID, types_namespace_index_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  BootstrapTypes(dbc, txn);
}

void PgTypeImpl::InsertType(const common::ManagedPointer<transaction::TransactionContext> txn,
                            const type_oid_t type_oid, const std::string &name, const namespace_oid_t namespace_oid,
                            const int16_t len, const bool by_val, const PgType::Type type_category) {
  // Stage the write into the table
  auto redo_record = txn->StageWrite(db_oid_, PgType::TYPE_TABLE_OID, pg_type_all_cols_pri_);
  auto *delta = redo_record->Delta();

  // Populate oid
  auto offset = pg_type_all_cols_prm_[PgType::TYPOID_COL_OID];
  *(reinterpret_cast<type_oid_t *>(delta->AccessForceNotNull(offset))) = type_oid;

  // Populate type name
  offset = pg_type_all_cols_prm_[PgType::TYPNAME_COL_OID];
  const auto name_varlen = storage::StorageUtil::CreateVarlen(name);

  *(reinterpret_cast<storage::VarlenEntry *>(delta->AccessForceNotNull(offset))) = name_varlen;

  // Populate namespace
  offset = pg_type_all_cols_prm_[PgType::TYPNAMESPACE_COL_OID];
  *(reinterpret_cast<namespace_oid_t *>(delta->AccessForceNotNull(offset))) = namespace_oid;

  // Populate len
  offset = pg_type_all_cols_prm_[PgType::TYPLEN_COL_OID];
  *(reinterpret_cast<int16_t *>(delta->AccessForceNotNull(offset))) = len;

  // Populate byval
  offset = pg_type_all_cols_prm_[PgType::TYPBYVAL_COL_OID];
  *(reinterpret_cast<bool *>(delta->AccessForceNotNull(offset))) = by_val;

  // Populate type
  offset = pg_type_all_cols_prm_[PgType::TYPTYPE_COL_OID];
  auto type = static_cast<uint8_t>(type_category);
  *(reinterpret_cast<uint8_t *>(delta->AccessForceNotNull(offset))) = type;

  // Insert into table
  auto tuple_slot = types_->Insert(txn, redo_record);

  // Allocate buffer of largest size needed
  NOISEPAGE_ASSERT((types_name_index_->GetProjectedRowInitializer().ProjectedRowSize() >=
                    types_oid_index_->GetProjectedRowInitializer().ProjectedRowSize()) &&
                       (types_name_index_->GetProjectedRowInitializer().ProjectedRowSize() >=
                        types_namespace_index_->GetProjectedRowInitializer().ProjectedRowSize()),
                   "Buffer must be allocated for largest ProjectedRow size");
  byte *buffer =
      common::AllocationUtil::AllocateAligned(types_name_index_->GetProjectedRowInitializer().ProjectedRowSize());

  // Insert into oid index
  auto oid_index_delta = types_oid_index_->GetProjectedRowInitializer().InitializeRow(buffer);
  auto oid_index_offset = types_oid_index_->GetKeyOidToOffsetMap().at(catalog::indexkeycol_oid_t(1));
  *(reinterpret_cast<uint32_t *>(oid_index_delta->AccessForceNotNull(oid_index_offset))) = type_oid.UnderlyingValue();
  auto result UNUSED_ATTRIBUTE = types_oid_index_->InsertUnique(txn, *oid_index_delta, tuple_slot);
  NOISEPAGE_ASSERT(result, "Insert into type oid index should always succeed");

  // Insert into (namespace_oid, name) index
  auto name_index_delta = types_name_index_->GetProjectedRowInitializer().InitializeRow(buffer);
  // Populate namespace
  auto name_index_offset = types_name_index_->GetKeyOidToOffsetMap().at(catalog::indexkeycol_oid_t(1));
  *(reinterpret_cast<uint32_t *>(name_index_delta->AccessForceNotNull(name_index_offset))) =
      namespace_oid.UnderlyingValue();
  // Populate type name
  name_index_offset = types_name_index_->GetKeyOidToOffsetMap().at(catalog::indexkeycol_oid_t(2));
  *(reinterpret_cast<storage::VarlenEntry *>(name_index_delta->AccessForceNotNull(name_index_offset))) = name_varlen;
  result = types_name_index_->InsertUnique(txn, *name_index_delta, tuple_slot);
  NOISEPAGE_ASSERT(result, "Insert into type name index should always succeed");

  // Insert into (non-unique) namespace oid index
  auto namespace_index_delta = types_namespace_index_->GetProjectedRowInitializer().InitializeRow(buffer);
  auto namespace_index_offset = types_namespace_index_->GetKeyOidToOffsetMap().at(catalog::indexkeycol_oid_t(1));
  *(reinterpret_cast<uint32_t *>(namespace_index_delta->AccessForceNotNull(namespace_index_offset))) =
      namespace_oid.UnderlyingValue();
  result = types_namespace_index_->Insert(txn, *name_index_delta, tuple_slot);
  NOISEPAGE_ASSERT(result, "Insert into type namespace index should always succeed");

  delete[] buffer;
}

void PgTypeImpl::BootstrapTypes(const common::ManagedPointer<DatabaseCatalog> dbc,
                                const common::ManagedPointer<transaction::TransactionContext> txn) {
#define INSERT_BASE_TYPE(type_id, type_name, type_size)                                                               \
  InsertType(txn, dbc->GetTypeOidForType((type_id)), (type_name), NAMESPACE_CATALOG_NAMESPACE_OID, (type_size), true, \
             PgType::Type::BASE)

  INSERT_BASE_TYPE(type::TypeId::INVALID, "invalid", 1);
  INSERT_BASE_TYPE(type::TypeId::BOOLEAN, "boolean", sizeof(bool));
  INSERT_BASE_TYPE(type::TypeId::TINYINT, "tinyint", sizeof(int8_t));
  INSERT_BASE_TYPE(type::TypeId::SMALLINT, "smallint", sizeof(int16_t));
  INSERT_BASE_TYPE(type::TypeId::INTEGER, "integer", sizeof(int32_t));
  INSERT_BASE_TYPE(type::TypeId::BIGINT, "bigint", sizeof(int64_t));
  INSERT_BASE_TYPE(type::TypeId::DECIMAL, "decimal", sizeof(double));
  INSERT_BASE_TYPE(type::TypeId::DATE, "date", sizeof(type::date_t));
  INSERT_BASE_TYPE(type::TypeId::TIMESTAMP, "timestamp", sizeof(type::timestamp_t));

#undef INSERT_BASE_TYPE

  InsertType(txn, dbc->GetTypeOidForType(type::TypeId::VARCHAR), "varchar", NAMESPACE_CATALOG_NAMESPACE_OID, -1, false,
             PgType::Type::BASE);

  InsertType(txn, dbc->GetTypeOidForType(type::TypeId::VARBINARY), "varbinary", NAMESPACE_CATALOG_NAMESPACE_OID, -1,
             false, PgType::Type::BASE);

  InsertType(txn, dbc->GetTypeOidForType(type::TypeId::HACK_PG_TYPE_VAR_ARRAY), "var_array",
             NAMESPACE_CATALOG_NAMESPACE_OID, -1, false, PgType::Type::COMPOSITE);
}

}  // namespace noisepage::catalog::postgres
