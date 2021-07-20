#include "catalog/postgres/pg_type_impl.h"

#include "catalog/database_catalog.h"
#include "catalog/index_schema.h"
#include "catalog/postgres/builder.h"
#include "catalog/postgres/pg_namespace.h"
#include "catalog/schema.h"
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
  dbc->BootstrapTable(txn, PgType::TYPE_TABLE_OID, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID, "pg_type",
                      Builder::GetTypeTableSchema(), types_);
  dbc->BootstrapIndex(txn, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID, PgType::TYPE_TABLE_OID,
                      PgType::TYPE_OID_INDEX_OID, "pg_type_oid_index", Builder::GetTypeOidIndexSchema(db_oid_),
                      types_oid_index_);
  dbc->BootstrapIndex(txn, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID, PgType::TYPE_TABLE_OID,
                      PgType::TYPE_NAME_INDEX_OID, "pg_type_name_index", Builder::GetTypeNameIndexSchema(db_oid_),
                      types_name_index_);
  dbc->BootstrapIndex(txn, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID, PgType::TYPE_TABLE_OID,
                      PgType::TYPE_NAMESPACE_INDEX_OID, "pg_type_namespace_index",
                      Builder::GetTypeNamespaceIndexSchema(db_oid_), types_namespace_index_);
  BootstrapTypes(dbc, txn);
}

void PgTypeImpl::InsertType(const common::ManagedPointer<transaction::TransactionContext> txn,
                            const type_oid_t type_oid, const std::string &name, const namespace_oid_t namespace_oid,
                            const int16_t len, const bool by_val, const PgType::Type type_category) {
  auto redo_record = txn->StageWrite(db_oid_, PgType::TYPE_TABLE_OID, pg_type_all_cols_pri_);
  auto delta = common::ManagedPointer(redo_record->Delta());

  const auto name_varlen = storage::StorageUtil::CreateVarlen(name);  // pg_type and pg_type_name_index use this.

  // Prepare PR for insertion.
  {
    auto &pm = pg_type_all_cols_prm_;
    PgType::TYPOID.Set(delta, pm, type_oid);
    PgType::TYPNAME.Set(delta, pm, name_varlen);
    PgType::TYPNAMESPACE.Set(delta, pm, namespace_oid);
    PgType::TYPLEN.Set(delta, pm, len);
    PgType::TYPBYVAL.Set(delta, pm, by_val);
    PgType::TYPTYPE.Set(delta, pm, static_cast<uint8_t>(type_category));
  }

  // Insert into pg_type.
  auto tuple_slot = types_->Insert(txn, redo_record);

  // Allocate a buffer of the largest size needed.
  NOISEPAGE_ASSERT((types_name_index_->GetProjectedRowInitializer().ProjectedRowSize() >=
                    types_oid_index_->GetProjectedRowInitializer().ProjectedRowSize()) &&
                       (types_name_index_->GetProjectedRowInitializer().ProjectedRowSize() >=
                        types_namespace_index_->GetProjectedRowInitializer().ProjectedRowSize()),
                   "Buffer must be allocated for the largest ProjectedRow size.");
  byte *buffer =
      common::AllocationUtil::AllocateAligned(types_name_index_->GetProjectedRowInitializer().ProjectedRowSize());

  // Insert into pg_type_oid_index.
  {
    auto oid_index_delta = types_oid_index_->GetProjectedRowInitializer().InitializeRow(buffer);
    auto typoid_offset = types_oid_index_->GetKeyOidToOffsetMap().at(catalog::indexkeycol_oid_t(1));
    oid_index_delta->Set<type_oid_t, false>(typoid_offset, type_oid, false);
    auto UNUSED_ATTRIBUTE result = types_oid_index_->InsertUnique(txn, *oid_index_delta, tuple_slot);
    NOISEPAGE_ASSERT(result, "Insert into pg_type_oid_index should always succeed");
  }

  // Insert into pg_type_name_index.
  {
    auto name_index_delta = types_name_index_->GetProjectedRowInitializer().InitializeRow(buffer);
    auto typnamespace_offset = types_name_index_->GetKeyOidToOffsetMap().at(catalog::indexkeycol_oid_t(1));
    auto typname_offset = types_name_index_->GetKeyOidToOffsetMap().at(catalog::indexkeycol_oid_t(2));
    name_index_delta->Set<namespace_oid_t, false>(typnamespace_offset, namespace_oid, false);
    name_index_delta->Set<storage::VarlenEntry, false>(typname_offset, name_varlen, false);
    auto UNUSED_ATTRIBUTE result = types_name_index_->InsertUnique(txn, *name_index_delta, tuple_slot);
    NOISEPAGE_ASSERT(result, "Insert into pg_type_name_index should always succeed");
  }

  // Insert into pg_type_namespace_index.
  {
    auto namespace_index_delta = types_namespace_index_->GetProjectedRowInitializer().InitializeRow(buffer);
    auto typnamespace_offset = types_namespace_index_->GetKeyOidToOffsetMap().at(catalog::indexkeycol_oid_t(1));
    namespace_index_delta->Set<namespace_oid_t, false>(typnamespace_offset, namespace_oid, false);
    auto UNUSED_ATTRIBUTE result = types_namespace_index_->Insert(txn, *namespace_index_delta, tuple_slot);
    NOISEPAGE_ASSERT(result, "Insert into pg_type_namespace_index should always succeed");
  }

  delete[] buffer;
}

void PgTypeImpl::BootstrapTypes(const common::ManagedPointer<DatabaseCatalog> dbc,
                                const common::ManagedPointer<transaction::TransactionContext> txn) {
  auto insert_base_type = [&](const execution::sql::SqlTypeId type, const std::string &type_name,
                              const int16_t type_size) {
    InsertType(txn, dbc->GetTypeOidForType(type), type_name, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID, type_size,
               true, PgType::Type::BASE);
  };

  insert_base_type(execution::sql::SqlTypeId::Boolean, "boolean",
                   execution::sql::GetSqlTypeIdSize(execution::sql::SqlTypeId::Boolean));
  insert_base_type(execution::sql::SqlTypeId::TinyInt, "tinyint",
                   execution::sql::GetSqlTypeIdSize(execution::sql::SqlTypeId::TinyInt));
  insert_base_type(execution::sql::SqlTypeId::SmallInt, "smallint",
                   execution::sql::GetSqlTypeIdSize(execution::sql::SqlTypeId::SmallInt));
  insert_base_type(execution::sql::SqlTypeId::Integer, "integer",
                   execution::sql::GetSqlTypeIdSize(execution::sql::SqlTypeId::Integer));
  insert_base_type(execution::sql::SqlTypeId::BigInt, "bigint",
                   execution::sql::GetSqlTypeIdSize(execution::sql::SqlTypeId::BigInt));
  insert_base_type(execution::sql::SqlTypeId::Double, "double",
                   execution::sql::GetSqlTypeIdSize(execution::sql::SqlTypeId::Double));
  insert_base_type(execution::sql::SqlTypeId::Decimal, "decimal",
                   execution::sql::GetSqlTypeIdSize(execution::sql::SqlTypeId::Decimal));
  insert_base_type(execution::sql::SqlTypeId::Date, "date",
                   execution::sql::GetSqlTypeIdSize(execution::sql::SqlTypeId::Date));
  insert_base_type(execution::sql::SqlTypeId::Timestamp, "timestamp",
                   execution::sql::GetSqlTypeIdSize(execution::sql::SqlTypeId::Timestamp));

  InsertType(txn, dbc->GetTypeOidForType(execution::sql::SqlTypeId::Varchar), "varchar",
             PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID, -1, false, PgType::Type::BASE);

  InsertType(txn, dbc->GetTypeOidForType(execution::sql::SqlTypeId::Varbinary), "varbinary",
             PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID, -1, false, PgType::Type::BASE);
}

}  // namespace noisepage::catalog::postgres
