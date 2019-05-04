#pragma once

#include <memory>
#include <string>
#include <vector>
#include "catalog/catalog.h"
#include "catalog/catalog_defs.h"
#include "loggers/main_logger.h"
#include "parser/parser_defs.h"
#include "storage/index/index.h"
#include "storage/index/index_factory.h"
#include "storage/sql_table.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_defs.h"
#include "transaction/transaction_manager.h"

namespace terrier::storage::index {

/**
 * An index populator is a helper class that populate the latest version of all tuples in the table
 * visible to the current transaction, pack target columns into ProjectedRow's and insert into
 * the given index.
 *
 * This class can only be used when creating an index.
 */
class IndexBuilder {
 private:
  static Index *GetEmptyIndex(transaction::TransactionContext *txn, catalog::db_oid_t db_oid,
                              catalog::table_oid_t table_oid, catalog::index_oid_t index_oid, bool unique_index,
                              const std::vector<std::string> &key_attrs, catalog::Catalog *catalog) {
    IndexFactory index_factory;
    ConstraintType constraint = (unique_index) ? ConstraintType::UNIQUE : ConstraintType::DEFAULT;
    index_factory.SetOid(index_oid);
    index_factory.SetConstraintType(constraint);
    IndexKeySchema key_schema;
    for (const auto &key_name : key_attrs) {
      auto entry =
          catalog->GetDatabaseHandle().GetAttributeHandle(txn, db_oid).GetAttributeEntry(txn, table_oid, key_name);
      type::TypeId type_id = static_cast<type::TypeId>(entry->GetIntegerColumn("atttypid"));
      if (type::TypeUtil::GetTypeSize(type_id) == VARLEN_COLUMN)
        key_schema.emplace_back(catalog::indexkeycol_oid_t(entry->GetIntegerColumn("oid")), type_id,
                                entry->ColumnIsNull(key_name));
      else
        key_schema.emplace_back(catalog::indexkeycol_oid_t(entry->GetIntegerColumn("oid")), type_id,
                                entry->ColumnIsNull(key_name), entry->GetIntegerColumn("attlen"));
    }
    index_factory.SetKeySchema(key_schema);
    return index_factory.Build();
  }

 public:
  static void CreateConcurrently(catalog::db_oid_t db_oid, catalog::namespace_oid_t ns_oid,
                                 catalog::table_oid_t table_oid, parser::IndexType index_type, bool unique_index,
                                 const std::string &index_name, const std::vector<std::string> &index_attrs,
                                 const std::vector<std::string> &key_attrs, transaction::TransactionManager *txn_mgr,
                                 catalog::Catalog *catalog) {
    transaction::TransactionContext *txn1 = txn_mgr->BeginTransaction();
    catalog::SqlTableHelper *sql_table_helper = catalog->GetUserTable(txn1, db_oid, ns_oid, table_oid);
    // user table does not exist
    if (sql_table_helper == nullptr) {
      txn_mgr->Abort(txn1);
      return;
    }
    std::shared_ptr<SqlTable> sql_table = sql_table_helper->GetSqlTable();
    catalog::IndexHandle index_handle = catalog->GetDatabaseHandle().GetIndexHandle(txn1, db_oid);
    // placeholder args
    catalog::index_oid_t index_oid(catalog->GetNextOid());
    int32_t indnatts = index_attrs.size();
    int32_t indnkeyatts = key_attrs.size();
    bool indisunique = unique_index;
    bool indisprimary = false;
    bool indisvalid = false;
    bool indisready = true;
    bool indislive = false;
    index_handle.AddEntry(txn1, index_oid, table_oid, indnatts, indnkeyatts, indisunique, indisprimary, indisvalid,
                          indisready, indislive);
    Index *index = GetEmptyIndex(txn1, db_oid, table_oid, index_oid, indisunique, key_attrs, catalog);
    transaction::timestamp_t commit_time = txn_mgr->Commit(txn1, nullptr, nullptr);

    // Wait for all transactions older than the timestamp of previous transaction commit
    // TODO(jiaqizuo): use more efficient way to wait for all previous transactions to complete.
    while (txn_mgr->OldestTransactionStartTime() < commit_time) {
    }

    // Start a new transaction to build the index.
    transaction::TransactionContext *build_txn = txn_mgr->BeginTransaction();
    PopulateIndex(build_txn, *sql_table, index);
    index_handle.SetEntryColumn(build_txn, index_oid, "indisready", type::TransientValueFactory::GetBoolean(false));
    index_handle.SetEntryColumn(build_txn, index_oid, "indisvalid", type::TransientValueFactory::GetBoolean(true));
    txn_mgr->Commit(build_txn, nullptr, nullptr);
  }

  /**
   * The method populates all tuples in the table visible to current transaction with latest version,
   * then pack target columns into ProjectedRow's and insert into the given index.
   *
   * @param txn current transaction context
   * @param sql_table the target sql table
   * @param index target index inserted into
   */
  static void PopulateIndex(transaction::TransactionContext *txn,  // NOLINT
                            SqlTable &sql_table, Index *index) {   // NOLINT
    // Create the projected row for the index
    const IndexMetadata &metadata = index->GetIndexMetadata();
    const IndexKeySchema &index_key_schema = metadata.GetKeySchema();
    const auto &index_pr_init = metadata.GetProjectedRowInitializer();
    auto *index_pr_buf = common::AllocationUtil::AllocateAligned(index_pr_init.ProjectedRowSize());
    ProjectedRow *indkey_pr = index_pr_init.InitializeRow(index_pr_buf);

    // Create the projected row for the sql table
    std::vector<catalog::col_oid_t> col_oids;
    for (const auto &it : index_key_schema) {
      col_oids.emplace_back(catalog::col_oid_t(!it.GetOid()));
    }
    auto table_pr_init = sql_table.InitializerForProjectedRow(col_oids).first;
    auto *table_pr_buf = common::AllocationUtil::AllocateAligned(table_pr_init.ProjectedRowSize());
    ProjectedRow *select_pr = table_pr_init.InitializeRow(table_pr_buf);

    // Record the col_id of each column
    const col_id_t *columns = select_pr->ColumnIds();
    uint16_t num_cols = select_pr->NumColumns();
    std::vector<col_id_t> sql_table_cols(columns, columns + num_cols);

    for (const auto &it : sql_table) {
      if (sql_table.Select(txn, it, select_pr)) {
        for (uint16_t i = 0; i < select_pr->NumColumns(); ++i) {
          select_pr->ColumnIds()[i] = indkey_pr->ColumnIds()[i];
        }
        index->Insert(txn, *select_pr, it);
        for (uint16_t i = 0; i < select_pr->NumColumns(); ++i) {
          select_pr->ColumnIds()[i] = sql_table_cols[i];
        }
      }
    }
    delete[] index_pr_buf;
    delete[] table_pr_buf;
  }
};
}  // namespace terrier::storage::index
