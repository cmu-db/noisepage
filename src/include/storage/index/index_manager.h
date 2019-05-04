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
 * An index builder is a class that creates an index on key attributes specified by users. It can create
 * the index both in the non-blocking manner and the blocking manner, which is also determined by users.
 *
 * This class can only be used when creating an index.
 */
class IndexManager {
 private:
  /**
   * The method sets the type of constraints and the key schema for the index. It finally returns an empty
   * index for further insertion.
   *
   * @param txn the context of index building transaction
   * @param db_oid the oid of the database
   * @param table_oid the oid of the target table
   * @param index_oid the oid of the index given by catalog
   * @param unique_index whether the index has unique constraint or not
   * @param key_attrs all attributes in the key
   * @param catalog the pointer to the catalog
   * @return an empty index with metadata set
   */
  static Index *GetEmptyIndex(transaction::TransactionContext *txn, catalog::db_oid_t db_oid,
                              catalog::table_oid_t table_oid, catalog::index_oid_t index_oid, bool unique_index,
                              const std::vector<std::string> &key_attrs, catalog::Catalog *catalog) {
    // Setup the oid and constraint type for the index
    IndexFactory index_factory;
    ConstraintType constraint = (unique_index) ? ConstraintType::UNIQUE : ConstraintType::DEFAULT;
    index_factory.SetOid(index_oid);
    index_factory.SetConstraintType(constraint);

    // Setup the key schema for the index
    IndexKeySchema key_schema;
    for (const auto &key_name : key_attrs) {
      // Get the catalog entry for each attribute
      auto entry =
          catalog->GetDatabaseHandle().GetAttributeHandle(txn, db_oid).GetAttributeEntry(txn, table_oid, key_name);

      // If there is no corresponding entry in the catalog, return nullptr.
      if (entry == nullptr) return nullptr;

      // Fill in the key schema with information from catalog entry
      type::TypeId type_id = static_cast<type::TypeId>(entry->GetIntegerColumn("atttypid"));
      if (type::TypeUtil::GetTypeSize(type_id) == VARLEN_COLUMN)
        key_schema.emplace_back(catalog::indexkeycol_oid_t(entry->GetIntegerColumn("oid")), type_id,
                                entry->ColumnIsNull(key_name));
      else
        key_schema.emplace_back(catalog::indexkeycol_oid_t(entry->GetIntegerColumn("oid")), type_id,
                                entry->ColumnIsNull(key_name), entry->GetIntegerColumn("attlen"));
    }
    index_factory.SetKeySchema(key_schema);

    // Build an empty index
    return index_factory.Build();
  }

 public:
  /**
   * The method can create the index in a non-blocking manner. It launches two transactions to build the index.
   * The first transaction inserts a new entry on the index into catalog and creates an empty index with all metadata
   * set. The second transaction inserts all keys in its snapshot into the index. Before the start of second
   * transaction, it needs to wait for all other transactions with timestamp smaller than the commit timestamp of the
   * first transaction to complete.
   *
   * @param db_oid the oid of the database
   * @param ns_oid the oid of the namespace
   * @param table_oid the oid of the table
   * @param index_type the type of the index
   * @param unique_index whether the index is unique
   * @param index_name the name of the index
   * @param index_attrs all attributes indexed on
   * @param key_attrs all attributes of the key
   * @param txn_mgr the pointer to the transaction manager
   * @param catalog the pointer to the catalog
   */
  static void CreateConcurrently(catalog::db_oid_t db_oid, catalog::namespace_oid_t ns_oid,
                                 catalog::table_oid_t table_oid, parser::IndexType index_type, bool unique_index,
                                 const std::string &index_name, const std::vector<std::string> &index_attrs,
                                 const std::vector<std::string> &key_attrs, transaction::TransactionManager *txn_mgr,
                                 catalog::Catalog *catalog) {
    // First transaction to insert an entry for the index in the catalog
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
    auto indnatts = static_cast<int32_t>(index_attrs.size());
    auto indnkeyatts = static_cast<int32_t>(key_attrs.size());
    bool indisunique = unique_index;
    bool indisprimary = false;
    bool indisvalid = false;
    bool indisready = true;
    bool indislive = false;
    index_handle.AddEntry(txn1, index_oid, table_oid, indnatts, indnkeyatts, indisunique, indisprimary, indisvalid,
                          indisready, indislive);

    Index *index = GetEmptyIndex(txn1, db_oid, table_oid, index_oid, indisunique, key_attrs, catalog);
    // Initializing the index fails
    if (index == nullptr) {
      txn_mgr->Abort(txn1);
      return;
    }
    // Commit first transaction
    transaction::timestamp_t commit_time = txn_mgr->Commit(txn1, nullptr, nullptr);

    // Wait for all transactions older than the timestamp of previous transaction commit
    // TODO(jiaqizuo): use more efficient way to wait for all previous transactions to complete.
    while (txn_mgr->OldestTransactionStartTime() < commit_time) {
    }

    // Start the second transaction to insert all keys into the index.
    transaction::TransactionContext *build_txn = txn_mgr->BeginTransaction();
    // Change "indisready" to false and "indisvalid" to the result of populating the index in the catalog entry
    index_handle.SetEntryColumn(build_txn, index_oid, "indisready", type::TransientValueFactory::GetBoolean(false));
    index_handle.SetEntryColumn(
        build_txn, index_oid, "indisvalid",
        type::TransientValueFactory::GetBoolean(PopulateIndex(build_txn, *sql_table, index, unique_index)));
    // Commit the transaction
    txn_mgr->Commit(build_txn, nullptr, nullptr);
  }

  /**
   * The method populates all tuples in the table visible to current transaction with latest version,
   * then pack target columns into ProjectedRow's and insert into the given index.
   *
   * @param txn current transaction context
   * @param sql_table the target sql table
   * @param index target index inserted into
   * @param unique_index whether the index is unique or not
   * @return true if populating index successes otherwise false
   */
  static bool PopulateIndex(transaction::TransactionContext *txn,                    // NOLINT
                            SqlTable &sql_table, Index *index, bool unique_index) {  // NOLINT
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

    bool success = true;
    for (const auto &it : sql_table) {
      if (sql_table.Select(txn, it, select_pr)) {
        for (uint16_t i = 0; i < select_pr->NumColumns(); ++i) {
          select_pr->ColumnIds()[i] = indkey_pr->ColumnIds()[i];
        }
        // Check whether the insertion successes
        if (unique_index) {
          if (!index->InsertUnique(txn, *select_pr, it)) {
            success = false;
            break;
          }
        } else {
          if (!index->Insert(txn, *select_pr, it)) {
            success = false;
            break;
          }
        }

        for (uint16_t i = 0; i < select_pr->NumColumns(); ++i) {
          select_pr->ColumnIds()[i] = sql_table_cols[i];
        }
      }
    }
    delete[] index_pr_buf;
    delete[] table_pr_buf;
    return success;
  }
};
}  // namespace terrier::storage::index
