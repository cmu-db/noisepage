#pragma once

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "catalog/postgres/pg_constraint_impl.h"
#include "catalog/postgres/pg_core_impl.h"
#include "catalog/postgres/pg_language_impl.h"
#include "catalog/postgres/pg_proc_impl.h"
#include "catalog/postgres/pg_statistic_impl.h"
#include "catalog/postgres/pg_type_impl.h"
#include "common/managed_pointer.h"
#include "optimizer/statistics/column_stats.h"
#include "optimizer/statistics/table_stats.h"

namespace noisepage::transaction {
class TransactionContext;
}

namespace noisepage::storage {
class GarbageCollector;
class RecoveryManager;
class SqlTable;
namespace index {
class Index;
}
}  // namespace noisepage::storage

namespace noisepage::catalog {
/**
 * DatabaseCatalog stores all of the metadata about user tables and user defined database objects
 * so that other parts of the system (i.e., binder, optimizer, and execution engine)
 * can reason about and execute operations on these objects.
 *
 * @warning     Only Catalog, CatalogAccessor, and RecoveryManager should be using the interface below.
 *              All other code should use the CatalogAccessor API, which:
 *              - enforces scoping to a specific database, and
 *              - handles namespace resolution for finding tables within that database.
 */
class DatabaseCatalog {
 public:
  /**
   * @brief Bootstrap the entire catalog with default entries.
   * @param txn         The transaction to bootstrap in.
   */
  void Bootstrap(common::ManagedPointer<transaction::TransactionContext> txn);

  /** @brief Create a new namespace, may fail with INVALID_NAMESPACE_OID. @see PgCoreImpl::CreateNamespace */
  namespace_oid_t CreateNamespace(common::ManagedPointer<transaction::TransactionContext> txn, const std::string &name);
  /** @brief Delete the specified namespace. @see PgCoreImpl::DeleteNamespace */
  bool DeleteNamespace(common::ManagedPointer<transaction::TransactionContext> txn, namespace_oid_t ns_oid);
  /** @brief Get the OID of the specified namespace. @see PgCoreImpl::GetNamespaceOid */
  namespace_oid_t GetNamespaceOid(common::ManagedPointer<transaction::TransactionContext> txn, const std::string &name);

  /** @brief Create a new table, may fail with INVALID_TABLE_OID. @see PgCoreImpl::CreateTable */
  table_oid_t CreateTable(common::ManagedPointer<transaction::TransactionContext> txn, namespace_oid_t ns,
                          const std::string &name, const Schema &schema);
  /** @brief Delete the specified table. @see PgCoreImpl::DeleteTable */
  bool DeleteTable(common::ManagedPointer<transaction::TransactionContext> txn, table_oid_t table);
  /** @brief Rename a table. @see PgCoreImpl::RenameTable */
  bool RenameTable(common::ManagedPointer<transaction::TransactionContext> txn, table_oid_t table,
                   const std::string &name);

  /**
   * @brief Set the location of the underlying storage for the specified table.
   *
   * @param txn         The transaction for the operation.
   * @param table       The OID of the table in the catalog.
   * @param table_ptr   The pointer to the underlying storage in memory.
   * @return            True if the operation was successful. False otherwise.
   *
   * @warning   The SqlTable pointer that is passed in must be on the heap as the catalog will take
   *            ownership of it and schedule its deletion with the GC at the appropriate time.
   * @warning   It is unsafe to call delete on the SqlTable pointer after calling this function.
   *            This is regardless of the return status.
   */
  bool SetTablePointer(common::ManagedPointer<transaction::TransactionContext> txn, table_oid_t table,
                       const storage::SqlTable *table_ptr);
  /**
   * @brief Set the location of the underlying implementation for the specified index.
   *
   * @param txn         The transaction for the operation.
   * @param index       The OID of the index in the catalog.
   * @param index_ptr   The pointer to the underlying index implementation in memory.
   * @return            True if the operation was successful. False otherwise.
   *
   * @warning   The Index pointer that is passed in must be on the heap as the catalog will take
   *            ownership of it and schedule its deletion with the GC at the appropriate time.
   * @warning   It is unsafe to call delete on the Index pointer after calling this function.
   *            This is regardless of the return status.
   */
  bool SetIndexPointer(common::ManagedPointer<transaction::TransactionContext> txn, index_oid_t index,
                       storage::index::Index *index_ptr);

  /** @brief Get the OID for the specified table, or INVALID_TABLE_OID if no such REGULAR_TABLE exists. */
  table_oid_t GetTableOid(common::ManagedPointer<transaction::TransactionContext> txn, namespace_oid_t ns,
                          const std::string &name);
  /** @brief Get the OID for the specified index, or INVALID_INDEX_OID if no such INDEX exists. */
  index_oid_t GetIndexOid(common::ManagedPointer<transaction::TransactionContext> txn, namespace_oid_t ns,
                          const std::string &name);

  /** @brief Get the storage pointer for the specified table, or nullptr if no such REGULAR_TABLE exists. */
  common::ManagedPointer<storage::SqlTable> GetTable(common::ManagedPointer<transaction::TransactionContext> txn,
                                                     table_oid_t table);
  /** @brief Get the index pointer for the specified index, or nullptr if no such INDEX exists. */
  common::ManagedPointer<storage::index::Index> GetIndex(common::ManagedPointer<transaction::TransactionContext> txn,
                                                         index_oid_t index);

  /** @brief Get the schema for the specified table. */
  const Schema &GetSchema(common::ManagedPointer<transaction::TransactionContext> txn, table_oid_t table);
  /** @brief Get the index schema for the specified index. */
  const IndexSchema &GetIndexSchema(common::ManagedPointer<transaction::TransactionContext> txn, index_oid_t index);

  /**
   * @brief Update the schema of the table.
   *
   * Apply a new schema to the given table.
   * The changes will modify the latest schema as provided by the catalog.
   * There is no guarantee that the OIDs for modified columns will be stable across a schema change.
   *
   * @param txn         The transaction to update the table's schema in.
   * @param table       The table whose schema should be updated.
   * @param new_schema  The new schema to update the table to.
   * @return            True if the update succeeded. False otherwise.
   *
   * @warning           The catalog accessor assumes it takes ownership of the schema object that is passed.
   *                    As such, there is no guarantee that the pointer is still valid when this function returns.
   *                    If the caller needs to reference the schema object after this call, the caller should use
   *                    the GetSchema function to obtain the authoritative schema for this table.
   */
  bool UpdateSchema(common::ManagedPointer<transaction::TransactionContext> txn, table_oid_t table, Schema *new_schema);

  /** @brief Create an index, may fail with INVALID_INDEX_OID. @see PgCoreImpl::CreateIndex */
  index_oid_t CreateIndex(common::ManagedPointer<transaction::TransactionContext> txn, namespace_oid_t ns,
                          const std::string &name, table_oid_t table, const IndexSchema &schema);
  /** @brief Delete the specified index. @see PgCoreImpl::DeleteIndex */
  bool DeleteIndex(common::ManagedPointer<transaction::TransactionContext> txn, index_oid_t index);
  /** @brief Get all of the index OIDs for a specific table. @see PgCoreImpl::GetIndexOids */
  std::vector<index_oid_t> GetIndexOids(common::ManagedPointer<transaction::TransactionContext> txn, table_oid_t table);
  /** @brief More efficient way of getting all the indexes for a specific table. @see PgCoreImpl::GetIndexes */
  std::vector<std::pair<common::ManagedPointer<storage::index::Index>, const IndexSchema &>> GetIndexes(
      common::ManagedPointer<transaction::TransactionContext> txn, table_oid_t table);

  /** @return The type_oid_t that corresponds to the internal TypeId. */
  type_oid_t GetTypeOidForType(type::TypeId type);

  /** @brief Get a list of all of the constraints for the specified table. */
  std::vector<constraint_oid_t> GetConstraints(common::ManagedPointer<transaction::TransactionContext> txn,
                                               table_oid_t table);

  /** @brief Create a new language, may fail with INVALID_LANGUAGE_OID. @see PgLanguageImpl::CreateLanguage */
  language_oid_t CreateLanguage(common::ManagedPointer<transaction::TransactionContext> txn,
                                const std::string &lanname);
  /** @brief Drop the specified language. @see PgLanguageImpl::DropLanguage */
  bool DropLanguage(common::ManagedPointer<transaction::TransactionContext> txn, language_oid_t oid);
  /** @brief Get the OID of the specified language. @see PgLanguageImpl::GetLanguageOid */
  language_oid_t GetLanguageOid(common::ManagedPointer<transaction::TransactionContext> txn,
                                const std::string &lanname);

  /** @brief Create a new procedure, may fail with INVALID_PROC_OID. @see PgProcImpl::CreateProcedure */
  proc_oid_t CreateProcedure(common::ManagedPointer<transaction::TransactionContext> txn, const std::string &procname,
                             language_oid_t language_oid, namespace_oid_t procns, const std::vector<std::string> &args,
                             const std::vector<type_oid_t> &arg_types, const std::vector<type_oid_t> &all_arg_types,
                             const std::vector<postgres::PgProc::ArgModes> &arg_modes, type_oid_t rettype,
                             const std::string &src, bool is_aggregate);
  /** @brief Drop the specified procedure. @see PgProcImpl::DropProcedure */
  bool DropProcedure(common::ManagedPointer<transaction::TransactionContext> txn, proc_oid_t proc);
  /** @brief Get the OID of the specified procedure. @see PgProcImpl::GetProcOid */
  proc_oid_t GetProcOid(common::ManagedPointer<transaction::TransactionContext> txn, namespace_oid_t procns,
                        const std::string &procname, const std::vector<type_oid_t> &all_arg_types);
  /** @brief Set the procedure context for the specified procedure. @see PgProcImpl::SetFunctionContextPointer */
  bool SetFunctionContextPointer(common::ManagedPointer<transaction::TransactionContext> txn, proc_oid_t proc_oid,
                                 const execution::functions::FunctionContext *func_context);
  /** @brief Get the procedure context for the specified procedure. @see PgProcImpl::GetFunctionContext */
  common::ManagedPointer<execution::functions::FunctionContext> GetFunctionContext(
      common::ManagedPointer<transaction::TransactionContext> txn, proc_oid_t proc_oid);

  /** @brief Get the statistics for the specified column. @see PgStatisticImpl::GetColumnStatistics */
  std::unique_ptr<optimizer::ColumnStatsBase> GetColumnStatistics(
      common::ManagedPointer<transaction::TransactionContext> txn, table_oid_t table_oid, col_oid_t col_oid);

  /** @brief Get the statistics for the specified table. @see PgStatisticImpl::GetTableStatistics */
  optimizer::TableStats GetTableStatistics(common::ManagedPointer<transaction::TransactionContext> txn,
                                           table_oid_t table_oid);

 private:
  /**
   * The maximum number of tuples to be read out at a time when scanning tables during teardown.
   * This is arbitrary and defined here so that all PgBlahImpl classes can use the same value.
   */
  static constexpr uint32_t TEARDOWN_MAX_TUPLES = 100;

  /**
   * DatabaseCatalog methods generally handle coarse-grained locking. The various PgXXXImpl classes need to invoke
   * private DatabaseCatalog methods such as CreateTableEntry and CreateIndexEntry during the Bootstrap process.
   */
  ///@{
  friend class postgres::PgCoreImpl;
  friend class postgres::PgConstraintImpl;
  friend class postgres::PgLanguageImpl;
  friend class postgres::PgProcImpl;
  friend class postgres::PgTypeImpl;
  friend class postgres::PgStatisticImpl;
  ///@}
  friend class Catalog;                   ///< Accesses write_lock_ (creating accessor) and TearDown (cleanup).
  friend class postgres::Builder;         ///< Initializes DatabaseCatalog's tables.
  friend class storage::RecoveryManager;  ///< Directly modifies DatabaseCatalog's tables.

  // Miscellaneous state.
  std::atomic<uint32_t> next_oid_;                    ///< The next OID, shared across different pg tables.
  std::atomic<transaction::timestamp_t> write_lock_;  ///< Used to prevent concurrent DDL change.
  const db_oid_t db_oid_;  ///< The OID of the database that this DatabaseCatalog is established in.
  const common::ManagedPointer<storage::GarbageCollector> garbage_collector_;  ///< The garbage collector used.

  // The Postgres tables.
  postgres::PgCoreImpl pg_core_;              ///< Core Postgres tables: pg_namespace, pg_class, pg_index, pg_attribute.
  postgres::PgTypeImpl pg_type_;              ///< Types: pg_type.
  postgres::PgConstraintImpl pg_constraint_;  ///< Constraints: pg_constraint.
  postgres::PgLanguageImpl pg_language_;      ///< Languages: pg_language.
  postgres::PgProcImpl pg_proc_;              ///< Procedures: pg_proc.
  postgres::PgStatisticImpl pg_stat_;         ///< Statistics: pg_statistic.

  /** @brief Create a new DatabaseCatalog. Does not create any tables until Bootstrap is called. */
  DatabaseCatalog(db_oid_t oid, common::ManagedPointer<storage::GarbageCollector> garbage_collector);

  /**
   * @brief Create all of the ProjectedRowInitializer and ProjectionMap objects for the catalog.
   *        The initializers and maps can be stashed because the catalog should not undergo schema changes at runtime.
   */
  void BootstrapPRIs();

  /** @brief Cleanup the tables and indexes maintained by the DatabaseCatalog. */
  void TearDown(common::ManagedPointer<transaction::TransactionContext> txn);

  /**
   * @brief Lock the DatabaseCatalog to disallow concurrent DDL changes.
   *
   * Internal function to DatabaseCatalog to disallow concurrent DDL changes.
   * This also disallows older txns to enact DDL changes after a newer transaction has committed one.
   * This effectively follows the same timestamp ordering logic as the version pointer MVCC stuff in the storage layer.
   * It also serializes all DDL within a database.
   *
   * @param txn     Requesting transaction.
   *                Used to inspect the timestamp and register commit/abort events to release the lock if acquired.
   * @return        True if the lock was acquired. False otherwise.
   *
   * @warning       This requires that commit actions be performed after the commit time is stored
   *                in the TransactionContext's FinishTime.
   */
  bool TryLock(common::ManagedPointer<transaction::TransactionContext> txn);

  /**
   * @brief Atomically update the next oid counter to the max of the current count and the provided next oid.
   * @param oid     The next oid to move the oid counter to.
   */
  void UpdateNextOid(uint32_t oid) {
    uint32_t expected, desired;
    do {
      expected = next_oid_.load();
      desired = std::max(expected, oid);
    } while (!next_oid_.compare_exchange_weak(expected, desired));
  }

  /** @brief Wrapper around calling CreateTableEntry and SetTablePointer. */
  void BootstrapTable(common::ManagedPointer<transaction::TransactionContext> txn, table_oid_t table_oid,
                      namespace_oid_t ns_oid, const std::string &name, const Schema &schema,
                      common::ManagedPointer<storage::SqlTable> table_ptr);
  /** @brief Wrapper around calling CreateIndexEntry and SetIndexPointer. */
  void BootstrapIndex(common::ManagedPointer<transaction::TransactionContext> txn, namespace_oid_t ns_oid,
                      table_oid_t table_oid, index_oid_t index_oid, const std::string &name, const IndexSchema &schema,
                      common::ManagedPointer<storage::index::Index> index_ptr);

  /**
   * @brief Create a new table entry WITHOUT TAKING THE DDL LOCK. Used by other members of DatabaseCatalog.
   * @see   PgCoreImpl::CreateTableEntry
   */
  bool CreateTableEntry(common::ManagedPointer<transaction::TransactionContext> txn, table_oid_t table_oid,
                        namespace_oid_t ns_oid, const std::string &name, const Schema &schema);
  /**
   * @brief Create a new table entry WITHOUT TAKING THE DDL LOCK. Used by other members of DatabaseCatalog.
   * @see   PgCoreImpl::CreateIndexEntry
   */
  bool CreateIndexEntry(common::ManagedPointer<transaction::TransactionContext> txn, namespace_oid_t ns_oid,
                        table_oid_t table_oid, index_oid_t index_oid, const std::string &name,
                        const IndexSchema &schema);
  /**
   * @brief Delete all of the indexes for a given table.
   *
   * This is currently designed as an internal function, though it could be exposed via CatalogAccessor if desired.
   *
   * @param txn     The transaction to perform the deletions in.
   * @param table   The OID of the table to remove all indexes for.
   * @return        True if the deletion succeeded. False otherwise.
   */
  bool DeleteIndexes(common::ManagedPointer<transaction::TransactionContext> txn, table_oid_t table);

  /** @brief Get all the columns for a particular pg_attribute entry. @see PgAttributeImpl::GetColumns */
  template <typename Column, typename ClassOid, typename ColOid>
  std::vector<Column> GetColumns(common::ManagedPointer<transaction::TransactionContext> txn, ClassOid class_oid);

  /**
   * @brief Set the schema of a table in pg_class.
   *
   * @tparam CallerType     The type of the caller. Should only be used by recovery!
   * @param txn             The transaction to perform the schema change in.
   * @param oid             The OID of the table.
   * @param schema          The new schema to set.
   * @return                True if the schema was set successfully. False otherwise.
   */
  template <typename CallerType>
  bool SetTableSchemaPointer(common::ManagedPointer<transaction::TransactionContext> txn, table_oid_t oid,
                             const Schema *schema) {
    static_assert(std::is_same_v<CallerType, storage::RecoveryManager>, "Only recovery should call this.");
    return SetClassPointer(txn, oid, schema, postgres::PgClass::REL_SCHEMA.oid_);
  }

  /**
   * @brief Set the schema of an index in pg_class.
   *
   * @tparam CallerType     The type of the caller. Should only be used by recovery!
   * @param txn             The transaction to perform the schema change in.
   * @param oid             The OID of the index.
   * @param schema          The new index schema to set.
   * @return                True if the index schema was set successfully. False otherwise.
   */
  template <typename CallerType>
  bool SetIndexSchemaPointer(common::ManagedPointer<transaction::TransactionContext> txn, index_oid_t oid,
                             const IndexSchema *schema) {
    static_assert(std::is_same_v<CallerType, storage::RecoveryManager>, "Only recovery should call this.");
    return SetClassPointer(txn, oid, schema, postgres::PgClass::REL_SCHEMA.oid_);
  }

  /** @brief Set REL_PTR for the specified pg_class column. @see PgCoreImpl::SetClassPointer */
  template <typename ClassOid, typename Ptr>
  bool SetClassPointer(common::ManagedPointer<transaction::TransactionContext> txn, ClassOid oid, const Ptr *pointer,
                       col_oid_t class_col);
};
}  // namespace noisepage::catalog
