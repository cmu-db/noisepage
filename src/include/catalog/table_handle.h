#pragma once

#include <iostream>
#include <memory>
#include <utility>
#include "catalog/catalog.h"
#include "catalog/catalog_defs.h"
#include "loggers/catalog_logger.h"
#include "storage/sql_table.h"
#include "transaction/transaction_context.h"
namespace terrier::catalog {

class Catalog;
/**
 * A namespace handle contains information about all the tables in a database.
 * It is equivalent to pg_tables;
 * it has columns:
 *      schemaname | tablename | tablespace
 */
class TableHandle {
 public:
  /**
   * A database entry represent a row in pg_tables catalog.
   */
  class TableEntry {
   public:
    /**
     * Constructs a namespace entry.
     * @param oid the namespace_oid of the underlying database
     * @param row a pointer points to the projection of the row
     * @param map a map that encodes how to access attributes of the row
     */
    TableEntry(uint32_t table_name, transaction::TransactionContext *txn, std::shared_ptr<storage::SqlTable> pg_class,
               std::shared_ptr<storage::SqlTable> pg_namespace, std::shared_ptr<storage::SqlTable> pg_tablespace)
        : table_name_(table_name),
          txn_(txn),
          pg_class_(std::move(pg_class)),
          pg_namespace_(std::move(pg_namespace)),
          pg_tablespace_(std::move(pg_tablespace)) {}

    /**
     * Get the value of an attribute
     * @param col the col_oid of the attribute
     * @return a pointer to the attribute value
     */
    byte *GetValue(const std::string &name) {
      CATALOG_LOG_TRACE("Getting the value of attribute {} ...", name);
      // get the namespace_oid and tablespace_oid of the table
      namespace_oid_t nsp_oid(0);
      tablespace_oid_t tsp_oid(0);
      std::vector<col_oid_t> cols;
      for (const auto &c : pg_class_->GetSchema().GetColumns()) {
        cols.emplace_back(c.GetOid());
      }
      auto row_pair = pg_class_->InitializerForProjectedRow(cols);
      auto read_buffer = common::AllocationUtil::AllocateAligned(row_pair.first.ProjectedRowSize());
      ptrs_.emplace_back(read_buffer);
      storage::ProjectedRow *read = row_pair.first.InitializeRow(read_buffer);
      auto tuple_iter = pg_class_->begin();
      for (; tuple_iter != pg_class_->end(); tuple_iter++) {
        pg_class_->Select(txn_, *tuple_iter, read);
        auto *addr = read->AccessForceNotNull(row_pair.second[cols[1]]);
        if (*reinterpret_cast<uint32_t *>(addr) == table_name_) {
          nsp_oid = namespace_oid_t(*reinterpret_cast<uint32_t *>(read->AccessForceNotNull(row_pair.second[cols[2]])));
          tsp_oid = tablespace_oid_t(*reinterpret_cast<uint32_t *>(read->AccessForceNotNull(row_pair.second[cols[3]])));
          break;
        }
      }
      CATALOG_LOG_TRACE("{} has namespace oid {}, tablespace_oid {}", table_name_, !nsp_oid, !tsp_oid);
      // for different attribute we need to look up different sql tables
      if (name == "schemaname") {
        std::vector<col_oid_t> cols;
        for (const auto &c : pg_namespace_->GetSchema().GetColumns()) {
          cols.emplace_back(c.GetOid());
        }
        auto row_pair = pg_namespace_->InitializerForProjectedRow(cols);
        auto read_buffer = common::AllocationUtil::AllocateAligned(row_pair.first.ProjectedRowSize());
        ptrs_.emplace_back(read_buffer);
        storage::ProjectedRow *read = row_pair.first.InitializeRow(read_buffer);
        auto tuple_iter = pg_namespace_->begin();
        for (; tuple_iter != pg_namespace_->end(); tuple_iter++) {
          pg_namespace_->Select(txn_, *tuple_iter, read);
          // TODO(yangjuns): we don't support strings at the moment
          auto *addr = read->AccessForceNotNull(row_pair.second[cols[0]]);
          if (*reinterpret_cast<uint32_t *>(addr) == !nsp_oid) {
            return read->AccessForceNotNull(row_pair.second[cols[1]]);
          }
        }
        return nullptr;
      }
      if (name == "tablename") {
        CATALOG_LOG_TRACE("retrieve information from pg_class ... ");
        std::vector<col_oid_t> cols;
        for (const auto &c : pg_class_->GetSchema().GetColumns()) {
          cols.emplace_back(c.GetOid());
        }
        auto row_pair = pg_class_->InitializerForProjectedRow(cols);
        auto read_buffer = common::AllocationUtil::AllocateAligned(row_pair.first.ProjectedRowSize());
        ptrs_.emplace_back(read_buffer);
        storage::ProjectedRow *read = row_pair.first.InitializeRow(read_buffer);
        auto tuple_iter = pg_class_->begin();
        for (; tuple_iter != pg_class_->end(); tuple_iter++) {
          pg_class_->Select(txn_, *tuple_iter, read);
          auto *addr = read->AccessForceNotNull(row_pair.second[cols[1]]);
          CATALOG_LOG_TRACE("looking at name: {}", *reinterpret_cast<uint32_t *>(addr));
          if (*reinterpret_cast<uint32_t *>(addr) == table_name_) {
            return addr;
          }
        }
        return nullptr;
      }

      if (name == "tablespace") {
        CATALOG_LOG_TRACE("looking at tablespace attribute ...");
        std::vector<col_oid_t> cols;
        std::cout << pg_tablespace_ << std::endl;
        for (const auto &c : pg_tablespace_->GetSchema().GetColumns()) {
          cols.emplace_back(c.GetOid());
        }
        CATALOG_LOG_TRACE("before allocating memory ...");
        auto row_pair = pg_tablespace_->InitializerForProjectedRow(cols);
        auto read_buffer = common::AllocationUtil::AllocateAligned(row_pair.first.ProjectedRowSize());
        ptrs_.emplace_back(read_buffer);

        storage::ProjectedRow *read = row_pair.first.InitializeRow(read_buffer);
        auto tuple_iter = pg_tablespace_->begin();
        CATALOG_LOG_TRACE("before for loop ...");
        for (; tuple_iter != pg_tablespace_->end(); tuple_iter++) {
          pg_tablespace_->Select(txn_, *tuple_iter, read);
          auto *addr = read->AccessForceNotNull(row_pair.second[cols[0]]);
          CATALOG_LOG_TRACE("looking at tablespace oid {}", *reinterpret_cast<uint32_t *>(addr));
          if (*reinterpret_cast<uint32_t *>(addr) == !tsp_oid) {
            return read->AccessForceNotNull(row_pair.second[cols[1]]);
          }
        }
        return nullptr;
      }
      TERRIER_ASSERT(false, "Non-existing attribute name");
      return nullptr;
    }

    /**
     * Return the namespace_oid of the underlying database
     * @return namespace_oid of the database
     */
    uint32_t GetTableName() { return table_name_; }

    /**
     * Destruct namespace entry. It frees the memory for storing the projected row.
     */
    ~TableEntry() {
      for (auto &addr : ptrs_) {
        if (addr != nullptr) delete[] addr;
      }
    }

   private:
    uint32_t table_name_;
    // keep track of the memory
    std::vector<byte *> ptrs_;
    transaction::TransactionContext *txn_;
    std::shared_ptr<storage::SqlTable> pg_class_;
    std::shared_ptr<storage::SqlTable> pg_namespace_;
    std::shared_ptr<storage::SqlTable> pg_tablespace_;
  };

  /**
   * Construct a namespace handle. It keeps a pointer to the pg_class sql table.
   * A know a table, you need pg_class, pg_namespace, pg_tablespace
   * @param pg_class a pointer to pg_namespace
   */
  TableHandle(Catalog *catalog, db_oid_t oid, std::shared_ptr<storage::SqlTable> pg_class,
              std::shared_ptr<storage::SqlTable> pg_namespace, std::shared_ptr<storage::SqlTable> pg_tablespace)
      : catalog_(catalog),
        db_oid_(oid),
        pg_class_(std::move(pg_class)),
        pg_namespace_(std::move(pg_namespace)),
        pg_tablespace_(std::move(pg_tablespace)) {}

  /**
   * Get a namespace entry for a given namespace_oid. It's essentially equivalent to reading a
   * row from pg_namespace. It has to be executed in a transaction context.
   *
   * @param txn the transaction that initiates the read
   * @param oid the namespace_oid of the database the transaction wants to read
   * @return a shared pointer to Namespace entry; NULL if the namespace doesn't exist in
   * the database
   */
  std::shared_ptr<TableEntry> GetTableEntry(transaction::TransactionContext *txn, const std::string &name);

 private:
  Catalog *catalog_;
  db_oid_t db_oid_;
  std::shared_ptr<storage::SqlTable> pg_class_;
  std::shared_ptr<storage::SqlTable> pg_namespace_;
  std::shared_ptr<storage::SqlTable> pg_tablespace_;
};

}  // namespace terrier::catalog
