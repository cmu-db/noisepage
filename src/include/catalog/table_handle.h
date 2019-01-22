#pragma once

#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "catalog/catalog.h"
#include "catalog/catalog_defs.h"
#include "loggers/catalog_logger.h"
#include "storage/sql_table.h"
#include "transaction/transaction_context.h"
namespace terrier::catalog {

class Catalog;
/**
 * A tablespace handle contains information about all the tables in a database.
 * It is equivalent to pg_tables in postgres, which is a view.
 * it has columns:
 *      schemaname | tablename | tablespace
 */
class TableHandle {
 public:
  /**
   * A table entry represent a row in pg_tables catalog.
   */
  class TableEntry {
   public:
    /**
     * Constructs a table entry in pg_tables.
     *
     * TODO(yangjuns): we need to change uint32_t to strings once we take varlen
     *
     * @param table_name the name of the table.
     * @param txn the transaction which wants the entry
     * @param pg_class a pointer to the pg_class catalog
     * @param pg_namespace a pointer to pg_namespace
     * @param pg_tablespace a pointer to tablespace
     */
    TableEntry(uint32_t table_name, transaction::TransactionContext *txn, std::shared_ptr<storage::SqlTable> pg_class,
               std::shared_ptr<storage::SqlTable> pg_namespace, std::shared_ptr<storage::SqlTable> pg_tablespace)
        : table_name_(table_name),
          txn_(txn),
          pg_class_(std::move(pg_class)),
          pg_namespace_(std::move(pg_namespace)),
          pg_tablespace_(std::move(pg_tablespace)) {}

    /**
     * Get the value of an attribute.
     * @param name the name of the attribute.
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
     * Destruct namespace entry. It frees the memory for storing the projected row.
     */
    ~TableEntry() {
      for (auto &addr : ptrs_) delete[] addr;
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
   * Construct a table handle. It keeps pointers to the pg_class, pg_namespace, pg_tablespace sql tables.
   * It uses use these three tables to provide the view of pg_tables.
   * @param pg_class a pointer to pg_class
   * @param pg_namespace a pointer to pg_namespace
   * @param pg_tablespace a pointer to pg_tablespace
   */
  TableHandle(std::shared_ptr<storage::SqlTable> pg_class, std::shared_ptr<storage::SqlTable> pg_namespace,
              std::shared_ptr<storage::SqlTable> pg_tablespace)
      : pg_class_(std::move(pg_class)),
        pg_namespace_(std::move(pg_namespace)),
        pg_tablespace_(std::move(pg_tablespace)) {}

  /**
   * Get a table entry for the given table name. It's essentially equivalent to reading a
   * row from pg_tables. It has to be executed in a transaction context.
   *
   * @param txn the transaction that initiates the read
   * @param name the name of the table
   * @return a shared pointer to table entry; NULL if the namespace doesn't exist in
   * the database
   */
  std::shared_ptr<TableEntry> GetTableEntry(transaction::TransactionContext *txn, const std::string &name);

 private:
  std::shared_ptr<storage::SqlTable> pg_class_;
  std::shared_ptr<storage::SqlTable> pg_namespace_;
  std::shared_ptr<storage::SqlTable> pg_tablespace_;
};

}  // namespace terrier::catalog
