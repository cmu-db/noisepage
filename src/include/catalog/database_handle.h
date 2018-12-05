#pragma once

#include <loggers/catalog_logger.h>
#include <memory>
#include <utility>
#include <vector>
#include "storage/sql_table.h"
#include "transaction/transaction_context.h"
namespace terrier::catalog {

class DatabaseHandle {
 public:
  class DatabaseEntry {
   public:
    DatabaseEntry(transaction::TransactionContext *txn, oid_t oid, storage::ProjectedRow *row,
                  storage::ProjectionMap map)
        : txn_(txn), oid_(oid), row_(row), map_(std::move(map)) {}

    byte *GetValue(col_oid_t col) { return row_->AccessWithNullCheck(map_[col]); }
    ~DatabaseEntry() {
      if (row_) delete[] reinterpret_cast<byte *>(row_);
    }

   private:
    transaction::TransactionContext *txn_;
    oid_t oid_;
    storage::ProjectedRow *row_;
    storage::ProjectionMap map_;
  };

  DatabaseHandle(oid_t oid, std::shared_ptr<storage::SqlTable> pg_database) : oid_(oid), pg_database_(pg_database) {}
  ~DatabaseHandle() = default;

  // std::vector<NameSpaceHandle> GetNameSpaceHandles(oid_t ns_oid);

  std::shared_ptr<DatabaseEntry> GetDatabaseEntry(transaction::TransactionContext *txn, oid_t oid) {
    // Each database handle can only see entry with the same oid
    if (oid_ != oid) return nullptr;

    // TODO(yangjun): we can cache this
    // create columns
    std::vector<col_oid_t> cols;
    for (const auto &c : pg_database_->GetSchema().GetColumns()) {
      cols.emplace_back(c.GetOid());
    }
    auto row_pair = pg_database_->InitializerForProjectedRow(cols);
    CATALOG_LOG_INFO("hello world");
    CATALOG_LOG_DEBUG("hello world");
    auto read_buffer = common::AllocationUtil::AllocateAligned(row_pair.first.ProjectedRowSize());
    storage::ProjectedRow *read = row_pair.first.InitializeRow(read_buffer);
    CATALOG_LOG_INFO("SIZE of Projected Row: {}", read->Size());
    CATALOG_LOG_INFO("hello world");
    // try to get the row
    auto tuple_iter = pg_database_->begin();
    int count = 0;
    for (; tuple_iter != pg_database_->end(); tuple_iter++) {
      // CATALOG_LOG_INFO("read: {} ", read);
      printf("count %d\n", count);
      printf("offset %d, block  %p\n", tuple_iter->GetOffset(), tuple_iter->GetBlock());
      CATALOG_LOG_INFO("BEFORE select");
      pg_database_->Select(txn, *tuple_iter, read);
      CATALOG_LOG_INFO("AFTER SELECT");
      count++;
      byte *addr = read->AccessForceNotNull(row_pair.second[cols[0]]);
      printf("expected: %d, real oid %d\n", !(oid_), !(*reinterpret_cast<oid_t *>(addr)));
      CATALOG_LOG_INFO("FLUSH");
      if ((*reinterpret_cast<oid_t *>(read->AccessForceNotNull(row_pair.second[cols[0]]))) == oid_) {
        return std::make_shared<DatabaseEntry>(txn, oid_, read, row_pair.second);
      }
    }
    delete[] read_buffer;
    return nullptr;
  }

 private:
  oid_t oid_;
  std::shared_ptr<storage::SqlTable> pg_database_;
};

}  // namespace terrier::catalog
