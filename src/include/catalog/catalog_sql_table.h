#pragma once

#include <algorithm>
#include <memory>
#include <random>
#include <string>
#include <utility>
#include <vector>
#include "storage/sql_table.h"
#include "transaction/transaction_manager.h"
namespace terrier::catalog {

/**
 * Help class to simplify operations on a SqlTable
 */
class SqlTableRW {
 public:
  /**
   * Constructor
   * @param table_oid the table oid of the underlying sql table
   */
  explicit SqlTableRW(catalog::table_oid_t table_oid) : table_oid_(table_oid) {}
  ~SqlTableRW() {
    delete pri_;
    delete pr_map_;
    delete schema_;
    // delete table_;
  }

  /**
   * Append a column definition to the internal list. The list will be
   * used when creating the SqlTable.
   * @param name of the column
   * @param type of the column
   * @param nullable
   * @param oid for the column
   */
  void DefineColumn(std::string name, type::TypeId type, bool nullable, catalog::col_oid_t oid) {
    cols_.emplace_back(name, type, nullable, oid);
  }

  /**
   * Create the SQL table.
   */
  void Create() {
    schema_ = new catalog::Schema(cols_);
    // table_ = new storage::SqlTable(&block_store_, *schema_, table_oid_);
    table_ = std::make_shared<storage::SqlTable>(&block_store_, *schema_, table_oid_);

    for (const auto &c : cols_) {
      col_oids_.emplace_back(c.GetOid());
    }

    // save information needed for (later) reading and writing
    auto row_pair = table_->InitializerForProjectedRow(col_oids_);
    pri_ = new storage::ProjectedRowInitializer(std::get<0>(row_pair));
    pr_map_ = new storage::ProjectionMap(std::get<1>(row_pair));
  }

  /**
   * First step in writing a row.
   */
  void StartRow() {
    insert_buffer_ = common::AllocationUtil::AllocateAligned(pri_->ProjectedRowSize());
    insert_ = pri_->InitializeRow(insert_buffer_);
  }

  /**
   * Insert the row into the table
   * @param txn_in - if non-null, use the supplied transaction.
   *    If nullptr, generate a transaction.
   */
  storage::TupleSlot EndRowAndInsert(transaction::TransactionContext *txn_in) {
    bool local_txn = false;
    transaction::TransactionContext *txn = nullptr;

    if (txn_in == nullptr) {
      local_txn = true;
      txn = txn_manager_.BeginTransaction();
    } else {
      txn = txn_in;
    }

    auto slot = table_->Insert(txn, *insert_);
    insert_ = nullptr;

    delete[] insert_buffer_;
    if (local_txn) {
      txn_manager_.Commit(txn, EmptyCallback, nullptr);
      delete txn;
    }
    return storage::TupleSlot(slot.GetBlock(), slot.GetOffset());
  }

  /**
   * Read an integer from a (supplied) row. This method is used by the handle
   * and entry classes.
   * @param col_num - column number in the schema
   * @param row - to read from
   * @return integer value
   */
  int32_t GetIntColInRow(int32_t col_num, storage::ProjectedRow *row) {
    byte *col_p = row->AccessForceNotNull(ColNumToOffset(col_num));
    return *(reinterpret_cast<uint32_t *>(col_p));
  }

  /**
   * Save an integer, for insertion by EndRowAndInsert
   * @param col_num column number in the schema
   * @param value to save
   */
  void SetIntColInRow(int32_t col_num, int32_t value) {
    byte *col_p = insert_->AccessForceNotNull(pr_map_->at(col_oids_[col_num]));
    (*reinterpret_cast<int32_t *>(col_p)) = value;
  }

  /**
   * Read a big integer from a (supplied) row. This method is used by the handle
   * and entry classes.
   * @param col_num - column number in the schema
   * @param row - to read from
   * @return integer value
   */
  int64_t GetBigintColInRow(int32_t col_num, storage::ProjectedRow *row) {
    byte *col_p = row->AccessForceNotNull(ColNumToOffset(col_num));
    return *(reinterpret_cast<int64_t *>(col_p));
  }

  /**
   * Save a big integer, for insertion by EndRowAndInsert
   * @param col_num column number in the schema
   * @param value to save
   */
  void SetBigintColInRow(int32_t col_num, int64_t value) {
    byte *col_p = insert_->AccessForceNotNull(pr_map_->at(col_oids_[col_num]));
    (*reinterpret_cast<int64_t *>(col_p)) = value;
  }

  /**
   * Read a string from a (supplied) row. This method is used by the handle
   * and entry classes.
   * @param col_num column number in the schema
   * @param row - to read from
   * @return malloc'ed C string (with null terminator). Caller must
   *   free.
   */
  char *GetVarcharColInRow(int32_t col_num, storage::ProjectedRow *row) {
    byte *col_p = row->AccessForceNotNull(ColNumToOffset(col_num));
    auto *entry = reinterpret_cast<storage::VarlenEntry *>(col_p);
    // stored string has no null terminator, add space for it
    uint32_t size = entry->Size() + 1;
    // allocate return string
    auto *ret_st = static_cast<char *>(malloc(size));
    memcpy(ret_st, entry->Content(), size - 1);
    // add the null terminator
    *(ret_st + size - 1) = 0;
    return ret_st;
  }

  /**
   * Save a string, for insertion by EndRowAndInsert
   * @param col_num column number in the schema
   * @param st C string to save.
   */
  void SetVarcharColInRow(int32_t col_num, const char *st) {
    byte *col_p = insert_->AccessForceNotNull(pr_map_->at(col_oids_[col_num]));
    // string size, without null terminator
    size_t size = strlen(st);
    byte *varlen = common::AllocationUtil::AllocateAligned(size);
    memcpy(varlen, st, size);
    *reinterpret_cast<storage::VarlenEntry *>(col_p) = {varlen, static_cast<uint32_t>(size), false};
  }

  /**
   * Convert a column number to its col_oid
   * @param col_num the column number
   * @return col_oid of the column
   */
  catalog::col_oid_t ColNumToOid(int32_t col_num) { return col_oids_[col_num]; }

  /**
   * Misc access.
   */
  // maybe not needed?
  std::shared_ptr<storage::SqlTable> GetSqlTable() { return table_; }

  /**
   * Return the oid of the sql table
   * @return table oid
   */
  catalog::table_oid_t Oid() { return table_->Oid(); }

  /**
   * Return a pointer to the projection map
   * @return pointer to the projection map
   */
  // shared ptr?
  storage::ProjectionMap *GetPRMap() { return pr_map_; }

  /**
   * Get the offset of the column in the projection map
   * @param col_num the column number
   * @return the offset
   */
  uint16_t ColNumToOffset(int32_t col_num) {
    // TODO(pakhtar): add safety checks
    return pr_map_->at(col_oids_[col_num]);
  }

  /**
   * Find a row in a sql table based on a value of an integer column attribute
   * @param txn the transaction context
   * @param col_num the column number
   * @param value the integer value of the column attribute we want to find
   * @return the corresponding row
   */
  storage::ProjectedRow *FindRow(transaction::TransactionContext *txn, int32_t col_num, uint32_t value) {
    // TODO(yangjuns): assert correct column type
    auto read_buffer = common::AllocationUtil::AllocateAligned(pri_->ProjectedRowSize());
    storage::ProjectedRow *read = pri_->InitializeRow(read_buffer);

    auto tuple_iter = table_->begin();
    for (; tuple_iter != table_->end(); tuple_iter++) {
      table_->Select(txn, *tuple_iter, read);
      byte *col_p = read->AccessForceNotNull(ColNumToOffset(col_num));
      if (*(reinterpret_cast<uint32_t *>(col_p)) == value) {
        return read;
      }
    }
    delete[] read_buffer;
    return nullptr;
  }

  /**
   * Find a row in a sql table based on a value of a varchar column attribute
   * @param txn the transaction context
   * @param col_num the column number
   * @param value the string value of the column attribute we want to find
   * @return the corresponding row
   */
  storage::ProjectedRow *FindRow(transaction::TransactionContext *txn, int32_t col_num, const char *value) {
    // TODO(yangjuns): assert correct column type
    auto read_buffer = common::AllocationUtil::AllocateAligned(pri_->ProjectedRowSize());
    storage::ProjectedRow *read = pri_->InitializeRow(read_buffer);

    auto tuple_iter = table_->begin();
    for (; tuple_iter != table_->end(); tuple_iter++) {
      table_->Select(txn, *tuple_iter, read);
      byte *col_p = read->AccessForceNotNull(ColNumToOffset(col_num));
      auto *entry = reinterpret_cast<storage::VarlenEntry *>(col_p);
      uint32_t size = entry->Size();
      if ((size == strlen(value)) && (memcmp(value, entry->Content(), size) == 0)) {
        return read;
      }
    }
    delete[] read_buffer;
    return nullptr;
  }

 private:
  static void EmptyCallback(void * /*unused*/) {}

  storage::RecordBufferSegmentPool buffer_pool_{100, 100};
  transaction::TransactionManager txn_manager_ = {&buffer_pool_, true, LOGGING_DISABLED};

  storage::BlockStore block_store_{100, 100};
  catalog::table_oid_t table_oid_;
  // storage::SqlTable *table_ = nullptr;
  std::shared_ptr<storage::SqlTable> table_ = nullptr;

  catalog::Schema *schema_ = nullptr;
  std::vector<catalog::Schema::Column> cols_;
  std::vector<catalog::col_oid_t> col_oids_;

  storage::ProjectedRowInitializer *pri_ = nullptr;
  storage::ProjectionMap *pr_map_ = nullptr;

  byte *insert_buffer_ = nullptr;
  storage::ProjectedRow *insert_ = nullptr;
};

}  // namespace terrier::catalog
