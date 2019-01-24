#include "storage/sql_table.h"
#include <algorithm>
#include <random>
#include <string>
#include <utility>
#include <vector>
#include "transaction/transaction_manager.h"
#include "util/test_harness.h"
#include "util/transaction_test_util.h"

namespace terrier::catalog {

  /**
 * Help class to simplify operations on a SqlTable
 */
class SqlTableRW {
 public:
  explicit SqlTableRW(catalog::table_oid_t table_oid) : table_oid_(table_oid) {}
  ~SqlTableRW() {
    delete pri_;
    delete pr_map_;
    delete schema_;
    //delete table_;
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
    //table_ = new storage::SqlTable(&block_store_, *schema_, table_oid_);
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
      txn_manager_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
      delete txn;
    }
    return storage::TupleSlot(slot.GetBlock(), slot.GetOffset());
  }

  /**
   * Read an integer from a row
   * @param col_num column number in the schema
   * @param slot - tuple to read from
   * @return integer value
   */
  uint32_t GetIntColInRow(int32_t col_num, storage::TupleSlot slot) {
    auto txn = txn_manager_.BeginTransaction();
    auto read_buffer = common::AllocationUtil::AllocateAligned(pri_->ProjectedRowSize());
    storage::ProjectedRow *read = pri_->InitializeRow(read_buffer);
    table_->Select(txn, slot, read);
    byte *col_p = read->AccessForceNotNull(pr_map_->at(col_oids_[col_num]));
    txn_manager_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
    auto ret_val = *(reinterpret_cast<uint32_t *>(col_p));

    delete txn;
    delete[] read_buffer;
    return ret_val;
  }

  /**
   * Save an integer, for insertion by EndRowAndInsert
   * @param col_num column number in the schema
   * @param value to save
   */
  void SetIntColInRow(int32_t col_num, int32_t value) {
    byte *col_p = insert_->AccessForceNotNull(pr_map_->at(col_oids_[col_num]));
    (*reinterpret_cast<uint32_t *>(col_p)) = value;
  }

  /**
   * Read a string from a row
   * @param col_num column number in the schema
   * @param slot - tuple to read from
   * @return malloc'ed C string (with null terminator). Caller must
   *   free.
   */
  char *GetVarcharColInRow(int32_t col_num, storage::TupleSlot slot) {
    auto txn = txn_manager_.BeginTransaction();
    auto read_buffer = common::AllocationUtil::AllocateAligned(pri_->ProjectedRowSize());
    storage::ProjectedRow *read = pri_->InitializeRow(read_buffer);
    table_->Select(txn, slot, read);
    byte *col_p = read->AccessForceNotNull(pr_map_->at(col_oids_[col_num]));

    auto *entry = reinterpret_cast<storage::VarlenEntry *>(col_p);
    // stored string has no null terminator, add space for it
    uint32_t size = entry->Size() + 1;
    // allocate return string
    auto *ret_st = static_cast<char *>(malloc(size));
    memcpy(ret_st, entry->Content(), size);
    // add the null terminator
    *(ret_st + size - 1) = 0;
    txn_manager_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
    delete txn;
    delete[] read_buffer;
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

  catalog::col_oid_t ColNumToOid(int32_t col_num) {
    return col_oids_[col_num];
  }

  /**
   * Misc access.
   */
  // maybe not needed?
  std::shared_ptr<storage::SqlTable> GetSqlTable() {
    return table_;
  }

  // possibly just return table_oid_?
  catalog::table_oid_t Oid() { return table_->Oid(); }

  // shared ptr?
  storage::ProjectionMap *GetPRMap() {
    return pr_map_;
  }

  uint16_t ColNumToOffset(int32_t col_num) {
    // TODO(pakhtar): add safety checks
    return pr_map_->at(col_oids_[col_num]);
  }

  /**
   * handle support
   */
  storage::ProjectedRow *FindRow(transaction::TransactionContext *txn,
                                 int32_t col_num,
                                 uint32_t value) {
    // TODO: assert correct column type
    auto read_buffer = common::AllocationUtil::AllocateAligned(pri_->ProjectedRowSize());
    storage::ProjectedRow *read = pri_->InitializeRow(read_buffer);

    auto tuple_iter = table_->begin();
    for (; tuple_iter != table_->end(); tuple_iter++) {
      table_->Select(txn, *tuple_iter, read);
      byte *col_p = read->AccessForceNotNull(pr_map_->at(col_oids_[col_num]));
      if (*(reinterpret_cast<uint32_t *>(col_p)) == value) {
        return read;
      }
    }
    delete[] read_buffer;
    return nullptr;
  }

 private:
  storage::RecordBufferSegmentPool buffer_pool_{100, 100};
  transaction::TransactionManager txn_manager_ = {&buffer_pool_, true, LOGGING_DISABLED};

  storage::BlockStore block_store_{100, 100};
  catalog::table_oid_t table_oid_;
  //storage::SqlTable *table_ = nullptr;
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

