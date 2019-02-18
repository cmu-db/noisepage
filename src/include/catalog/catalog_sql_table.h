#pragma once

// #include "storage/sql_table.h"
#include <algorithm>
#include <memory>
#include <random>
#include <string>
#include <utility>
#include <vector>
#include "common/exception.h"
#include "storage/sql_table.h"
#include "transaction/transaction_manager.h"
#include "type/value.h"
#include "type/value_factory.h"
#include "util/storage_test_util.h"
namespace terrier::catalog {

/**
 * Helper class to simplify operations on a SqlTable
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
    // delete[] read_buffer_;
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
   * Save a boolean, for insertion by EndRowAndInsert
   * @param col_num column number in the schema
   * @param value to save
   */
  void SetBooleanColInRow(int32_t col_num, bool value) {
    auto bool_value = static_cast<int8_t>(value);
    byte *col_p = insert_->AccessForceNotNull(pr_map_->at(col_oids_[col_num]));
    (*reinterpret_cast<int8_t *>(col_p)) = bool_value;
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
    size_t size = 0;
    byte *varlen = nullptr;
    byte *col_p = insert_->AccessForceNotNull(pr_map_->at(col_oids_[col_num]));
    if (st != nullptr) {
      size = strlen(st);
      varlen = common::AllocationUtil::AllocateAligned(size);
      memcpy(varlen, st, size);
    }
    *reinterpret_cast<storage::VarlenEntry *>(col_p) = {varlen, static_cast<uint32_t>(size), false};
  }

  /**
   * Convert a column number to its col_oid
   * @param col_num the column number
   * @return col_oid of the column
   */
  catalog::col_oid_t ColNumToOid(int32_t col_num) { return col_oids_[col_num]; }

  /**
   * Return the number of rows in the table.
   */
  int32_t GetNumRows() {
    int32_t num_cols = 0;
    auto layout_and_map = storage::StorageUtil::BlockLayoutFromSchema(*schema_);
    auto layout = layout_and_map.first;

    auto txn = txn_manager_.BeginTransaction();
    std::vector<storage::col_id_t> all_cols = StorageTestUtil::ProjectionListAllColumns(layout);
    storage::ProjectedColumnsInitializer col_initer(layout, all_cols, 100);
    auto *buffer = common::AllocationUtil::AllocateAligned(col_initer.ProjectedColumnsSize());
    storage::ProjectedColumns *proj_col_bufp = col_initer.Initialize(buffer);

    auto it = table_->begin();
    while (it != table_->end()) {
      table_->Scan(txn, &it, proj_col_bufp);
      num_cols += proj_col_bufp->NumTuples();
    }
    txn_manager_.Commit(txn, EmptyCallback, nullptr);
    delete[] buffer;
    delete txn;
    return num_cols;
  }

  /**
   * DO NOT USE
   * @param txn
   * @param row_num
   * @return
   */
  storage::ProjectedRow *GetRow(transaction::TransactionContext *txn, int32_t row_num) {
    TERRIER_ASSERT(row_num < GetNumRows(), "not enough rows");

    auto read_buffer = common::AllocationUtil::AllocateAligned(pri_->ProjectedRowSize());
    storage::ProjectedRow *read = pri_->InitializeRow(read_buffer);

    auto tuple_iter = table_->begin();
    for (int32_t row = 0; row < row_num; row++) {
      tuple_iter++;
    }
    // Select returns all rows, not just valid, populated ones
    table_->Select(txn, *tuple_iter, read);
    return read;
  }

  /**
   * Return a Value, from the requested col_num of the row
   *
   * @param p_row projected row
   * @param col_num
   * @return Value instance
   * Deprecate?
   */
  type::Value GetColInRow(storage::ProjectedRow *p_row, int32_t col_num) {
    storage::col_id_t storage_col_id(static_cast<uint16_t>(col_num));
    type::TypeId col_type = table_->GetSchema().GetColumn(storage_col_id).GetType();
    byte *col_p = p_row->AccessForceNotNull(ColNumToOffset(col_num));
    return CreateColValue(col_type, col_p);
  }

  /**
   * Create a value by reinterpret a byte stream
   * @param type_id the type of the value that we want to create
   * @param col_p the pointer to bytes
   * @return a value
   */
  type::Value CreateColValue(type::TypeId type_id, byte *col_p) {
    switch (type_id) {
      case type::TypeId::INTEGER:
        return type::ValueFactory::GetIntegerValue(*(reinterpret_cast<uint32_t *>(col_p)));

      default:
        throw std::runtime_error("unknown type");
    }
  }

  /**
   * Misc access.
   */
  // maybe not needed?
  std::shared_ptr<storage::SqlTable> GetSqlTable() { return table_; }

  /**
   * Return the oid of the sql table
   * @return table oid  row_p = table.FindRow(txn, search_vec);

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
   * notes: to be deprecated
   */
  storage::ProjectedRow *FindRow(transaction::TransactionContext *txn, int32_t col_num, uint32_t value) {
    // TODO(yangjuns): assert correct column type
    auto read_buffer_ = common::AllocationUtil::AllocateAligned(pri_->ProjectedRowSize());
    storage::ProjectedRow *read = pri_->InitializeRow(read_buffer_);

    auto tuple_iter = table_->begin();
    for (; tuple_iter != table_->end(); tuple_iter++) {
      table_->Select(txn, *tuple_iter, read);
      byte *col_p = read->AccessForceNotNull(ColNumToOffset(col_num));
      if (*(reinterpret_cast<uint32_t *>(col_p)) == value) {
        // TODO(pakhtar): need to free read_buffer...
        return read;
      }
    }
    delete[] read_buffer_;
    read_buffer_ = nullptr;
    return nullptr;
  }

  /**
   * Find a row in a sql table based on a value of a varchar column attribute
   * @param txn the transaction context
   * @param col_num the column number
   * @param value the string value of the column attribute we want to find
   * @return the corresponding row
   * notes: to be deprecated
   */
  storage::ProjectedRow *FindRow(transaction::TransactionContext *txn, int32_t col_num, const char *value) {
    // TODO(yangjuns): assert correct column type
    auto read_buffer_ = common::AllocationUtil::AllocateAligned(pri_->ProjectedRowSize());
    storage::ProjectedRow *read = pri_->InitializeRow(read_buffer_);

    auto tuple_iter = table_->begin();
    for (; tuple_iter != table_->end(); tuple_iter++) {
      table_->Select(txn, *tuple_iter, read);
      byte *col_p = read->AccessForceNotNull(ColNumToOffset(col_num));
      auto *entry = reinterpret_cast<storage::VarlenEntry *>(col_p);
      uint32_t size = entry->Size();
      if ((size == strlen(value)) && (memcmp(value, entry->Content(), size) == 0)) {
        // TODO(pakhtar): caller needs to free read_buffer_
        return read;
      }
    }
    delete[] read_buffer_;
    return nullptr;
  }

  /**
   * @param txn transaction
   * @param search_vec - a vector of Values to match on. This may be smaller
   *    than the number of columns. If the vector is of size > 1,
   *    all values are matched (i.e. AND for values).
   * @return on success, a vector of Values for the first matching row.
   *    only one row is returned.
   *    on failure, throws a catalog exception.
   */
  std::vector<type::Value> FindRow(transaction::TransactionContext *txn, const std::vector<type::Value> &search_vec) {
    bool row_match;
    auto layout_and_map = storage::StorageUtil::BlockLayoutFromSchema(*schema_);
    auto layout = layout_and_map.first;
    // setup parameters for a scan
    std::vector<storage::col_id_t> all_cols = StorageTestUtil::ProjectionListAllColumns(layout);
    // get one row at a time
    storage::ProjectedColumnsInitializer col_initer(layout, all_cols, 1);
    auto *buffer = common::AllocationUtil::AllocateAligned(col_initer.ProjectedColumnsSize());
    storage::ProjectedColumns *proj_col_bufp = col_initer.Initialize(buffer);

    // do a Scan
    auto it = table_->begin();
    while (it != table_->end()) {
      table_->Scan(txn, &it, proj_col_bufp);
      // interpret as a row
      storage::ProjectedColumns::RowView row_view = proj_col_bufp->InterpretAsRow(layout, 0);
      // check if this row matches
      row_match = RowFound(row_view, search_vec);
      if (row_match) {
        // convert the row into a Value vector and return
        auto ret_vec = ColToValueVec(row_view);
        delete[] buffer;
        return ret_vec;
      }
    }
    delete[] buffer;
    throw CATALOG_EXCEPTION("row not found");
  }

 private:
  static void EmptyCallback(void * /*unused*/) {}

  /**
   * @param row_view - row view from a Scan
   * @param search_vec - a vector of Values upon which to match
   * @return true if all values in the search vector match the row
   *         false otherwise
   */
  bool RowFound(storage::ProjectedColumns::RowView row_view, const std::vector<type::Value> &search_vec) {
    // assert that row_view has enough columns
    TERRIER_ASSERT(row_view.NumColumns() >= search_vec.size(), "row_view columns < search_vector");
    // assert that search vector is not empty
    TERRIER_ASSERT(!search_vec.empty(), "empty search vector");
    // iterate over the search_vec columns
    for (uint32_t index = 0; index < search_vec.size(); index++) {
      // Ignore NULL values in search_vec
      if (search_vec[index].Null()) {
        continue;
      }
      if (!ColEqualsValue(index, row_view, search_vec)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Check if a column in the row_view matches a value in the search vector
   * @param index - which column to check
   * @param row_view - a row
   * @param search_vector - values to check against
   * @return true if the column value matches
   *         false otherwise
   */
  bool ColEqualsValue(int32_t index, storage::ProjectedColumns::RowView row_view,
                      const std::vector<type::Value> &search_vec) {
    type::TypeId col_type = cols_[index].GetType();
    // TODO(pakhtar): add back updated type check
    // TERRIER_ASSERT(col_type == search_vec[index].GetType(), "schema <-> column type mismatch");
    byte *col_p = row_view.AccessForceNotNull(ColNumToOffset(index));

    switch (col_type) {
      case type::TypeId::BOOLEAN: {
        auto row_bool_val = *(reinterpret_cast<int8_t *>(col_p));
        return (row_bool_val == static_cast<int8_t>(search_vec[index].GetBooleanValue()));
      } break;

      case type::TypeId::INTEGER: {
        auto row_int_val = *(reinterpret_cast<int32_t *>(col_p));
        return (row_int_val == search_vec[index].GetIntValue());
      } break;

      case type::TypeId::VARCHAR: {
        auto *vc_entry = reinterpret_cast<storage::VarlenEntry *>(col_p);
        const char *st = search_vec[index].GetVarcharValue();
        uint32_t size = vc_entry->Size();
        if (strlen(st) != size) {
          return false;
        }
        return strncmp(st, reinterpret_cast<const char *>(vc_entry->Content()), size) == 0;
      } break;

      default:
        throw NOT_IMPLEMENTED_EXCEPTION("unsupported type in ColEqualsValue");
    }
  }

  /**
   * Convert a row into a vector of Values
   * @param row_view - row to convert
   * @return a vector of Values
   */
  std::vector<type::Value> ColToValueVec(storage::ProjectedColumns::RowView row_view) {
    std::vector<type::Value> ret_vec;
    for (int32_t i = 0; i < row_view.NumColumns(); i++) {
      type::TypeId schema_col_type = cols_[i].GetType();
      byte *col_p = row_view.AccessForceNotNull(ColNumToOffset(i));

      switch (schema_col_type) {
        case type::TypeId::BOOLEAN: {
          auto row_bool_val = *(reinterpret_cast<int8_t *>(col_p));
          ret_vec.emplace_back(type::ValueFactory::GetBooleanValue(static_cast<bool>(row_bool_val)));
        } break;

        case type::TypeId::INTEGER: {
          auto row_int_val = *(reinterpret_cast<int32_t *>(col_p));
          ret_vec.emplace_back(type::ValueFactory::GetIntegerValue(row_int_val));
        } break;

        case type::TypeId::VARCHAR: {
          auto *vc_entry = reinterpret_cast<storage::VarlenEntry *>(col_p);
          // TODO(pakhtar): unnecessary copy. Fix appropriately when
          // replaced by updated Value implementation.
          // add space for null terminator
          uint32_t size = vc_entry->Size() + 1;
          auto *ret_st = static_cast<char *>(malloc(size));
          memcpy(ret_st, vc_entry->Content(), size - 1);
          *(ret_st + size - 1) = 0;
          // TODO(pakhtar): replace with Value varchar
          ret_vec.emplace_back(type::ValueFactory::GetVarcharValue(ret_st));
          free(ret_st);
        } break;

        default:
          throw NOT_IMPLEMENTED_EXCEPTION("unsupported type in ColToValueVec");
      }
    }
    return ret_vec;
  }

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

  // byte *read_buffer_ = nullptr;
};

}  // namespace terrier::catalog
