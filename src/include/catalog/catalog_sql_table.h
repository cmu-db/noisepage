#pragma once

// #include "storage/sql_table.h"
#include <algorithm>
#include <memory>
#include <random>
#include <string>
#include <utility>
#include <vector>
#include "common/exception.h"
#include "loggers/catalog_logger.h"
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
    delete layout_and_map_;
    delete col_initer_;
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
    table_ = std::make_shared<storage::SqlTable>(&block_store_, *schema_, table_oid_);

    for (const auto &c : cols_) {
      col_oids_.emplace_back(c.GetOid());
    }

    // save information needed for (later) reading and writing
    // TODO(pakhtar): review to see if still needed, since we are using
    // projected columns
    auto row_pair = table_->InitializerForProjectedRow(col_oids_);
    pri_ = new storage::ProjectedRowInitializer(std::get<0>(row_pair));
    pr_map_ = new storage::ProjectionMap(std::get<1>(row_pair));
  }

  /**
   * Save a value, for insertion by EndRowAndInsert
   * @param proj_row projected row
   * @param col_num column number in the schema
   * @param value to save
   */
  void SetColInRow(storage::ProjectedRow *proj_row, int32_t col_num, const type::Value &value) {
    auto offset = pr_map_->at(col_oids_[col_num]);
    if (value.Null()) {
      proj_row->SetNull(offset);
      return;
    }

    // value must be non-null onwards
    switch (value.Type()) {
      case type::TypeId::BOOLEAN: {
        byte *col_p = proj_row->AccessForceNotNull(offset);
        (*reinterpret_cast<int8_t *>(col_p)) = static_cast<int8_t>(value.GetBooleanValue());
        break;
      }
      case type::TypeId::INTEGER: {
        byte *col_p = proj_row->AccessForceNotNull(offset);
        (*reinterpret_cast<int32_t *>(col_p)) = value.GetIntValue();
        break;
      }
      case type::TypeId::BIGINT: {
        byte *col_p = proj_row->AccessForceNotNull(offset);
        (*reinterpret_cast<int64_t *>(col_p)) = value.GetBigIntValue();
        break;
      }
      case type::TypeId::VARCHAR: {
        size_t size = 0;
        byte *varlen = nullptr;
        byte *col_p = proj_row->AccessForceNotNull(offset);
        size = strlen(value.GetVarcharValue());
        if (size > storage::VarlenEntry::InlineThreshold()) {
          // not inline, allocate storage
          varlen = common::AllocationUtil::AllocateAligned(size);
          memcpy(varlen, value.GetVarcharValue(), size);
          *reinterpret_cast<storage::VarlenEntry *>(col_p) =
              storage::VarlenEntry::Create(varlen, static_cast<uint32_t>(size), true);
        } else {
          // small enought to be stored inline
          auto byte_p = reinterpret_cast<const byte *>(value.GetVarcharValue());
          *reinterpret_cast<storage::VarlenEntry *>(col_p) =
              storage::VarlenEntry::CreateInline(byte_p, static_cast<uint32_t>(size));
        }
        break;
      }
        // TODO(yangjuns): support other types
      default:
        break;
    }
  }

  /**
   * Convert a column number to its col_oid
   * @param col_num the column number
   * @return col_oid of the column
   */
  catalog::col_oid_t ColNumToOid(int32_t col_num) { return col_oids_[col_num]; }

  /**
   * Return the number of rows in the table.
   * @param txn transaction
   */
  int32_t GetNumRows(transaction::TransactionContext *txn) {
    int32_t num_cols = 0;
    auto layout = GetLayout();
    std::vector<storage::col_id_t> all_cols = StorageTestUtil::ProjectionListAllColumns(layout);
    storage::ProjectedColumnsInitializer col_initer(layout, all_cols, 100);
    auto *buffer = common::AllocationUtil::AllocateAligned(col_initer.ProjectedColumnsSize());
    storage::ProjectedColumns *proj_col_bufp = col_initer.Initialize(buffer);

    auto it = table_->begin();
    while (it != table_->end()) {
      table_->Scan(txn, &it, proj_col_bufp);
      num_cols += proj_col_bufp->NumTuples();
    }
    delete[] buffer;
    return num_cols;
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
    // fix
    return CreateColValue(col_type, col_p);
  }

  /**
   * Misc access.
   */
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
    if (static_cast<size_t>(col_num) >= pr_map_->size()) {
      throw CATALOG_EXCEPTION("col_num > size");
    }
    return pr_map_->at(col_oids_[col_num]);
  }

  /**
   * Insert a row. (This function is noticeably slower than SetIntColInRow ... due to Value type copies)
   * @param txn
   * @param row - vector of values to insert
   */
  void InsertRow(transaction::TransactionContext *txn, const std::vector<type::Value> &row) {
    // get buffer for insertion and use as a row
    auto insert_buffer = common::AllocationUtil::AllocateAligned(pri_->ProjectedRowSize());
    auto proj_row = pri_->InitializeRow(insert_buffer);

    for (size_t i = 0; i < row.size(); i++) {
      SqlTableRW::SetColInRow(proj_row, static_cast<int32_t>(i), row[i]);
    }
    table_->Insert(txn, *proj_row);

    delete[] insert_buffer;
  }

  /**
   * @param txn transaction
   * @param search_vec - a vector of Values to match on. This may be smaller
   *    than the number of columns. If the vector is of size > 1,
   *    all values are matched (i.e. AND for values).
   * @return on success, a vector of Values for the first matching row.
   *    only one row is returned.
   *    on failure, returns an empty vector;
   */
  std::vector<type::Value> FindRow(transaction::TransactionContext *txn, const std::vector<type::Value> &search_vec) {
    bool row_match;

    auto layout = GetLayout();
    // setup parameters for a scan
    std::vector<storage::col_id_t> all_cols = StorageTestUtil::ProjectionListAllColumns(layout);
    // get one row at a time
    if (col_initer_ == nullptr) {
      col_initer_ = new storage::ProjectedColumnsInitializer(layout, all_cols, 1);
    }
    auto *buffer = common::AllocationUtil::AllocateAligned(col_initer_->ProjectedColumnsSize());
    storage::ProjectedColumns *proj_col_bufp = col_initer_->Initialize(buffer);

    // do a Scan
    auto it = table_->begin();
    while (it != table_->end()) {
      table_->Scan(txn, &it, proj_col_bufp);
      if (proj_col_bufp->NumTuples() == 0) {
        continue;
      }
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
    // return an empty vector
    return std::vector<type::Value>();
  }

  /**
   * Find a row and return a projected column pointer
   *
   * For entry deletion, we need access to the tuple slot via the projected column api, in order to delete.
   */
  storage::ProjectedColumns *FindRowProjCol(transaction::TransactionContext *txn,
                                            const std::vector<type::Value> &search_vec) {
    bool row_match;

    auto layout = GetLayout();
    // setup parameters for a scan
    std::vector<storage::col_id_t> all_cols = StorageTestUtil::ProjectionListAllColumns(layout);
    // get one row at a time
    // storage::ProjectedColumnsInitializer col_initer(layout, all_cols, 1);
    if (col_initer_ == nullptr) {
      col_initer_ = new storage::ProjectedColumnsInitializer(layout, all_cols, 1);
    }
    auto *buffer = common::AllocationUtil::AllocateAligned(col_initer_->ProjectedColumnsSize());
    storage::ProjectedColumns *proj_col_bufp = col_initer_->Initialize(buffer);

    // do a Scan
    auto it = table_->begin();
    while (it != table_->end()) {
      table_->Scan(txn, &it, proj_col_bufp);
      if (proj_col_bufp->NumTuples() == 0) {
        continue;
      }
      // interpret as a row
      storage::ProjectedColumns::RowView row_view = proj_col_bufp->InterpretAsRow(layout, 0);
      // check if this row matches
      row_match = RowFound(row_view, search_vec);
      if (row_match) {
        // buffer ownership rules?
        return proj_col_bufp;
      }
    }
    delete[] buffer;
    // delete col_initer_;
    return nullptr;
  }

  /**
   * Get the layout of the SQL table.
   */
  storage::BlockLayout GetLayout() {
    if (layout_and_map_ == nullptr) {
      layout_and_map_ = new std::pair<storage::BlockLayout, storage::ColumnMap>(
          storage::StorageUtil::BlockLayoutFromSchema(*schema_));
    }
    return layout_and_map_->first;
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
      byte *col_p = row_view.AccessWithNullCheck(ColNumToOffset(i));
      if (col_p == nullptr) {
        ret_vec.emplace_back(type::ValueFactory::GetNullValue(schema_col_type));
        continue;
      }

      switch (schema_col_type) {
        case type::TypeId::BOOLEAN: {
          auto row_bool_val = *(reinterpret_cast<int8_t *>(col_p));
          ret_vec.emplace_back(type::ValueFactory::GetBooleanValue(static_cast<bool>(row_bool_val)));
          break;
        }
        case type::TypeId::SMALLINT: {
          auto row_int_val = *(reinterpret_cast<int16_t *>(col_p));
          ret_vec.emplace_back(type::ValueFactory::GetSmallIntValue(row_int_val));
          break;
        }
        case type::TypeId::INTEGER: {
          auto row_int_val = *(reinterpret_cast<int32_t *>(col_p));
          ret_vec.emplace_back(type::ValueFactory::GetIntegerValue(row_int_val));
          break;
        }
        case type::TypeId::BIGINT: {
          auto row_int_val = *(reinterpret_cast<int64_t *>(col_p));
          ret_vec.emplace_back(type::ValueFactory::GetBigIntValue(row_int_val));
          break;
        }
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
          break;
        }

        default:
          throw NOT_IMPLEMENTED_EXCEPTION("unsupported type in ColToValueVec");
      }
    }
    return ret_vec;
  }

  /* -----------------
   *Debugging support
   * -----------------
   */

  /**
   * @param txn transaction
   */
  void Dump(transaction::TransactionContext *txn) {
    auto layout = GetLayout();
    // setup parameters for a scan
    std::vector<storage::col_id_t> all_cols = StorageTestUtil::ProjectionListAllColumns(layout);
    // get one row at a time
    if (col_initer_ == nullptr) {
      col_initer_ = new storage::ProjectedColumnsInitializer(layout, all_cols, 1);
    }
    auto *buffer = common::AllocationUtil::AllocateAligned(col_initer_->ProjectedColumnsSize());
    storage::ProjectedColumns *proj_col_bufp = col_initer_->Initialize(buffer);
    int32_t row_num = 0;
    // do a Scan
    auto it = table_->begin();
    while (it != table_->end()) {
      table_->Scan(txn, &it, proj_col_bufp);
      if (proj_col_bufp->NumTuples() == 0) {
        continue;
      }
      // interpret as a row
      storage::ProjectedColumns::RowView row_view = proj_col_bufp->InterpretAsRow(layout, 0);
      // convert the row into a Value vector and return
      // auto ret_vec = ColToValueVec(row_view);
      CATALOG_LOG_DEBUG("");
      CATALOG_LOG_DEBUG("row {}", row_num);
      for (int32_t i = 0; i < row_view.NumColumns(); i++) {
        type::TypeId schema_col_type = cols_[i].GetType();
        byte *col_p = row_view.AccessWithNullCheck(ColNumToOffset(i));
        if (col_p == nullptr) {
          CATALOG_LOG_DEBUG("col {}: NULL", i);
          continue;
        }

        switch (schema_col_type) {
          case type::TypeId::BOOLEAN: {
            auto row_bool_val = *(reinterpret_cast<int8_t *>(col_p));
            CATALOG_LOG_DEBUG("col {}: {}", i, row_bool_val);
            break;
          }

          case type::TypeId::SMALLINT: {
            auto row_int_val = *(reinterpret_cast<int16_t *>(col_p));
            CATALOG_LOG_DEBUG("col {}: {}", i, row_int_val);
            break;
          }

          case type::TypeId::INTEGER: {
            auto row_int_val = *(reinterpret_cast<int32_t *>(col_p));
            CATALOG_LOG_DEBUG("col {}: {}", i, row_int_val);
            break;
          }

          case type::TypeId::BIGINT: {
            auto row_int_val = *(reinterpret_cast<int64_t *>(col_p));
            CATALOG_LOG_DEBUG("col {}: {}", i, row_int_val);
            break;
          }

          case type::TypeId::VARCHAR: {
            auto *vc_entry = reinterpret_cast<storage::VarlenEntry *>(col_p);
            // TODO(pakhtar): unnecessary copy. Fix appropriately when
            // replaced by updated Value implementation.
            // add space for null terminator
            uint32_t size = vc_entry->Size() + 1;
            auto *st = new char[size + sizeof(uint32_t)];
            std::memcpy(st, vc_entry->Content(), size - 1);
            *(st + size - 1) = 0;
            CATALOG_LOG_DEBUG("col {}: {}", i, st);
            delete[] st;
            break;
          }

          default:
            throw NOT_IMPLEMENTED_EXCEPTION("unsupported type in Dump");
        }
      }
      row_num++;
    }
    delete[] buffer;
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
   * Create a value by reinterpret a byte stream
   * @param type_id the type of the value that we want to create
   * @param col_p the pointer to bytes
   * @return a value
   */
  type::Value CreateColValue(type::TypeId type_id, byte *col_p) {
    switch (type_id) {
      case type::TypeId::INTEGER:
        return type::ValueFactory::GetIntegerValue(*(reinterpret_cast<uint32_t *>(col_p)));
      case type::TypeId::VARCHAR: {
        auto *vc_entry = reinterpret_cast<storage::VarlenEntry *>(col_p);
        // TODO(pakhtar): unnecessary copy. Fix appropriately when
        // replaced by updated Value implementation.
        // add space for null terminator
        uint32_t size = vc_entry->Size() + 1;
        auto *ret_st = static_cast<char *>(malloc(size));
        memcpy(ret_st, vc_entry->Content(), size - 1);
        *(ret_st + size - 1) = 0;
        // TODO(pakhtar): replace w
        auto result = type::ValueFactory::GetVarcharValue(ret_st);
        free(ret_st);
        return result;
      }
      default:
        throw std::runtime_error("unknown type");
    }
  }

  /**
   * Check if a column in the row_view matches a value in the search vector.
   * @param index - which column to check.
   * @param row_view - a row
   * @param search_vector - values to check against
   * @return true if the column value matches
   *         false otherwise
   */
  bool ColEqualsValue(int32_t index, storage::ProjectedColumns::RowView row_view,
                      const std::vector<type::Value> &search_vec) {
    type::TypeId col_type = cols_[index].GetType();
    if (col_type != search_vec[index].Type()) {
      TERRIER_ASSERT(col_type == search_vec[index].Type(), "schema <-> column type mismatch");
    }
    TERRIER_ASSERT(search_vec[index].Null() == false, "search_vec[index] is null");
    byte *col_p = row_view.AccessWithNullCheck(ColNumToOffset(index));
    if (col_p == nullptr) {
      // since search_vec[index] cannot be null
      return false;
    }

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

  storage::RecordBufferSegmentPool buffer_pool_{100, 100};
  transaction::TransactionManager txn_manager_ = {&buffer_pool_, true, LOGGING_DISABLED};

  storage::BlockStore block_store_{100, 100};
  catalog::table_oid_t table_oid_;
  std::shared_ptr<storage::SqlTable> table_ = nullptr;

  catalog::Schema *schema_ = nullptr;
  std::vector<catalog::Schema::Column> cols_;
  std::vector<catalog::col_oid_t> col_oids_;

  storage::ProjectedRowInitializer *pri_ = nullptr;
  storage::ProjectionMap *pr_map_ = nullptr;

  // cache some items, for efficiency
  std::pair<storage::BlockLayout, storage::ColumnMap> *layout_and_map_ = nullptr;
  storage::ProjectedColumnsInitializer *col_initer_ = nullptr;
};

}  // namespace terrier::catalog
