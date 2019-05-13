#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "type/transient_value.h"

namespace terrier::catalog {

/**
 * A catalog entry that is indexed by type Oid. It should *NOT* be used by views(e.g. pg_tables).
 * View catalogs require special treatments. Most catalog entries are essentially the same as this one.
 * However, we enforce differnet names for type safety.
 *
 * @tparam Oid this catalog is indexed by type Oid.
 */
template <typename Oid>
class CatalogEntry {
 public:
  /**
   * Get the a transient value of a column.
   */
  const type::TransientValue &GetColumn(int32_t col) { return entry_[col]; }

  /**
   * Is the column null
   * @param st column name
   * @return true if column is null
   */
  bool ColumnIsNull(const std::string &st) {
    int32_t index = sql_table_->ColNameToIndex(st);
    return (entry_[index].Null());
  }

  /**
   * Return the value of a boolean column
   * @param st column name
   * @return boolean column value
   */
  bool GetBooleanColumn(const std::string &st) {
    int32_t index = sql_table_->ColNameToIndex(st);
    TERRIER_ASSERT(entry_[index].Type() == type::TypeId::BOOLEAN, "Type is not boolean");
    return (type::TransientValuePeeker::PeekBoolean(entry_[index]));
  }

  /**
   * Return the value of a tinyint column
   * @param st column name
   * @return int8_t column value
   */
  int8_t GetTinyIntColumn(const std::string &st) {
    int32_t index = sql_table_->ColNameToIndex(st);
    TERRIER_ASSERT(entry_[index].Type() == type::TypeId::TINYINT, "Type is not tiny integer");
    return (type::TransientValuePeeker::PeekTinyInt(entry_[index]));
  }

  /**
   * Return the value of a smallint column
   * @param st column name
   * @return int16_t column value
   */
  int16_t GetSmallIntColumn(const std::string &st) {
    int32_t index = sql_table_->ColNameToIndex(st);
    TERRIER_ASSERT(entry_[index].Type() == type::TypeId::SMALLINT, "Type is not small integer");
    return (type::TransientValuePeeker::PeekSmallInt(entry_[index]));
  }

  /**
   * Return the value of an integer column
   * @param st column name
   * @return int32_t column value
   */
  int32_t GetIntegerColumn(const std::string &st) {
    int32_t index = sql_table_->ColNameToIndex(st);
    TERRIER_ASSERT(entry_[index].Type() == type::TypeId::INTEGER, "Type is not integer");
    return (type::TransientValuePeeker::PeekInteger(entry_[index]));
  }

  /**
   * Return the value of a bigint column
   * @param st column name
   * @return int64_t column value
   */
  int64_t GetBigIntColumn(const std::string &st) {
    int32_t index = sql_table_->ColNameToIndex(st);
    TERRIER_ASSERT(entry_[index].Type() == type::TypeId::BIGINT, "Type is not big integer");
    return (type::TransientValuePeeker::PeekBigInt(entry_[index]));
  }

  /**
   * Return the value of a decimal column
   * @param st column name
   * @return double column value
   */
  double GetDecimalColumn(const std::string &st) {
    int32_t index = sql_table_->ColNameToIndex(st);
    TERRIER_ASSERT(entry_[index].Type() == type::TypeId::DECIMAL, "Type is not decimal");
    return (type::TransientValuePeeker::PeekDecimal(entry_[index]));
  }

  /**
   * Return the value of a timestamp column
   * @param st column name
   * @return timestamp_t column value
   */
  type::timestamp_t GetTimestampColumn(const std::string &st) {
    int32_t index = sql_table_->ColNameToIndex(st);
    TERRIER_ASSERT(entry_[index].Type() == type::TypeId::TIMESTAMP, "Type is not timestamp");
    return (type::TransientValuePeeker::PeekTimestamp(entry_[index]));
  }

  /**
   * Return the value of a date column
   * @param st column name
   * @return date_t column value
   */
  type::date_t GetDateColumn(const std::string &st) {
    int32_t index = sql_table_->ColNameToIndex(st);
    TERRIER_ASSERT(entry_[index].Type() == type::TypeId::DATE, "Type is not date");
    return (type::TransientValuePeeker::PeekDate(entry_[index]));
  }

  /**
   * Return the value of a varchar column
   * @param st column name
   * @return string_view of column
   */
  std::string_view GetVarcharColumn(const std::string &st) {
    int32_t index = sql_table_->ColNameToIndex(st);
    TERRIER_ASSERT(entry_[index].Type() == type::TypeId::VARCHAR, "Type is not varchar");
    return (type::TransientValuePeeker::PeekVarChar(entry_[index]));
  }

  /**
   * Get oid, e.g. tablespace_oid_t, table_oid_t.
   */
  Oid GetOid() { return oid_; }

 protected:
  /**
   * Constructor, should not be directly instantiated.
   */
  CatalogEntry(Oid oid, catalog::SqlTableHelper *sql_table, std::vector<type::TransientValue> &&entry)
      : oid_(oid), sql_table_(sql_table), entry_(std::move(entry)) {}

 private:
  Oid oid_;
  catalog::SqlTableHelper *sql_table_;
  std::vector<type::TransientValue> entry_;
};

}  // namespace terrier::catalog
