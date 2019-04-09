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

  bool ColumnIsNull(const std::string &st) {
    int32_t index = sql_table_->ColNameToIndex(st);
    return (entry_[index].Null());
  }

  bool GetBooleanColumn(const std::string &st) {
    int32_t index = sql_table_->ColNameToIndex(st);
    TERRIER_ASSERT(entry_[index].Type() == type::TypeId::BOOLEAN, "Type is not boolean");
    return (type::TransientValuePeeker::PeekBoolean(entry_[index]));
  }

  int8_t GetTinyIntColumn(const std::string &st) {
    int32_t index = sql_table_->ColNameToIndex(st);
    TERRIER_ASSERT(entry_[index].Type() == type::TypeId::TINYINT, "Type is not tiny integer");
    return (type::TransientValuePeeker::PeekTinyInt(entry_[index]));
  }

  int16_t GetSmallIntColumn(const std::string &st) {
    int32_t index = sql_table_->ColNameToIndex(st);
    TERRIER_ASSERT(entry_[index].Type() == type::TypeId::SMALLINT, "Type is not small integer");
    return (type::TransientValuePeeker::PeekSmallInt(entry_[index]));
  }
  int32_t GetIntegerColumn(const std::string &st) {
    int32_t index = sql_table_->ColNameToIndex(st);
    TERRIER_ASSERT(entry_[index].Type() == type::TypeId::INTEGER, "Type is not integer");
    return (type::TransientValuePeeker::PeekInteger(entry_[index]));
  }

  int64_t GetBigIntColumn(const std::string &st) {
    int32_t index = sql_table_->ColNameToIndex(st);
    TERRIER_ASSERT(entry_[index].Type() == type::TypeId::BIGINT, "Type is not big integer");
    return (type::TransientValuePeeker::PeekBigInt(entry_[index]));
  }

  double GetDecimalColumn(const std::string &st) {
    int32_t index = sql_table_->ColNameToIndex(st);
    TERRIER_ASSERT(entry_[index].Type() == type::TypeId::DECIMAL, "Type is not decimal");
    return (type::TransientValuePeeker::PeekDecimal(entry_[index]));
  }

  type::timestamp_t GetTimestampColumn(const std::string &st) {
    int32_t index = sql_table_->ColNameToIndex(st);
    TERRIER_ASSERT(entry_[index].Type() == type::TypeId::TIMESTAMP, "Type is not timestamp");
    return (type::TransientValuePeeker::PeekTimestamp(entry_[index]));
  }

  type::date_t GetDateColumn(const std::string &st) {
    int32_t index = sql_table_->ColNameToIndex(st);
    TERRIER_ASSERT(entry_[index].Type() == type::TypeId::DATE, "Type is not date");
    return (type::TransientValuePeeker::PeekDate(entry_[index]));
  }

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
  CatalogEntry(Oid oid, catalog::SqlTableRW *sql_table, std::vector<type::TransientValue> &&entry) :
      oid_(oid), sql_table_(sql_table), entry_(std::move(entry)) {}

 private:
  Oid oid_;
  catalog::SqlTableRW *sql_table_;
  std::vector<type::TransientValue> entry_;
};

}  // namespace terrier::catalog
