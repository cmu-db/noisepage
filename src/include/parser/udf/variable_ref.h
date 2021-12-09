#pragma once

#include <string>
#include <utility>

#include "common/macros.h"

namespace noisepage::parser::udf {

/**
 * The VariableRefType enumeration defines the
 * valid types of variable references.
 */
enum class VariableRefType { SCALAR, TABLE };

/**
 * The VariableRef type represents a UDF variable reference
 * within a SQL query. It is used during binding to identify
 * and track the query parameters that must be read from the
 * UDF environment prior to query execution.
 */
class VariableRef {
 public:
  /**
   * Construct a new VariableRef instance for a TABLE reference.
   * @param table_name The name of the table
   * @param column_name The name of the column
   * @param index The index
   */
  VariableRef(std::string table_name, std::string column_name, std::size_t index)
      : type_{VariableRefType::TABLE},
        table_name_{std::move(table_name)},
        column_name_{std::move(column_name)},
        index_{index} {}

  /**
   * Construct a new VariableRef instance for a SCALAR reference.
   * @param column_name The name of the column
   * @param index The index
   */
  VariableRef(std::string column_name, std::size_t index)
      : type_{VariableRefType::SCALAR}, table_name_{}, column_name_{std::move(column_name)}, index_{index} {}

  /** @return `true` if this is a SCALAR variable reference, `false` otherwise */
  bool IsScalar() const { return type_ == VariableRefType::SCALAR; }

  /** @return The table name of the variable reference */
  const std::string &TableName() const {
    NOISEPAGE_ASSERT(!IsScalar(), "SCALAR variable references do not have associated table names");
    return table_name_;
  }

  /** @return The column name of the variable reference */
  const std::string &ColumnName() const { return column_name_; }

  /** @return The index of the variable reference */
  std::size_t Index() const { return index_; }

  /** @return A string representation of the variable reference */
  std::string ToString() const { return fmt::format("{} {} {}", table_name_.c_str(), column_name_.c_str(), index_); }

 private:
  /** The type of this variable reference */
  const VariableRefType type_;
  /** The table name associated with this variable reference (may be empty) */
  const std::string table_name_;
  /** The column name associated with this variable reference */
  const std::string column_name_;
  /** The index of the reference in the query */
  const std::size_t index_;
};

}  // namespace noisepage::parser::udf
