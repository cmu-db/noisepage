#pragma once

#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "execution/sql/sql.h"

namespace noisepage::execution::ast::udf {

/**
 * The UdfAstContext class maintains state that is utilized
 * throughout construction of the UDF abstract syntax tree.
 */
class UdfAstContext {
  /** An invidual entry for a record type, (name, type ID) */
  using RecordTypeEntry = std::pair<std::string, sql::SqlTypeId>;

  /** A full description of a record type */
  using RecordType = std::vector<RecordTypeEntry>;

 public:
  /**
   * Construct a new AstContext instance.
   */
  UdfAstContext() = default;

  /**
   * Push a new local variable.
   * @param name The name of the variable
   */
  void AddLocal(const std::string &name) { locals_.push_back(name); }

  /**
   * Get the local variable at index `index`.
   * @param index The index of interest
   * @return The name of the variable at the specified index
   */
  const std::string &GetLocalAtIndex(const std::size_t index) const {
    NOISEPAGE_ASSERT(locals_.size() >= index, "Index out of range");
    // TODO(Kyle): I moved the subtraction to the call site because
    // it seems misleading to have a getter for an index but deliver
    // a local that does not actually appear at that index...
    return locals_.at(index);
  }

  /**
   * Determine if a variable with name `name` is present in the UDF AST.
   * @param name The name of the variable
   * @return `true` if the UDF AST context contains a variable
   * identified by `name`, `false` otherwise
   */
  bool HasVariable(const std::string &name) const { return (symbol_table_.find(name) != symbol_table_.cend()); }

  /**
   * Set the type of the variabel identifed by `name`.
   * @param name The name of the variable
   * @param type The type to which the variable should be set
   */
  void SetVariableType(const std::string &name, sql::SqlTypeId type) { symbol_table_[name] = type; }

  /**
   * Get the type of the variable identified by `name`.
   * @param name The name of the variable
   * @return The type ID for the specified variable if present,
   * empty optional value otherwise
   */
  std::optional<sql::SqlTypeId> GetVariableType(const std::string &name) const {
    auto it = symbol_table_.find(name);
    return (it == symbol_table_.cend()) ? std::nullopt : std::make_optional(it->second);
  }

  /**
   * Get the type of the variable identified by `name`.
   * @param name The name of the variable
   * @return The type ID for the specified variable
   *
   * NOTE: This function terminates the program in the event
   * that the variable is not present; for variable queries
   * that may fail, use UdfAstContext::GetVariableType().
   */
  sql::SqlTypeId GetVariableTypeFailFast(const std::string &name) const {
    auto it = symbol_table_.find(name);
    NOISEPAGE_ASSERT(it != symbol_table_.cend(), "Required variable is not present in UDF AST");
    return it->second;
  }

  /**
   * Determine if a record variable with name `name` is present in the UDF AST.
   * @param name The name of the variable
   * @return `true` if the UDF AST context contains a record variable
   * identified by `name`, `false` otherwise
   */
  bool HasRecord(const std::string &name) const { return (record_types_.find(name) != record_types_.cend()); }

  /**
   * Set the record type for the variable identified by `name`.
   * @param name The name of the variable
   * @param elems The record
   */
  void SetRecordType(const std::string &name, std::vector<std::pair<std::string, sql::SqlTypeId>> &&elems) {
    record_types_[name] = std::move(elems);
  }

  /**
   * Get the record type for the variable identified by `name`.
   * @param name The name of the variable
   * @return The type of the record variable if present,
   * empty optional value otherwise
   */
  std::optional<RecordType> GetRecordType(const std::string &name) const {
    auto it = record_types_.find(name);
    // TODO(Kyle): I updated the API for this function to use std::optional,
    // I like this more, but it makes it impossible to return a reference to
    // the underlying data so this now materializes a copy every time
    return (it == record_types_.cend()) ? std::nullopt : std::make_optional(it->second);
  }

  /**
   * Get the record type for the variable identified by `name`.
   * @param name The name of the variable
   * @return The type of the record variable
   *
   * NOTE: This function terminates the program in the event
   * that the variable is not present; for variable queries
   * that may fail, use UdfAstContext::GetRecordType().
   */
  RecordType GetRecordTypeFailFast(const std::string &name) const {
    auto it = record_types_.find(name);
    NOISEPAGE_ASSERT(it != record_types_.cend(), "Required record variable is not present in UDF AST");
    return it->second;
  }

 private:
  /** Collection of local variable names for the UDF. */
  std::vector<std::string> locals_;
  /** The symbol table for the UDF. */
  std::unordered_map<std::string, sql::SqlTypeId> symbol_table_;
  /** Collection of record types for the UDF. */
  std::unordered_map<std::string, RecordType> record_types_;
};

}  // namespace noisepage::execution::ast::udf
