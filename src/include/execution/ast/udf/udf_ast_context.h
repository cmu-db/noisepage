#pragma once

#include "type/type_id.h"

namespace noisepage {
namespace execution {
namespace ast {
namespace udf {

class UDFASTContext {
 public:
  UDFASTContext() = default;

  /**
   * Set the type of the variabel identifed by `name`.
   * @param name The name of the variable
   * @param type The type to which the variable should be set
   */
  void SetVariableType(const std::string &name, type::TypeId type) { symbol_table_[name] = type; }

  /**
   * Get the type of the variable identified by `name`.
   * @param name The name of the variable
   * @param type The out-parameter used to store the result
   * @return `true` if the variable is present in the symbol
   * table and the Get() succeeds, `false` otherwise
   */
  bool GetVariableType(const std::string &name, type::TypeId *type) {
    auto it = symbol_table_.find(name);
    if (it == symbol_table_.end()) {
      return false;
    }
    if (type != nullptr) {
      *type = it->second;
    }
    return true;
  }

  /**
   * Add a new variable to the symbol table.
   * @param name The name of the variable
   */
  void AddVariable(const std::string &name) { local_variables_.push_back(name); }

  /**
   * Get the local variable at index `index`.
   * @param index The index of interest
   * @return The name of the variable at the specified index
   */
  const std::string &GetLocalVariableAtIndex(const std::size_t index) {
    NOISEPAGE_ASSERT(local_variables_.size() >= index, "Bad variable");
    // TODO(Kyle): Why did this originally have index - 1?
    return local_variables_.at(index);
  }

  /**
   * Get the record type for the specified variable.
   * @param name The name of the variable
   * @return The record
   */
  const std::vector<std::pair<std::string, type::TypeId>> &GetRecordType(const std::string &name) const {
    return record_types_.find(name)->second;
  }

  /**
   * Set the record type for the specified variable.
   * @param name The name of the variable
   * @param elems The record
   */
  void SetRecordType(const std::string &name, std::vector<std::pair<std::string, type::TypeId>> &&elems) {
    record_types_[name] = std::move(elems);
  }

 private:
  // The symbol table for the UDF.
  std::unordered_map<std::string, type::TypeId> symbol_table_;
  // Collection of local variable names for the UDF.
  std::vector<std::string> local_variables_;
  // Collection of record types for the UDF.
  std::unordered_map<std::string, std::vector<std::pair<std::string, type::TypeId>>> record_types_;
};

}  // namespace udf
}  // namespace ast
}  // namespace execution
}  // namespace noisepage
