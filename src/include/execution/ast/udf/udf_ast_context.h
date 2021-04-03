#pragma once

#include "type/type_id.h"

// TODO(Kyle): Documentation.

namespace noisepage {
namespace execution {
namespace ast {
namespace udf {

class UDFASTContext {
 public:
  UDFASTContext() {}

  void SetVariableType(std::string &var, type::TypeId type) { symbol_table_[var] = type; }

  bool GetVariableType(const std::string &var, type::TypeId *type) {
    auto it = symbol_table_.find(var);
    if (it == symbol_table_.end()) {
      return false;
    }
    if (type != nullptr) {
      *type = it->second;
    }
    return true;
  }

  void AddVariable(std::string name) { local_variables_.push_back(name); }

  const std::string &GetVariableAtIndex(int index) {
    NOISEPAGE_ASSERT(local_variables_.size() >= index, "Bad var");
    return local_variables_[index - 1];
  }

  void SetRecordType(std::string var, std::vector<std::pair<std::string, type::TypeId>> &&elems) {
    record_types_[var] = std::move(elems);
  }

  const std::vector<std::pair<std::string, type::TypeId>> &GetRecordType(const std::string &var) {
    return record_types_.find(var)->second;
  }

 private:
  std::unordered_map<std::string, type::TypeId> symbol_table_;
  std::vector<std::string> local_variables_;
  std::unordered_map<std::string, std::vector<std::pair<std::string, type::TypeId>>> record_types_;
};

}  // namespace udf
}  // namespace ast
}  // namespace execution
}  // namespace noisepage
