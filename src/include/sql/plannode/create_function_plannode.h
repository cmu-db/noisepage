//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// create_function_plan.h
//
// Identification: src/include/planner/create_function_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/*
 * TODO:
 * 1. Bring in parser objects.
 * 2. Figure out how UDFs should be handled (types, PLTypes
 * 3. Fix copy function
 */

#pragma once

#include "abstract_plannode.h"

namespace terrier::sql::plannode {

class CreateFunctionPlanNode : public AbstractPlanNode {
 public:
  CreateFunctionPlanNode() = delete;

  // TODO: Temporary fix to handle Copy()
  explicit CreateFunctionPlanNode(std::string func);

  explicit CreateFunctionPlanNode(parser::CreateFunctionStatement *parse_tree);

  inline PlanNodeType GetPlanNodeType() const {
    return PlanNodeType::CREATE_FUNC;
  }

  const std::string GetInfo() const { return "Get Create Function Plan"; }

  // TODO: This looks like a hack
  std::unique_ptr<AbstractPlanNode> Copy() const {
    return std::unique_ptr<AbstractPlanNode>(
        new CreateFunctionPlanNode("UDF function"));
  }

  inline std::string GetFunctionName() const { return function_name; }

  inline PLType GetUDFLanguage() const { return language; }

  inline std::vector<std::string> GetFunctionBody() const {
    return function_body;
  }

  inline std::vector<std::string> GetFunctionParameterNames() const {
    return function_param_names;
  }

  inline std::vector<type::TypeId> GetFunctionParameterTypes() const {
    return function_param_types;
  }

  inline type::TypeId GetReturnType() const { return return_type; }

  inline bool IsReplace() const { return is_replace; }

  inline int GetNumParams() const { return param_count; }

 private:
  // Indicates the UDF language type
  PLType language;

  // Function parameters names passed to the UDF
  std::vector<std::string> function_param_names;

  // Function parameter types passed to the UDF
  std::vector<type::TypeId> function_param_types;

  // Query string/ function body of the UDF
  std::vector<std::string> function_body;

  // Indicates if the function definition needs to be replaced
  bool is_replace;

  // Function name of the UDF
  std::string function_name;

  // Return type of the UDF
  type::TypeId return_type;

  int param_count = 0;
};

}  // namespace terrier:sql::plannode