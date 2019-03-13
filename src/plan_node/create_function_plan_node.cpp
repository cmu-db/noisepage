#include "plan_node/create_function_plan_node.h"
#include "storage/data_table.h"

namespace terrier::plan_node {

CreateFunctionPlanNode::CreateFunctionPlanNode(parser::CreateFunctionStatement *create_func_stmt) {
  language = create_func_stmt->GetPLType();
  function_body = create_func_stmt->GetFuncBody();
  is_replace = create_func_stmt->ShouldReplace();
  function_name = create_func_stmt->GetFuncName();

  for (const auto &col : create_func_stmt->GetFuncParameters()) {
    // Adds the function names
    function_param_names.push_back(col->GetParamName());
    param_count++;
    // Adds the function types
    function_param_types.push_back(col->GetDataType());
  }

  auto ret_type_obj = *(create_func_stmt->GetFuncReturnType());
  return_type = ret_type_obj.GetDataType();
}

}  // namespace terrier::plan_node