#include "binder/cte/binder_util.h"

#include "parser/delete_statement.h"
#include "parser/insert_statement.h"
#include "parser/select_statement.h"
#include "parser/table_ref.h"
#include "parser/update_statement.h"

namespace noisepage::binder::cte {

std::vector<common::ManagedPointer<parser::TableRef>> BinderUtil::GetSelectWithOrder(
    common::ManagedPointer<parser::SelectStatement> select_statement) {
  return {};
}

std::vector<common::ManagedPointer<parser::TableRef>> BinderUtil::GetInsertWithOrder(
    common::ManagedPointer<parser::InsertStatement> insert_statement) {
  throw NOT_IMPLEMENTED_EXCEPTION("Statement Dependency Analysis for WITH ... INSERT Not Implemeneted");
}

std::vector<common::ManagedPointer<parser::TableRef>> BinderUtil::GetUpdateWithOrder(
    common::ManagedPointer<parser::UpdateStatement> update_statement) {
  throw NOT_IMPLEMENTED_EXCEPTION("Statement Dependency Analysis for WITH ... UPDATE Not Implemeneted");
}

std::vector<common::ManagedPointer<parser::TableRef>> BinderUtil::GetDeleteWithOrder(
    common::ManagedPointer<parser::DeleteStatement> delete_statement) {
  throw NOT_IMPLEMENTED_EXCEPTION("Statement Dependency Analysis for WITH ... DELETE Not Implemeneted");
}
}  // namespace noisepage::binder::cte
