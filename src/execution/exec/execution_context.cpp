#include "execution/exec/execution_context.h"
#include "execution/sql/value.h"

namespace terrier::execution::exec {

char *ExecutionContext::StringAllocator::Allocate(std::size_t size) {
  return reinterpret_cast<char *>(region_.Allocate(size));
}

uint32_t ExecutionContext::ComputeTupleSize(const planner::OutputSchema *schema) {
  uint32_t tuple_size = 0;
  for (const auto &col : schema->GetColumns()) {
    tuple_size += sql::ValUtil::GetSqlSize(col.GetType());
  }
  return tuple_size;
}

}  // namespace terrier::execution::exec
