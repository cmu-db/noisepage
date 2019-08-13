#include "execution/exec/execution_context.h"
#include "execution/sql/value.h"

namespace terrier::execution::exec {

ExecutionContext::StringAllocator::StringAllocator() : region_("") {}

ExecutionContext::StringAllocator::~StringAllocator() = default;

char *ExecutionContext::StringAllocator::Allocate(std::size_t size) {
  return reinterpret_cast<char *>(region_.Allocate(size));
}

void ExecutionContext::StringAllocator::Deallocate(UNUSED char *str) {
  // No-op. Bulk de-allocated upon destruction.
}

uint32_t ExecutionContext::ComputeTupleSize(const planner::OutputSchema *schema) {
  uint32_t tuple_size = 0;
  for (const auto &col : schema->GetColumns()) {
    tuple_size += sql::ValUtil::GetSqlSize(col.GetType());
  }
  return tuple_size;
}

}  // namespace terrier::execution::exec
