#include "execution/sql/functions/system_functions.h"

#include <random>

#include "common/version.h"
#include "execution/exec/execution_context.h"

namespace noisepage::execution::sql {

void SystemFunctions::Version(UNUSED_ATTRIBUTE exec::ExecutionContext *ctx, StringVal *result) {
  const char *version = common::NOISEPAGE_VERSION_STR.data();
  *result = StringVal(version);
}

void SystemFunctions::Random(Real *result) {
  // TODO(Kyle): Static locals are kind of gross, where
  // should state for this type of one-off thing live?
  static std::mt19937 generator{std::random_device{}()};  // NOLINT
  static std::uniform_real_distribution<> distribution{0, 1};
  *result = Real(distribution(generator));
}

}  // namespace noisepage::execution::sql
