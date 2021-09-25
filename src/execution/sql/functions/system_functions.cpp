#include "execution/sql/functions/system_functions.h"

#include "common/version.h"
#include "execution/exec/execution_context.h"

namespace noisepage::execution::sql {

void SystemFunctions::Version(UNUSED_ATTRIBUTE exec::ExecutionContext *ctx, StringVal *result) {
  const char *version = common::NOISEPAGE_VERSION_STR.data();
  *result = StringVal(version);
}

void SystemFunctions::Random(Real *result) {
  // TODO(Kyle): Actually generate a random value
  *result = Real(1.0);
}

}  // namespace noisepage::execution::sql
