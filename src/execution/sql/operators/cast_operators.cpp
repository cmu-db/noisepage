#include "execution/sql/operators/cast_operators.h"

#include "execution/util/fast_double_parser.h"

namespace noisepage::execution::sql {

bool TryCast<storage::VarlenEntry, float>::operator()(const storage::VarlenEntry &input, float *output) const {
  double double_output;
  if (!TryCast<storage::VarlenEntry, double>{}(input, &double_output)) {
    return false;
  }
  return TryCast<double, float>{}(double_output, output);
}

bool TryCast<storage::VarlenEntry, double>::operator()(const storage::VarlenEntry &input, double *output) const {
  const auto *buf = reinterpret_cast<const char *>(input.Content());
  return util::FastDoubleParser::ParseNumber(buf, output);
}

}  // namespace noisepage::execution::sql
