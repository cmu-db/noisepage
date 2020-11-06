#include "execution/sql/operators/cast_operators.h"

#include "fast_float/fast_float.h"

namespace noisepage::execution::sql {

bool TryCast<storage::VarlenEntry, float>::operator()(const storage::VarlenEntry &input, float *output) const {
  const auto *buf = reinterpret_cast<const char *>(input.Content());
  auto [ptr, ec] = fast_float::from_chars(buf, buf + input.Size(), *output);
  return ptr == buf + input.Size() && ec == std::errc();
}

bool TryCast<storage::VarlenEntry, double>::operator()(const storage::VarlenEntry &input, double *output) const {
  const auto *buf = reinterpret_cast<const char *>(input.Content());
  auto [ptr, ec] = fast_float::from_chars(buf, buf + input.Size(), *output);
  return ptr == buf + input.Size() && ec == std::errc();
}

}  // namespace noisepage::execution::sql
