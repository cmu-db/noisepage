#include "common/constants.h"
#include "common/error/exception.h"
#include "execution/sql/operators/comparison_operators.h"
#include "execution/sql/vector_operations/vector_operations.h"
#include "execution/util/bit_vector.h"
#include "ips4o/ips4o.hpp"
#include "spdlog/fmt/fmt.h"

namespace noisepage::execution::sql {

namespace {

template <typename T>
void TemplatedSort(const Vector &input, const TupleIdList &non_null_selections, sel_t result[]) {
  const auto *data = reinterpret_cast<const T *>(input.GetData());
  const auto size = non_null_selections.ToSelectionVector(result);
  ips4o::sort(result, result + size, [&](auto idx1, auto idx2) { return LessThanEqual<T>{}(data[idx1], data[idx2]); });
}

}  // namespace

void VectorOps::Sort(const Vector &input, sel_t result[]) {
  TupleIdList non_nulls(common::Constants::K_DEFAULT_VECTOR_SIZE), nulls(common::Constants::K_DEFAULT_VECTOR_SIZE);
  input.GetNonNullSelections(&non_nulls, &nulls);

  // Write NULLs indexes first
  auto num_nulls = nulls.ToSelectionVector(result);

  // Sort non-NULL elements now
  auto non_null_result = result + num_nulls;
  switch (input.GetTypeId()) {
    case TypeId::Boolean:
      TemplatedSort<bool>(input, non_nulls, non_null_result);
      break;
    case TypeId::TinyInt:
      TemplatedSort<int8_t>(input, non_nulls, non_null_result);
      break;
    case TypeId::SmallInt:
      TemplatedSort<int16_t>(input, non_nulls, non_null_result);
      break;
    case TypeId::Integer:
      TemplatedSort<int32_t>(input, non_nulls, non_null_result);
      break;
    case TypeId::BigInt:
      TemplatedSort<int64_t>(input, non_nulls, non_null_result);
      break;
    case TypeId::Hash:
      TemplatedSort<hash_t>(input, non_nulls, non_null_result);
      break;
    case TypeId::Pointer:
      TemplatedSort<uintptr_t>(input, non_nulls, non_null_result);
      break;
    case TypeId::Float:
      TemplatedSort<float>(input, non_nulls, non_null_result);
      break;
    case TypeId::Double:
      TemplatedSort<double>(input, non_nulls, non_null_result);
      break;
    case TypeId::Date:
      TemplatedSort<Date>(input, non_nulls, non_null_result);
      break;
    case TypeId::Timestamp:
      TemplatedSort<Timestamp>(input, non_nulls, non_null_result);
      break;
    case TypeId::Varchar:
      TemplatedSort<storage::VarlenEntry>(input, non_nulls, non_null_result);
      break;
    default:
      throw NOT_IMPLEMENTED_EXCEPTION(fmt::format("Cannot sort vector of type {}.", TypeIdToString(input.GetTypeId())));
  }
}

}  // namespace noisepage::execution::sql
