#include "common/error/exception.h"
#include "execution/sql/generic_value.h"
#include "execution/sql/vector_operations/vector_operations.h"
#include "execution/util/bit_vector.h"
#include "spdlog/fmt/fmt.h"

namespace noisepage::execution::sql {

namespace {

void CheckFillArguments(const Vector &input, const GenericValue &value) {
  if (input.GetTypeId() != value.GetTypeId()) {
    throw EXECUTION_EXCEPTION(fmt::format("Invalid types for fill, input {} value {}.",
                                          TypeIdToString(input.GetTypeId()), TypeIdToString(value.GetTypeId())),
                              common::ErrorCode::ERRCODE_INTERNAL_ERROR);
  }
}

template <typename T>
void TemplatedFillOperation(Vector *vector, T val) {
  auto *data = reinterpret_cast<T *>(vector->GetData());
  VectorOps::Exec(*vector, [&](uint64_t i, uint64_t k) { data[i] = val; });
}

}  // namespace

void VectorOps::Fill(Vector *vector, const GenericValue &value) {
  // Sanity check
  CheckFillArguments(*vector, value);

  if (value.IsNull()) {
    vector->GetMutableNullMask()->SetAll();
    return;
  }

  vector->GetMutableNullMask()->Reset();

  // Lift-off
  switch (vector->GetTypeId()) {
    case TypeId::Boolean:
      TemplatedFillOperation(vector, value.value_.boolean_);
      break;
    case TypeId::TinyInt:
      TemplatedFillOperation(vector, value.value_.tinyint_);
      break;
    case TypeId::SmallInt:
      TemplatedFillOperation(vector, value.value_.smallint_);
      break;
    case TypeId::Integer:
      TemplatedFillOperation(vector, value.value_.integer_);
      break;
    case TypeId::BigInt:
      TemplatedFillOperation(vector, value.value_.bigint_);
      break;
    case TypeId::Hash:
      TemplatedFillOperation(vector, value.value_.hash_);
      break;
    case TypeId::Float:
      TemplatedFillOperation(vector, value.value_.float_);
      break;
    case TypeId::Double:
      TemplatedFillOperation(vector, value.value_.double_);
      break;
    case TypeId::Date:
      TemplatedFillOperation(vector, value.value_.date_);
      break;
    case TypeId::Timestamp:
      TemplatedFillOperation(vector, value.value_.timestamp_);
      break;
    case TypeId::Varchar:
      TemplatedFillOperation(vector, vector->varlen_heap_.AddVarlen(value.str_value_));
      break;
    default:
      throw EXECUTION_EXCEPTION(fmt::format("Vector of type {} cannot be filled.", TypeIdToString(vector->GetTypeId())),
                                common::ErrorCode::ERRCODE_INTERNAL_ERROR);
  }
}

void VectorOps::FillNull(Vector *vector) { vector->null_mask_.SetAll(); }

}  // namespace noisepage::execution::sql
