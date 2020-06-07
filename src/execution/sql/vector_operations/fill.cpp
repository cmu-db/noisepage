#include <execution/util/exception.h>
#include "execution/sql/vector_operations/vector_operations.h"

#include "common/exception.h"
#include "execution/sql/generic_value.h"
#include "execution/util/bit_vector.h"

namespace terrier::execution::sql {

namespace {

void CheckFillArguments(const Vector &input, const GenericValue &value) {
  if (input.GetTypeId() != value.GetTypeId()) {
    throw TypeMismatchException(input.GetTypeId(), value.GetTypeId(), "invalid types for fill");
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
      TemplatedFillOperation(vector, value.value_.boolean);
      break;
    case TypeId::TinyInt:
      TemplatedFillOperation(vector, value.value_.tinyint);
      break;
    case TypeId::SmallInt:
      TemplatedFillOperation(vector, value.value_.smallint);
      break;
    case TypeId::Integer:
      TemplatedFillOperation(vector, value.value_.integer);
      break;
    case TypeId::BigInt:
      TemplatedFillOperation(vector, value.value_.bigint);
      break;
    case TypeId::Hash:
      TemplatedFillOperation(vector, value.value_.hash);
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
      throw InvalidTypeException(vector->GetTypeId(), "vector cannot be filled");
  }
}

void VectorOps::FillNull(Vector *vector) { vector->null_mask_.SetAll(); }

}  // namespace terrier::execution::sql
