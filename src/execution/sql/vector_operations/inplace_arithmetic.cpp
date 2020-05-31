#include "execution/sql/vector_operations/vector_operations.h"

#include "execution/util/settings.h"
#include "execution/sql/operators/numeric_inplace_operators.h"
#include "execution/sql/vector_operations/inplace_operation_executor.h"

namespace terrier::execution::sql {

namespace traits {

template <typename T>
struct ShouldPerformFullCompute<terrier::execution::sql::AddInPlace<T>> {
  bool operator()(const TupleIdList *tid_list) const {
    auto full_compute_threshold =
        Settings::Instance()->GetDouble(Settings::Name::ArithmeticFullComputeOptThreshold);
    return tid_list == nullptr || full_compute_threshold <= tid_list->ComputeSelectivity();
  }
};

}  // namespace traits

namespace {

template <typename T, template <typename> typename Op>
void InPlaceOperation(Vector *left, const Vector &right) {
  InPlaceOperationExecutor::Execute<T, T, Op<T>>(left, right);
}

}  // namespace

void VectorOps::AddInPlace(Vector *left, const Vector &right) {
  // Sanity check
  CheckInplaceOperation(left, right);

  // Lift-off
  switch (left->GetTypeId()) {
    case TypeId::TinyInt:
      InPlaceOperation<int8_t, terrier::execution::sql::AddInPlace>(left, right);
      break;
    case TypeId::SmallInt:
      InPlaceOperation<int16_t, terrier::execution::sql::AddInPlace>(left, right);
      break;
    case TypeId::Integer:
      InPlaceOperation<int32_t, terrier::execution::sql::AddInPlace>(left, right);
      break;
    case TypeId::BigInt:
      InPlaceOperation<int64_t, terrier::execution::sql::AddInPlace>(left, right);
      break;
    case TypeId::Float:
      InPlaceOperation<float, terrier::execution::sql::AddInPlace>(left, right);
      break;
    case TypeId::Double:
      InPlaceOperation<double, terrier::execution::sql::AddInPlace>(left, right);
      break;
    case TypeId::Pointer:
      InPlaceOperation<uintptr_t, terrier::execution::sql::AddInPlace>(left, right);
      break;
    default:
      throw InvalidTypeException(left->GetTypeId(),
                                 "invalid type for in-place arithmetic operation");
  }
}

}  // namespace terrier::execution::sql
