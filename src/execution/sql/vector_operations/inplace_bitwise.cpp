#include "execution/sql/vector_operations/vector_operations.h"

#include "execution/sql/operators/numeric_inplace_operators.h"
#include "execution/sql/vector_operations/inplace_operation_executor.h"
#include "execution/util/settings.h"

namespace terrier::execution::sql {

namespace traits {

template <typename T>
struct ShouldPerformFullCompute<terrier::execution::sql::BitwiseANDInPlace<T>> {
  bool operator()(const TupleIdList *tid_list) const {
    auto full_compute_threshold =
        execution::Settings::Instance()->GetDouble(Settings::Name::ArithmeticFullComputeOptThreshold);
    return tid_list == nullptr || full_compute_threshold <= tid_list->ComputeSelectivity();
  }
};

}  // namespace traits

namespace {

template <typename T, template <typename> typename Op>
void BitwiseOperation(Vector *left, const Vector &right) {
  InPlaceOperationExecutor::Execute<T, T, Op<T>>(left, right, Op<T>{});
}

}  // namespace

void VectorOps::BitwiseAndInPlace(Vector *left, const Vector &right) {
  // Sanity check
  CheckInplaceOperation(left, right);

  // Lift-off
  switch (left->GetTypeId()) {
    case TypeId::TinyInt:
      BitwiseOperation<int8_t, terrier::execution::sql::BitwiseANDInPlace>(left, right);
      break;
    case TypeId::SmallInt:
      BitwiseOperation<int16_t, terrier::execution::sql::BitwiseANDInPlace>(left, right);
      break;
    case TypeId::Integer:
      BitwiseOperation<int32_t, terrier::execution::sql::BitwiseANDInPlace>(left, right);
      break;
    case TypeId::BigInt:
      BitwiseOperation<int64_t, terrier::execution::sql::BitwiseANDInPlace>(left, right);
      break;
    case TypeId::Pointer:
      BitwiseOperation<uintptr_t, terrier::execution::sql::BitwiseANDInPlace>(left, right);
      break;
    default:
      throw InvalidTypeException(left->GetTypeId(), "invalid type for in-place bitwise operation");
  }
}

}  // namespace terrier::execution::sql
