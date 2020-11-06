#include "execution/exec/execution_settings.h"
#include "execution/sql/operators/numeric_inplace_operators.h"
#include "execution/sql/vector_operations/inplace_operation_executor.h"
#include "execution/sql/vector_operations/vector_operations.h"

namespace noisepage::execution::sql {

namespace traits {

template <typename T>
struct ShouldPerformFullCompute<noisepage::execution::sql::BitwiseANDInPlace<T>> {
  bool operator()(const exec::ExecutionSettings &exec_settings, const TupleIdList *tid_list) const {
    auto full_compute_threshold = exec_settings.GetArithmeticFullComputeOptThreshold();
    return tid_list == nullptr || full_compute_threshold <= tid_list->ComputeSelectivity();
  }
};

}  // namespace traits

namespace {

template <typename T, template <typename> typename Op>
void BitwiseOperation(const exec::ExecutionSettings &exec_settings, Vector *left, const Vector &right) {
  InPlaceOperationExecutor::Execute<T, T, Op<T>>(exec_settings, left, right, Op<T>{});
}

}  // namespace

void VectorOps::BitwiseAndInPlace(const exec::ExecutionSettings &exec_settings, Vector *left, const Vector &right) {
  // Sanity check
  CheckInplaceOperation(left, right);

  // Lift-off
  switch (left->GetTypeId()) {
    case TypeId::TinyInt:
      BitwiseOperation<int8_t, noisepage::execution::sql::BitwiseANDInPlace>(exec_settings, left, right);
      break;
    case TypeId::SmallInt:
      BitwiseOperation<int16_t, noisepage::execution::sql::BitwiseANDInPlace>(exec_settings, left, right);
      break;
    case TypeId::Integer:
      BitwiseOperation<int32_t, noisepage::execution::sql::BitwiseANDInPlace>(exec_settings, left, right);
      break;
    case TypeId::BigInt:
      BitwiseOperation<int64_t, noisepage::execution::sql::BitwiseANDInPlace>(exec_settings, left, right);
      break;
    case TypeId::Pointer:
      BitwiseOperation<uintptr_t, noisepage::execution::sql::BitwiseANDInPlace>(exec_settings, left, right);
      break;
    default:
      throw EXECUTION_EXCEPTION(
          fmt::format("Invalid type for in-place bitwise operation, type {}.", TypeIdToString(left->GetTypeId())),
          common::ErrorCode::ERRCODE_INTERNAL_ERROR);
  }
}

}  // namespace noisepage::execution::sql
