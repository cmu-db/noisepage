#include "execution/exec/execution_settings.h"
#include "execution/sql/operators/numeric_inplace_operators.h"
#include "execution/sql/vector_operations/inplace_operation_executor.h"
#include "execution/sql/vector_operations/vector_operations.h"

namespace noisepage::execution::sql {

namespace traits {

template <typename T>
struct ShouldPerformFullCompute<noisepage::execution::sql::AddInPlace<T>> {
  bool operator()(const exec::ExecutionSettings &exec_settings, const TupleIdList *tid_list) const {
    auto full_compute_threshold = exec_settings.GetArithmeticFullComputeOptThreshold();
    return tid_list == nullptr || full_compute_threshold <= tid_list->ComputeSelectivity();
  }
};

}  // namespace traits

namespace {

template <typename T, template <typename> typename Op>
void InPlaceOperation(const exec::ExecutionSettings &exec_settings, Vector *left, const Vector &right) {
  InPlaceOperationExecutor::Execute<T, T, Op<T>>(exec_settings, left, right);
}

}  // namespace

void VectorOps::AddInPlace(const exec::ExecutionSettings &exec_settings, Vector *left, const Vector &right) {
  // Sanity check
  CheckInplaceOperation(left, right);

  // Lift-off
  switch (left->GetTypeId()) {
    case TypeId::TinyInt:
      InPlaceOperation<int8_t, noisepage::execution::sql::AddInPlace>(exec_settings, left, right);
      break;
    case TypeId::SmallInt:
      InPlaceOperation<int16_t, noisepage::execution::sql::AddInPlace>(exec_settings, left, right);
      break;
    case TypeId::Integer:
      InPlaceOperation<int32_t, noisepage::execution::sql::AddInPlace>(exec_settings, left, right);
      break;
    case TypeId::BigInt:
      InPlaceOperation<int64_t, noisepage::execution::sql::AddInPlace>(exec_settings, left, right);
      break;
    case TypeId::Float:
      InPlaceOperation<float, noisepage::execution::sql::AddInPlace>(exec_settings, left, right);
      break;
    case TypeId::Double:
      InPlaceOperation<double, noisepage::execution::sql::AddInPlace>(exec_settings, left, right);
      break;
    case TypeId::Pointer:
      InPlaceOperation<uintptr_t, noisepage::execution::sql::AddInPlace>(exec_settings, left, right);
      break;
    default:
      throw EXECUTION_EXCEPTION(
          fmt::format("Invalid type for in-place arithmetic operation, type {}.", TypeIdToString(left->GetTypeId())),
          common::ErrorCode::ERRCODE_INTERNAL_ERROR);
  }
}

}  // namespace noisepage::execution::sql
