#include "common/error/error_code.h"
#include "common/error/exception.h"
#include "execution/exec/execution_settings.h"
#include "execution/sql/operators/numeric_binary_operators.h"
#include "execution/sql/vector_operations/binary_operation_executor.h"
#include "execution/sql/vector_operations/vector_operations.h"
#include "spdlog/fmt/fmt.h"

namespace noisepage::execution::sql {

namespace traits {

// Specialized struct to enable full-computation.
template <template <typename> typename Op, typename T>
struct ShouldPerformFullCompute<Op<T>,
                                std::enable_if_t<std::is_same_v<Op<T>, noisepage::execution::sql::Add<T>> ||
                                                 std::is_same_v<Op<T>, noisepage::execution::sql::Subtract<T>> ||
                                                 std::is_same_v<Op<T>, noisepage::execution::sql::Multiply<T>>>> {
  bool operator()(const exec::ExecutionSettings &exec_settings, const TupleIdList *tid_list) const {
    auto full_compute_threshold = exec_settings.GetArithmeticFullComputeOptThreshold();
    return tid_list == nullptr || full_compute_threshold <= tid_list->ComputeSelectivity();
  }
};

}  // namespace traits

namespace {

// Check:
// 1. Input vectors have the same type.
// 2. Input vectors have the same shape.
// 3. Input and output vectors have the same type.
void CheckBinaryOperation(const Vector &left, const Vector &right, Vector *result) {
  if (left.GetTypeId() != right.GetTypeId()) {
    throw EXECUTION_EXCEPTION(
        fmt::format("Left and right vector types to binary operation must be the same, left {} right {}",
                    TypeIdToString(left.GetTypeId()), TypeIdToString(right.GetTypeId())),
        common::ErrorCode::ERRCODE_INTERNAL_ERROR);
  }
  if (left.GetTypeId() != result->GetTypeId()) {
    throw EXECUTION_EXCEPTION(
        fmt::format("Result type of binary operation must be the same as input type, input {} result {}.",
                    TypeIdToString(left.GetTypeId()), TypeIdToString(result->GetTypeId())),
        common::ErrorCode::ERRCODE_INTERNAL_ERROR);
  }
  if (!left.IsConstant() && !right.IsConstant() && left.GetCount() != right.GetCount()) {
    throw EXECUTION_EXCEPTION(
        fmt::format("Left and right input vectors to binary operation must have the same size, left {} right {}.",
                    left.GetCount(), right.GetCount()),
        common::ErrorCode::ERRCODE_INTERNAL_ERROR);
  }
}

template <typename T, typename Op>
void TemplatedDivModOperationConstantVector(const Vector &left, const Vector &right, Vector *result, Op op) {
  auto *left_data = reinterpret_cast<T *>(left.GetData());
  auto *right_data = reinterpret_cast<T *>(right.GetData());
  auto *result_data = reinterpret_cast<T *>(result->GetData());

  result->Resize(right.GetSize());
  result->SetFilteredTupleIdList(right.GetFilteredTupleIdList(), right.GetCount());

  if (left.IsNull(0)) {
    VectorOps::FillNull(result);
  } else {
    result->GetMutableNullMask()->Copy(right.GetNullMask());

    VectorOps::Exec(right, [&](uint64_t i, uint64_t k) {
      if (right_data[i] == T(0)) {
        result->GetMutableNullMask()->Set(i);
      } else {
        result_data[i] = op(left_data[0], right_data[i]);
      }
    });
  }
}

template <typename T, typename Op>
void TemplatedDivModOperationVectorConstant(const Vector &left, const Vector &right, Vector *result, Op op) {
  auto *left_data = reinterpret_cast<T *>(left.GetData());
  auto *right_data = reinterpret_cast<T *>(right.GetData());
  auto *result_data = reinterpret_cast<T *>(result->GetData());

  result->Resize(left.GetSize());
  result->SetFilteredTupleIdList(left.GetFilteredTupleIdList(), left.GetCount());

  if (right.IsNull(0)) {
    VectorOps::FillNull(result);
  } else {
    result->GetMutableNullMask()->Copy(left.GetNullMask());

    VectorOps::Exec(left, [&](uint64_t i, uint64_t k) {
      if (left_data[i] == T(0)) {
        result->GetMutableNullMask()->Set(i);
      } else {
        result_data[i] = op(left_data[i], right_data[0]);
      }
    });
  }
}

template <typename T, typename Op>
void TemplatedDivModOperationVectorVector(const Vector &left, const Vector &right, Vector *result, Op op) {
  auto *left_data = reinterpret_cast<T *>(left.GetData());
  auto *right_data = reinterpret_cast<T *>(right.GetData());
  auto *result_data = reinterpret_cast<T *>(result->GetData());

  result->Resize(left.GetSize());
  result->GetMutableNullMask()->Copy(left.GetNullMask()).Union(right.GetNullMask());
  result->SetFilteredTupleIdList(left.GetFilteredTupleIdList(), left.GetCount());

  VectorOps::Exec(left, [&](uint64_t i, uint64_t k) {
    if (right_data[i] == T(0)) {
      result->GetMutableNullMask()->Set(i);
    } else {
      result_data[i] = op(left_data[i], right_data[i]);
    }
  });
}

template <typename T, template <typename...> typename Op>
void XTemplatedDivModOperation(const Vector &left, const Vector &right, Vector *result) {
  if (left.IsConstant()) {
    TemplatedDivModOperationConstantVector<T>(left, right, result, Op<T>{});
  } else if (right.IsConstant()) {
    TemplatedDivModOperationVectorConstant<T>(left, right, result, Op<T>{});
  } else {
    TemplatedDivModOperationVectorVector<T>(left, right, result, Op<T>{});
  }
}

// Helper function to execute a divide or modulo operations. The operations are
// performed only on the active elements in the input vectors.
template <template <typename...> typename Op>
void DivModOperation(const Vector &left, const Vector &right, Vector *result) {
  // Sanity check
  CheckBinaryOperation(left, right, result);

  // Lift-off
  switch (left.GetTypeId()) {
    case TypeId::TinyInt:
      XTemplatedDivModOperation<int8_t, Op>(left, right, result);
      break;
    case TypeId::SmallInt:
      XTemplatedDivModOperation<int16_t, Op>(left, right, result);
      break;
    case TypeId::Integer:
      XTemplatedDivModOperation<int32_t, Op>(left, right, result);
      break;
    case TypeId::BigInt:
      XTemplatedDivModOperation<int64_t, Op>(left, right, result);
      break;
    case TypeId::Float:
      XTemplatedDivModOperation<float, Op>(left, right, result);
      break;
    case TypeId::Double:
      XTemplatedDivModOperation<double, Op>(left, right, result);
      break;
    default:
      throw EXECUTION_EXCEPTION(
          fmt::format("Invalid type for arithmetic operation, {}.", TypeIdToString(left.GetTypeId())),
          common::ErrorCode::ERRCODE_INTERNAL_ERROR);
  }
}

template <typename T, template <typename> typename Op>
void TemplatedBinaryArithmeticOperation(const exec::ExecutionSettings &exec_settings, const Vector &left,
                                        const Vector &right, Vector *result) {
  BinaryOperationExecutor::Execute<T, T, T, Op<T>>(exec_settings, left, right, result);
}

// Dispatch to the generic BinaryOperation() function with full types.
template <template <typename> typename Op>
void BinaryArithmeticOperation(const exec::ExecutionSettings &exec_settings, const Vector &left, const Vector &right,
                               Vector *result) {
  // Sanity check
  CheckBinaryOperation(left, right, result);

  // Lift-off
  switch (left.GetTypeId()) {
    case TypeId::TinyInt:
      TemplatedBinaryArithmeticOperation<int8_t, Op>(exec_settings, left, right, result);
      break;
    case TypeId::SmallInt:
      TemplatedBinaryArithmeticOperation<int16_t, Op>(exec_settings, left, right, result);
      break;
    case TypeId::Integer:
      TemplatedBinaryArithmeticOperation<int32_t, Op>(exec_settings, left, right, result);
      break;
    case TypeId::BigInt:
      TemplatedBinaryArithmeticOperation<int64_t, Op>(exec_settings, left, right, result);
      break;
    case TypeId::Float:
      TemplatedBinaryArithmeticOperation<float, Op>(exec_settings, left, right, result);
      break;
    case TypeId::Double:
      TemplatedBinaryArithmeticOperation<double, Op>(exec_settings, left, right, result);
      break;
    case TypeId::Pointer:
      TemplatedBinaryArithmeticOperation<uintptr_t, Op>(exec_settings, left, right, result);
      break;
    default:
      throw EXECUTION_EXCEPTION(
          fmt::format("Invalid type for arithmetic operation, {}.", TypeIdToString(left.GetTypeId())),
          common::ErrorCode::ERRCODE_INTERNAL_ERROR);
  }
}

}  // namespace

void VectorOps::Add(const exec::ExecutionSettings &exec_settings, const Vector &left, const Vector &right,
                    Vector *result) {
  BinaryArithmeticOperation<noisepage::execution::sql::Add>(exec_settings, left, right, result);
}

void VectorOps::Subtract(const exec::ExecutionSettings &exec_settings, const Vector &right, Vector *result,
                         const Vector &left) {
  BinaryArithmeticOperation<noisepage::execution::sql::Subtract>(exec_settings, left, right, result);
}

void VectorOps::Multiply(const exec::ExecutionSettings &exec_settings, const Vector &left, const Vector &right,
                         Vector *result) {
  BinaryArithmeticOperation<noisepage::execution::sql::Multiply>(exec_settings, left, right, result);
}

void VectorOps::Divide(const exec::ExecutionSettings &exec_settings, const Vector &left, const Vector &right,
                       Vector *result) {
  DivModOperation<noisepage::execution::sql::Divide>(left, right, result);
}

void VectorOps::Modulo(const Vector &left, const Vector &right, Vector *result) {
  DivModOperation<noisepage::execution::sql::Modulo>(left, right, result);
}

}  // namespace noisepage::execution::sql
