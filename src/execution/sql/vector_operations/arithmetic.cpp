#include <execution/util/settings.h>
#include "execution/sql/vector_operations/vector_operations.h"

#include "common/settings.h"
#include "execution/sql/operators/numeric_binary_operators.h"
#include "execution/sql/vector_operations/binary_operation_executor.h"

namespace terrier::execution::sql {

namespace traits {

// Specialized struct to enable full-computation.
template <template <typename> typename Op, typename T>
struct ShouldPerformFullCompute<Op<T>, std::enable_if_t<std::is_same_v<Op<T>, terrier::execution::sql::Add<T>> ||
                                                        std::is_same_v<Op<T>, terrier::execution::sql::Subtract<T>> ||
                                                        std::is_same_v<Op<T>, terrier::execution::sql::Multiply<T>>>> {
  bool operator()(const TupleIdList *tid_list) const {
    auto full_compute_threshold = Settings::Instance()->GetDouble(Settings::Name::ArithmeticFullComputeOptThreshold);
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
    throw TypeMismatchException(left.GetTypeId(), right.GetTypeId(),
                                "left and right vector types to binary operation must be the same");
  }
  if (left.GetTypeId() != result->GetTypeId()) {
    throw TypeMismatchException(left.GetTypeId(), result->GetTypeId(),
                                "result type of binary operation must be the same as input types");
  }
  if (!left.IsConstant() && !right.IsConstant() && left.GetCount() != right.GetCount()) {
    throw Exception(ExceptionType::Cardinality,
                    "left and right input vectors to binary operation must have the same size");
  }
}

template <typename T, typename Op>
void TemplatedDivModOperation_Constant_Vector(const Vector &left, const Vector &right, Vector *result, Op op) {
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
void TemplatedDivModOperation_Vector_Constant(const Vector &left, const Vector &right, Vector *result, Op op) {
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
void TemplatedDivModOperation_Vector_Vector(const Vector &left, const Vector &right, Vector *result, Op op) {
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
    TemplatedDivModOperation_Constant_Vector<T>(left, right, result, Op<T>{});
  } else if (right.IsConstant()) {
    TemplatedDivModOperation_Vector_Constant<T>(left, right, result, Op<T>{});
  } else {
    TemplatedDivModOperation_Vector_Vector<T>(left, right, result, Op<T>{});
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
      throw InvalidTypeException(left.GetTypeId(), "Invalid type for arithmetic operation");
  }
}

template <typename T, template <typename> typename Op>
void TemplatedBinaryArithmeticOperation(const Vector &left, const Vector &right, Vector *result) {
  BinaryOperationExecutor::Execute<T, T, T, Op<T>>(left, right, result);
}

// Dispatch to the generic BinaryOperation() function with full types.
template <template <typename> typename Op>
void BinaryArithmeticOperation(const Vector &left, const Vector &right, Vector *result) {
  // Sanity check
  CheckBinaryOperation(left, right, result);

  // Lift-off
  switch (left.GetTypeId()) {
    case TypeId::TinyInt:
      TemplatedBinaryArithmeticOperation<int8_t, Op>(left, right, result);
      break;
    case TypeId::SmallInt:
      TemplatedBinaryArithmeticOperation<int16_t, Op>(left, right, result);
      break;
    case TypeId::Integer:
      TemplatedBinaryArithmeticOperation<int32_t, Op>(left, right, result);
      break;
    case TypeId::BigInt:
      TemplatedBinaryArithmeticOperation<int64_t, Op>(left, right, result);
      break;
    case TypeId::Float:
      TemplatedBinaryArithmeticOperation<float, Op>(left, right, result);
      break;
    case TypeId::Double:
      TemplatedBinaryArithmeticOperation<double, Op>(left, right, result);
      break;
    case TypeId::Pointer:
      TemplatedBinaryArithmeticOperation<uintptr_t, Op>(left, right, result);
      break;
    default:
      throw InvalidTypeException(left.GetTypeId(), "Invalid type for arithmetic operation");
  }
}

}  // namespace

void VectorOps::Add(const Vector &left, const Vector &right, Vector *result) {
  BinaryArithmeticOperation<terrier::execution::sql::Add>(left, right, result);
}

void VectorOps::Subtract(const Vector &left, const Vector &right, Vector *result) {
  BinaryArithmeticOperation<terrier::execution::sql::Subtract>(left, right, result);
}

void VectorOps::Multiply(const Vector &left, const Vector &right, Vector *result) {
  BinaryArithmeticOperation<terrier::execution::sql::Multiply>(left, right, result);
}

void VectorOps::Divide(const Vector &left, const Vector &right, Vector *result) {
  DivModOperation<terrier::execution::sql::Divide>(left, right, result);
}

void VectorOps::Modulo(const Vector &left, const Vector &right, Vector *result) {
  DivModOperation<terrier::execution::sql::Modulo>(left, right, result);
}

}  // namespace terrier::execution::sql
