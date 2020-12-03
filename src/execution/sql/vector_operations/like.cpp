#include "common/error/error_code.h"
#include "common/error/exception.h"
#include "common/macros.h"
#include "execution/sql/operators/like_operators.h"
#include "execution/sql/tuple_id_list.h"
#include "execution/sql/vector_operations/vector_operations.h"
#include "spdlog/fmt/fmt.h"

namespace noisepage::execution::sql {

namespace {

template <typename Op>
void TemplatedLikeOperationVectorConstant(const Vector &a, const Vector &b, TupleIdList *tid_list) {
  if (b.IsNull(0)) {
    tid_list->Clear();
    return;
  }

  const auto *RESTRICT a_data = reinterpret_cast<const storage::VarlenEntry *>(a.GetData());
  const auto *RESTRICT b_data = reinterpret_cast<const storage::VarlenEntry *>(b.GetData());

  // Remove NULL entries from the left input
  tid_list->GetMutableBits()->Difference(a.GetNullMask());

  // Lift-off
  tid_list->Filter([&](const uint64_t i) { return Op{}(a_data[i], b_data[0]); });
}

template <typename Op>
void TemplatedLikeOperationVectorVector(const Vector &a, const Vector &b, TupleIdList *tid_list) {
  NOISEPAGE_ASSERT(a.GetSize() == tid_list->GetCapacity() && b.GetSize() == tid_list->GetCapacity(),
                   "Input/output TID list not large enough to store all TIDS from inputs to LIKE()");

  const auto *RESTRICT a_data = reinterpret_cast<const storage::VarlenEntry *>(a.GetData());
  const auto *RESTRICT b_data = reinterpret_cast<const storage::VarlenEntry *>(b.GetData());

  // Remove NULL entries in both left and right inputs (cheap)
  tid_list->GetMutableBits()->Difference(a.GetNullMask()).Difference(b.GetNullMask());

  // Lift-off
  tid_list->Filter([&](const uint64_t i) { return Op{}(a_data[i], b_data[i]); });
}

template <typename Op>
void TemplatedLikeOperation(const Vector &a, const Vector &b, TupleIdList *tid_list) {
  if (a.GetTypeId() != TypeId::Varchar || b.GetTypeId() != TypeId::Varchar) {
    throw EXECUTION_EXCEPTION(fmt::format("Inputs to (NOT) LIKE must be VARCHAR, left {} right {}.",
                                          TypeIdToString(a.GetTypeId()), TypeIdToString(b.GetTypeId())),
                              common::ErrorCode::ERRCODE_INTERNAL_ERROR);
  }

  if (b.IsConstant()) {
    TemplatedLikeOperationVectorConstant<Op>(a, b, tid_list);
  } else {
    TemplatedLikeOperationVectorVector<Op>(a, b, tid_list);
  }
}

}  // namespace

void VectorOps::SelectLike(const exec::ExecutionSettings &exec_settings, const Vector &a, const Vector &b,
                           TupleIdList *tid_list) {
  TemplatedLikeOperation<sql::Like>(a, b, tid_list);
}

void VectorOps::SelectNotLike(const exec::ExecutionSettings &exec_settings, const Vector &a, const Vector &b,
                              TupleIdList *tid_list) {
  TemplatedLikeOperation<sql::NotLike>(a, b, tid_list);
}

}  // namespace noisepage::execution::sql
