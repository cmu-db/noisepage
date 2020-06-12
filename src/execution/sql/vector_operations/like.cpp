#include "execution/sql/vector_operations/vector_operations.h"

#include "common/macros.h"
#include "execution/sql/operators/like_operators.h"
#include "execution/sql/tuple_id_list.h"
#include "execution/util/exception.h"

namespace terrier::execution::sql {

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
  TERRIER_ASSERT(a.GetSize() == tid_list->GetCapacity() && b.GetSize() == tid_list->GetCapacity(),
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
    throw INVALID_TYPE_EXCEPTION(a.GetTypeId(), "Inputs to (NOT) LIKE must be VARCHAR");
  }
  if (a.IsConstant()) {
    throw EXECUTOR_EXCEPTION("First input to LIKE cannot be constant");
  }

  if (b.IsConstant()) {
    TemplatedLikeOperationVectorConstant<Op>(a, b, tid_list);
  } else {
    TemplatedLikeOperationVectorVector<Op>(a, b, tid_list);
  }
}

}  // namespace

void VectorOps::Like(const Vector &a, const Vector &b, TupleIdList *tid_list) {
  TemplatedLikeOperation<sql::Like>(a, b, tid_list);
}

void VectorOps::NotLike(const Vector &a, const Vector &b, TupleIdList *tid_list) {
  TemplatedLikeOperation<sql::NotLike>(a, b, tid_list);
}

}  // namespace terrier::execution::sql
