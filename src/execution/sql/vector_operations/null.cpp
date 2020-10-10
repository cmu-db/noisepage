#include "execution/sql/tuple_id_list.h"
#include "execution/sql/vector_operations/vector_operations.h"

namespace terrier::execution::sql {

void VectorOps::IsNull(const Vector &input, TupleIdList *tid_list) {
  TERRIER_ASSERT(input.GetSize() == tid_list->GetCapacity(), "Input vector size != TID list size");
  tid_list->GetMutableBits()->Intersect(input.GetNullMask());
}

void VectorOps::IsNotNull(const Vector &input, TupleIdList *tid_list) {
  TERRIER_ASSERT(input.GetSize() == tid_list->GetCapacity(), "Input vector size != TID list size");
  tid_list->GetMutableBits()->Difference(input.GetNullMask());
}

}  // namespace terrier::execution::sql
