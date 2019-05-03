#include "execution/sql/vector_projection_iterator.h"

#include "execution/util/vector_util.h"

namespace tpl::sql {

VectorProjectionIterator::VectorProjectionIterator()
    : vector_projection_(nullptr),
      curr_idx_(0),
      num_selected_(0),
      selection_vector_{0},
      selection_vector_read_idx_(0),
      selection_vector_write_idx_(0) {
  selection_vector_[0] = VectorProjectionIterator::kInvalidPos;
}

VectorProjectionIterator::VectorProjectionIterator(VectorProjection *vp) : VectorProjectionIterator() {
  SetVectorProjection(vp);
}

void VectorProjectionIterator::SetVectorProjection(VectorProjection *vp) {
  vector_projection_ = vp;
  num_selected_ = vp->total_tuple_count();
  curr_idx_ = 0;
  selection_vector_[0] = kInvalidPos;
  selection_vector_read_idx_ = 0;
  selection_vector_write_idx_ = 0;
}

// Filter an entire column's data by the provided constant value
template <typename T, template <typename> typename Op>
u32 VectorProjectionIterator::FilterColByValImpl(u32 col_idx, T val) {
  // Get the input column's data
  const T *input = vector_projection_->GetVectorAs<T>(col_idx);

  // Use the existing selection vector if this VPI has been filtered
  const u32 *sel_vec = (IsFiltered() ? selection_vector_ : nullptr);

  // Filter!
  selection_vector_write_idx_ =
      util::VectorUtil::FilterVectorByVal<T, Op>(input, num_selected_, val, selection_vector_, sel_vec);

  // After the filter has been run on the entire vector projection, we need to
  // ensure that we reset it so that clients can query the updated state of the
  // VPI, and subsequent filters operate only on valid tuples potentially
  // filtered out in this filter.
  ResetFiltered();

  // After the call to ResetFiltered(), num_selected_ should indicate the number
  // of valid tuples in the filter.
  return num_selected();
}

// Filter an entire column's data by the provided constant value
template <template <typename> typename Op>
u32 VectorProjectionIterator::FilterColByVal(const u32 col_idx, const FilterVal val) {
  auto *col_type = vector_projection_->GetColumnInfo(col_idx);

  switch (col_type->type.type_id()) {
    case TypeId::SmallInt: {
      return FilterColByValImpl<i16, Op>(col_idx, val.si);
    }
    case TypeId::Integer: {
      return FilterColByValImpl<i32, Op>(col_idx, val.i);
    }
    case TypeId::BigInt: {
      return FilterColByValImpl<i64, Op>(col_idx, val.bi);
    }
    default: { throw std::runtime_error("Filter not supported on type"); }
  }
}

// clang-format off
template u32 VectorProjectionIterator::FilterColByVal<std::equal_to>(u32, FilterVal);
template u32 VectorProjectionIterator::FilterColByVal<std::greater>(u32, FilterVal);
template u32 VectorProjectionIterator::FilterColByVal<std::greater_equal>(u32, FilterVal);
template u32 VectorProjectionIterator::FilterColByVal<std::less>(u32, FilterVal);
template u32 VectorProjectionIterator::FilterColByVal<std::less_equal>(u32, FilterVal);
template u32 VectorProjectionIterator::FilterColByVal<std::not_equal_to>(u32, FilterVal);
// clang-format on

}  // namespace tpl::sql
