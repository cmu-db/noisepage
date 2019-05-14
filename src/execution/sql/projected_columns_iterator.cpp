#include "execution/sql/projected_columns_iterator.h"
#include <type/type_id.h>
#include "storage/projected_columns.h"
#include "execution/util/vector_util.h"

namespace tpl::sql {

ProjectedColumnsIterator::ProjectedColumnsIterator()
    : projected_column_(nullptr),
      curr_idx_(0),
      num_selected_(0),
      selection_vector_{0},
      selection_vector_read_idx_(0),
      selection_vector_write_idx_(0) {
  selection_vector_[0] = ProjectedColumnsIterator::kInvalidPos;
}

ProjectedColumnsIterator::ProjectedColumnsIterator(
    ProjectedColumns *projected_column)
    : ProjectedColumnsIterator() {
  SetProjectedColumn(projected_column);
}

void ProjectedColumnsIterator::SetProjectedColumn(
    ProjectedColumns *projected_column) {
  projected_column_ = projected_column;
  num_selected_ = projected_column_->NumTuples();
  curr_idx_ = 0;
  selection_vector_[0] = kInvalidPos;
  selection_vector_read_idx_ = 0;
  selection_vector_write_idx_ = 0;
}

// Filter an entire column's data by the provided constant value
template <typename T, template <typename> typename Op>
u32 ProjectedColumnsIterator::FilterColByValImpl(u32 col_idx, T val) {
  // Get the input column's data
  const T *input =
      reinterpret_cast<const T *>(projected_column_->ColumnStart(static_cast<u16>(col_idx)));

  // Use the existing selection vector if this PCI has been filtered
  const u32 *sel_vec = (IsFiltered() ? selection_vector_ : nullptr);

  // Filter!
  selection_vector_write_idx_ = util::VectorUtil::FilterVectorByVal<T, Op>(
      input, num_selected_, val, selection_vector_, sel_vec);

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
u32 ProjectedColumnsIterator::FilterColByVal(u32 col_idx,
                                             terrier::type::TypeId type,
                                             FilterVal val) {
  switch (type) {
    case terrier::type::TypeId::SMALLINT: {
      return FilterColByValImpl<i16, Op>(col_idx, val.si);
    }
    case terrier::type::TypeId::INTEGER: {
      return FilterColByValImpl<i32, Op>(col_idx, val.i);
    }
    case terrier::type::TypeId::BIGINT: {
      return FilterColByValImpl<i64, Op>(col_idx, val.bi);
    }
    default: { throw std::runtime_error("Filter not supported on type"); }
  }
}

// clang-format off
template u32 ProjectedColumnsIterator::FilterColByVal<std::equal_to>(u32, terrier::type::TypeId, FilterVal);
template u32 ProjectedColumnsIterator::FilterColByVal<std::greater>(u32, terrier::type::TypeId, FilterVal);
template u32 ProjectedColumnsIterator::FilterColByVal<std::greater_equal>(u32, terrier::type::TypeId, FilterVal);
template u32 ProjectedColumnsIterator::FilterColByVal<std::less>(u32, terrier::type::TypeId, FilterVal);
template u32 ProjectedColumnsIterator::FilterColByVal<std::less_equal>(u32, terrier::type::TypeId, FilterVal);
template u32 ProjectedColumnsIterator::FilterColByVal<std::not_equal_to>(u32, terrier::type::TypeId, FilterVal);
// clang-format on

}  // namespace tpl::sql
