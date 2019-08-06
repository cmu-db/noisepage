#include "execution/sql/projected_columns_iterator.h"
#include <type/type_id.h>
#include "execution/util/vector_util.h"
#include "storage/projected_columns.h"

namespace terrier::sql {

ProjectedColumnsIterator::ProjectedColumnsIterator() : selection_vector_{0} {
  selection_vector_[0] = ProjectedColumnsIterator::kInvalidPos;
}

ProjectedColumnsIterator::ProjectedColumnsIterator(ProjectedColumns *projected_column) : ProjectedColumnsIterator() {
  SetProjectedColumn(projected_column);
}

void ProjectedColumnsIterator::SetProjectedColumn(ProjectedColumns *projected_column) {
  projected_column_ = projected_column;
  num_selected_ = projected_column_->NumTuples();
  curr_idx_ = 0;
  selection_vector_[0] = kInvalidPos;
  selection_vector_read_idx_ = 0;
  selection_vector_write_idx_ = 0;
}

template <typename T, template <typename> typename Op>
u32 ProjectedColumnsIterator::FilterColByColImpl(const u32 col_idx_1, const u32 col_idx_2) {
  // Get the input column's data
  const auto *input_1 = reinterpret_cast<const T *>(projected_column_->ColumnStart(static_cast<u16>(col_idx_1)));
  const auto *input_2 = reinterpret_cast<const T *>(projected_column_->ColumnStart(static_cast<u16>(col_idx_2)));

  // Use the existing selection vector if this PCI has been filtered
  const u32 *sel_vec = (IsFiltered() ? selection_vector_ : nullptr);

  // Filter!
  selection_vector_write_idx_ =
      util::VectorUtil::FilterVectorByVector<T, Op>(input_1, input_2, num_selected_, selection_vector_, sel_vec);

  // After the filter has been run on the entire vector projection, we need to
  // ensure that we reset it so that clients can query the updated state of the
  // PCI, and subsequent filters operate only on valid tuples potentially
  // filtered out in this filter.
  ResetFiltered();

  // After the call to ResetFiltered(), num_selected_ should indicate the number
  // of valid tuples in the filter.
  return num_selected();
}

// Filter an entire column's data by the provided constant value
template <typename T, template <typename> typename Op>
u32 ProjectedColumnsIterator::FilterColByValImpl(u32 col_idx, T val) {
  // Get the input column's data
  const auto *input = reinterpret_cast<const T *>(projected_column_->ColumnStart(static_cast<u16>(col_idx)));

  // Use the existing selection vector if this PCI has been filtered
  const u32 *sel_vec = (IsFiltered() ? selection_vector_ : nullptr);

  // Filter!
  selection_vector_write_idx_ =
      util::VectorUtil::FilterVectorByVal<T, Op>(input, num_selected_, val, selection_vector_, sel_vec);

  // After the filter has been run on the entire vector projection, we need to
  // ensure that we reset it so that clients can query the updated state of the
  // PCI, and subsequent filters operate only on valid tuples potentially
  // filtered out in this filter.
  ResetFiltered();

  // After the call to ResetFiltered(), num_selected_ should indicate the number
  // of valid tuples in the filter.
  return num_selected();
}

// Filter an entire column's data by the provided constant value
template <template <typename> typename Op>
u32 ProjectedColumnsIterator::FilterColByVal(u32 col_idx, terrier::type::TypeId type, FilterVal val) {
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

template <template <typename> typename Op>
u32 ProjectedColumnsIterator::FilterColByCol(const u32 col_idx_1, terrier::type::TypeId type_1, const u32 col_idx_2,
                                             terrier::type::TypeId type_2) {
  TPL_ASSERT(type_1 == type_2, "Incompatible column types for filter");

  switch (type_1) {
    case terrier::type::TypeId::SMALLINT: {
      return FilterColByColImpl<i16, Op>(col_idx_1, col_idx_2);
    }
    case terrier::type::TypeId::INTEGER: {
      return FilterColByColImpl<i32, Op>(col_idx_1, col_idx_2);
    }
    case terrier::type::TypeId::BIGINT: {
      return FilterColByColImpl<i64, Op>(col_idx_1, col_idx_2);
    }
    default: { throw std::runtime_error("Filter not supported on type"); }
  }
}

template u32 ProjectedColumnsIterator::FilterColByVal<std::equal_to>(u32, terrier::type::TypeId, FilterVal);
template u32 ProjectedColumnsIterator::FilterColByVal<std::greater>(u32, terrier::type::TypeId, FilterVal);
template u32 ProjectedColumnsIterator::FilterColByVal<std::greater_equal>(u32, terrier::type::TypeId, FilterVal);
template u32 ProjectedColumnsIterator::FilterColByVal<std::less>(u32, terrier::type::TypeId, FilterVal);
template u32 ProjectedColumnsIterator::FilterColByVal<std::less_equal>(u32, terrier::type::TypeId, FilterVal);
template u32 ProjectedColumnsIterator::FilterColByVal<std::not_equal_to>(u32, terrier::type::TypeId, FilterVal);
template u32 ProjectedColumnsIterator::FilterColByCol<std::equal_to>(u32, terrier::type::TypeId, u32,
                                                                     terrier::type::TypeId);
template u32 ProjectedColumnsIterator::FilterColByCol<std::greater>(u32, terrier::type::TypeId, u32,
                                                                    terrier::type::TypeId);
template u32 ProjectedColumnsIterator::FilterColByCol<std::greater_equal>(u32, terrier::type::TypeId, u32,
                                                                          terrier::type::TypeId);
template u32 ProjectedColumnsIterator::FilterColByCol<std::less>(u32, terrier::type::TypeId, u32,
                                                                 terrier::type::TypeId);
template u32 ProjectedColumnsIterator::FilterColByCol<std::less_equal>(u32, terrier::type::TypeId, u32,
                                                                       terrier::type::TypeId);
template u32 ProjectedColumnsIterator::FilterColByCol<std::not_equal_to>(u32, terrier::type::TypeId, u32,
                                                                         terrier::type::TypeId);

}  // namespace terrier::sql
