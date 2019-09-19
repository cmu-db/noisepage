#include "execution/sql/projected_columns_iterator.h"
#include "execution/util/vector_util.h"
#include "storage/projected_columns.h"
#include "type/type_id.h"

namespace terrier::execution::sql {

ProjectedColumnsIterator::ProjectedColumnsIterator() : selection_vector_{0} {
  selection_vector_[0] = ProjectedColumnsIterator::K_INVALID_POS;
}

ProjectedColumnsIterator::ProjectedColumnsIterator(storage::ProjectedColumns *projected_column)
    : ProjectedColumnsIterator() {
  SetProjectedColumn(projected_column);
}

void ProjectedColumnsIterator::SetProjectedColumn(storage::ProjectedColumns *projected_column) {
  projected_column_ = projected_column;
  num_selected_ = projected_column_->NumTuples();
  curr_idx_ = 0;
  selection_vector_[0] = K_INVALID_POS;
  selection_vector_read_idx_ = 0;
  selection_vector_write_idx_ = 0;
}

template <typename T, template <typename> typename Op>
uint32_t ProjectedColumnsIterator::FilterColByColImpl(const uint32_t col_idx_1, const uint32_t col_idx_2) {
  // Get the input column's data
  const auto *input_1 = reinterpret_cast<const T *>(projected_column_->ColumnStart(static_cast<uint16_t>(col_idx_1)));
  const auto *input_2 = reinterpret_cast<const T *>(projected_column_->ColumnStart(static_cast<uint16_t>(col_idx_2)));

  // Use the existing selection vector if this PCI has been filtered
  const uint32_t *sel_vec = (IsFiltered() ? selection_vector_ : nullptr);

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
  return NumSelected();
}

// Filter an entire column's data by the provided constant value
template <typename T, template <typename> typename Op>
uint32_t ProjectedColumnsIterator::FilterColByValImpl(uint32_t col_idx, T val) {
  // Get the input column's data
  const auto *input = reinterpret_cast<const T *>(projected_column_->ColumnStart(static_cast<uint16_t>(col_idx)));

  // Use the existing selection vector if this PCI has been filtered
  const uint32_t *sel_vec = (IsFiltered() ? selection_vector_ : nullptr);

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
  return NumSelected();
}

// Filter an entire column's data by the provided constant value
template <template <typename> typename Op>
uint32_t ProjectedColumnsIterator::FilterColByVal(uint32_t col_idx, type::TypeId type, FilterVal val) {
  switch (type) {
    case type::TypeId::SMALLINT: {
      return FilterColByValImpl<int16_t, Op>(col_idx, val.si_);
    }
    case type::TypeId::INTEGER: {
      return FilterColByValImpl<int32_t, Op>(col_idx, val.i_);
    }
    case type::TypeId::BIGINT: {
      return FilterColByValImpl<int64_t, Op>(col_idx, val.bi_);
    }
    default: {
      throw std::runtime_error("Filter not supported on type");
    }
  }
}

template <template <typename> typename Op>
uint32_t ProjectedColumnsIterator::FilterColByCol(const uint32_t col_idx_1, type::TypeId type_1,
                                                  const uint32_t col_idx_2, type::TypeId type_2) {
  TERRIER_ASSERT(type_1 == type_2, "Incompatible column types for filter");

  switch (type_1) {
    case type::TypeId::SMALLINT: {
      return FilterColByColImpl<int16_t, Op>(col_idx_1, col_idx_2);
    }
    case type::TypeId::INTEGER: {
      return FilterColByColImpl<int32_t, Op>(col_idx_1, col_idx_2);
    }
    case type::TypeId::BIGINT: {
      return FilterColByColImpl<int64_t, Op>(col_idx_1, col_idx_2);
    }
    default: {
      throw std::runtime_error("Filter not supported on type");
    }
  }
}

template uint32_t ProjectedColumnsIterator::FilterColByVal<std::equal_to>(uint32_t, type::TypeId, FilterVal);
template uint32_t ProjectedColumnsIterator::FilterColByVal<std::greater>(uint32_t, type::TypeId, FilterVal);
template uint32_t ProjectedColumnsIterator::FilterColByVal<std::greater_equal>(uint32_t, type::TypeId, FilterVal);
template uint32_t ProjectedColumnsIterator::FilterColByVal<std::less>(uint32_t, type::TypeId, FilterVal);
template uint32_t ProjectedColumnsIterator::FilterColByVal<std::less_equal>(uint32_t, type::TypeId, FilterVal);
template uint32_t ProjectedColumnsIterator::FilterColByVal<std::not_equal_to>(uint32_t, type::TypeId, FilterVal);
template uint32_t ProjectedColumnsIterator::FilterColByCol<std::equal_to>(uint32_t, type::TypeId, uint32_t,
                                                                          type::TypeId);
template uint32_t ProjectedColumnsIterator::FilterColByCol<std::greater>(uint32_t, type::TypeId, uint32_t,
                                                                         type::TypeId);
template uint32_t ProjectedColumnsIterator::FilterColByCol<std::greater_equal>(uint32_t, type::TypeId, uint32_t,
                                                                               type::TypeId);
template uint32_t ProjectedColumnsIterator::FilterColByCol<std::less>(uint32_t, type::TypeId, uint32_t, type::TypeId);
template uint32_t ProjectedColumnsIterator::FilterColByCol<std::less_equal>(uint32_t, type::TypeId, uint32_t,
                                                                            type::TypeId);
template uint32_t ProjectedColumnsIterator::FilterColByCol<std::not_equal_to>(uint32_t, type::TypeId, uint32_t,
                                                                              type::TypeId);

}  // namespace terrier::execution::sql
