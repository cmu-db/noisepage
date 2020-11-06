#include "execution/sql/vector_projection.h"

#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include "execution/sql/tuple_id_list.h"
#include "execution/sql/vector.h"
#include "execution/sql/vector_operations/vector_operations.h"
#include "storage/storage_util.h"

namespace noisepage::execution::sql {

VectorProjection::VectorProjection()
    : owned_tid_list_(common::Constants::K_DEFAULT_VECTOR_SIZE), owned_buffer_(nullptr) {
  owned_tid_list_.Resize(0);
}

void VectorProjection::SetStorageColIds(std::vector<storage::col_id_t> storage_col_ids) {
  storage_col_ids_ = std::move(storage_col_ids);
}

void VectorProjection::InitializeEmpty(const std::vector<TypeId> &col_types) {
  NOISEPAGE_ASSERT(!col_types.empty(), "Cannot create projection with zero columns");
  columns_.resize(col_types.size());
  for (uint32_t i = 0; i < col_types.size(); i++) {
    columns_[i] = std::make_unique<Vector>(col_types[i]);
  }

  // Reset the cached TID list to NULL indicating all TIDs are active.
  filter_ = nullptr;
}

void VectorProjection::Initialize(const std::vector<TypeId> &col_types) {
  // First initialize an empty projection
  InitializeEmpty(col_types);

  // Now allocate space to accommodate all child vector data
  std::size_t size_in_bytes = 0;
  for (const auto &col_type : col_types) {
    size_in_bytes += GetTypeIdSize(col_type) * common::Constants::K_DEFAULT_VECTOR_SIZE;
  }

  // std::make_unique() with an array-type zeros the array for us due to
  // value-initialization. We don't need to explicitly memset() it.
  owned_buffer_ = std::make_unique<byte[]>(size_in_bytes);

  // Setup the vector's to reference our data chunk
  byte *ptr = owned_buffer_.get();
  for (uint64_t i = 0; i < col_types.size(); i++) {
    columns_[i]->Reference(ptr, nullptr, 0);
    ptr += GetTypeIdSize(col_types[i]) * common::Constants::K_DEFAULT_VECTOR_SIZE;
  }
}

void VectorProjection::RefreshFilteredTupleIdList() {
  // If the list of active TIDs is a strict subset of all TIDs in the projection,
  // we need to update the cached filter list. Otherwise, we set the filter list
  // to NULL to indicate the non-existence of a filter. In either case, we also
  // propagate the list to all child vectors.

  uint32_t count = owned_tid_list_.GetTupleCount();

  if (count < owned_tid_list_.GetCapacity()) {
    filter_ = &owned_tid_list_;
  } else {
    filter_ = nullptr;
    count = owned_tid_list_.GetCapacity();
  }

  for (auto &col : columns_) {
    col->SetFilteredTupleIdList(filter_, count);
  }
}

void VectorProjection::SetFilteredSelections(const TupleIdList &tid_list) {
  NOISEPAGE_ASSERT(tid_list.GetCapacity() == owned_tid_list_.GetCapacity(),
                   "Input TID list capacity doesn't match projection capacity");

  // Copy the input TID list.
  owned_tid_list_.AssignFrom(tid_list);

  // Let the child vectors know of the new list, if need be.
  RefreshFilteredTupleIdList();
}

void VectorProjection::CopySelectionsTo(TupleIdList *tid_list) const {
  tid_list->Resize(owned_tid_list_.GetCapacity());
  tid_list->AssignFrom(owned_tid_list_);
}

void VectorProjection::Reset(uint64_t num_tuples) {
  // Reset the cached TID list to NULL indicating all TIDs are active
  filter_ = nullptr;

  // Setup TID list to include all tuples
  owned_tid_list_.Resize(num_tuples);
  owned_tid_list_.AddAll();

  // If the projection is an owning projection, we need to reset each child
  // vector to point to its designated chunk of the internal buffer. If the
  // projection is a referencing projection, just notify each child vector of
  // its new size.

  if (owned_buffer_ != nullptr) {
    auto ptr = owned_buffer_.get();
    for (const auto &col : columns_) {
      col->ReferenceNullMask(ptr, &col->null_mask_, num_tuples);
      ptr += GetTypeIdSize(col->GetTypeId()) * common::Constants::K_DEFAULT_VECTOR_SIZE;
    }
  } else {
    for (auto &col : columns_) {
      col->Resize(num_tuples);
    }
  }

  tuple_slots_.resize(num_tuples);
}

void VectorProjection::Pack() {
  if (!IsFiltered()) {
    return;
  }

  filter_ = nullptr;
  owned_tid_list_.Resize(GetSelectedTupleCount());
  owned_tid_list_.AddAll();

  for (auto &col : columns_) {
    col->Pack();
  }
}

void VectorProjection::ProjectColumns(const std::vector<uint32_t> &cols, VectorProjection *result) const {
  std::vector<TypeId> schema(cols.size());
  for (uint32_t i = 0; i < cols.size(); i++) {
    schema[i] = GetColumn(cols[i])->GetTypeId();
  }

  // Create the resulting projection.
  result->InitializeEmpty(schema);

  // Create referencing vectors for each projected vector.
  for (uint32_t i = 0; i < cols.size(); i++) {
    result->GetColumn(i)->Reference(GetColumn(cols[i]));
  }

  // Copy active TIDs and refresh the result's filtration status.
  CopySelectionsTo(&result->owned_tid_list_);
  result->RefreshFilteredTupleIdList();
}

void VectorProjection::Hash(const std::vector<uint32_t> &cols, Vector *result) const {
  NOISEPAGE_ASSERT(!cols.empty(), "Must provide at least one column to hash.");
  VectorOps::Hash(*GetColumn(cols[0]), result);
  for (uint32_t i = 1; i < cols.size(); i++) {
    VectorOps::HashCombine(*GetColumn(cols[i]), result);
  }
}

void VectorProjection::Hash(Vector *result) const {
  std::vector<uint32_t> cols(GetColumnCount());
  std::iota(cols.begin(), cols.end(), 0);
  Hash(cols, result);
}

std::string VectorProjection::ToString() const {
  std::string result = "VectorProjection(#cols=" + std::to_string(columns_.size()) + "):\n";
  for (auto &col : columns_) {
    result += "- " + col->ToString() + "\n";
  }
  return result;
}

void VectorProjection::Dump(std::ostream &os) const { os << ToString() << std::endl; }

void VectorProjection::CheckIntegrity() const {
#ifndef NDEBUG
  // Check that the TID list size is sufficient for this vector projection
  NOISEPAGE_ASSERT(owned_tid_list_.GetCapacity() == GetTotalTupleCount(),
                   "TID list capacity doesn't match vector projection capacity!");

  // Check if the filtered TID list matches the owned list when filtered
  NOISEPAGE_ASSERT(!IsFiltered() || filter_ == &owned_tid_list_,
                   "Filtered list pointer doesn't match internal owned active TID list");

  // Check that all contained vectors have the same size and selection vector
  for (const auto &col : columns_) {
    NOISEPAGE_ASSERT(!IsFiltered() || filter_ == col->GetFilteredTupleIdList(),
                     "Vector in projection with different selection vector");
    NOISEPAGE_ASSERT(GetSelectedTupleCount() == col->GetCount(), "Vector size does not match rest of projection");
  }

  // Let the vectors do an integrity check
  for (const auto &col : columns_) {
    col->CheckIntegrity();
  }
#endif
}

}  // namespace noisepage::execution::sql
