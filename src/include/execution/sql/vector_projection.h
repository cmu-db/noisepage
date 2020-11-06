#pragma once

#include <iosfwd>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "common/macros.h"
#include "execution/sql/sql.h"
#include "execution/sql/tuple_id_list.h"
#include "execution/sql/vector.h"
#include "storage/storage_defs.h"

namespace noisepage::storage {
class BlockLayout;
}

namespace noisepage::execution::sql {

class ColumnVectorIterator;

/**
 * A container representing a collection of tuples whose attributes are stored in columnar format.
 * It's used in the execution engine to represent subsets of materialized state such partitions of
 * base tables and intermediate state including hash tables or sorter instances.
 *
 * Columns in the projection have a well-defined order and are accessed using this unchanging order.
 * All columns in the projection have the same size and selection count at any given time.
 *
 * In addition to holding all vector data, vector projections also contain a tuple ID (TID) list
 * containing the IDs of all tuples that are externally visible. Child column vectors hold
 * references to the TID list owned by this projection. At any given time, projections have a
 * selected count (see VectorProjection::GetSelectedTupleCount()) that is <= the total tuple count
 * (see VectorProjection::GetTotalTupleCount()). Users can manually set the selections by calling
 * VectorProjection::SetSelections() providing a tuple ID list. The provided list must have the same
 * shape (i.e., capacity) as the projection.
 *
 * VectorProjections come in two flavors: referencing and owning projections. A referencing vector
 * projection contains a set of column vectors that reference data stored externally. An owning
 * vector projection allocates and owns a chunk of data that it partitions and assigns to all child
 * vectors. By default, it will allocate enough data for each child to have a capacity determined by
 * the global constant DEFAULT_VECTOR_SIZE, usually 2048 elements. After construction, and
 * owning vector projection has a <b>zero</b> size (though it's capacity is
 * DEFAULT_VECTOR_SIZE). Thus, users must explicitly set the size through
 * VectorProjection::Resize() before interacting with the projection. Resizing sets up all contained
 * column vectors.
 *
 * To create a referencing vector, use VectorProjection::InitializeEmpty(), and fill each column
 * vector with VectorProjection::ResetColumn():
 * @code
 * VectorProjection vp;
 * vp.InitializeEmpty(...);
 * vp.ResetColumn(0, ...);
 * vp.ResetColumn(1, ...);
 * ...
 * @endcode
 *
 * To create an owning vector, use VectorProjection::Initialize():
 * @code
 * VectorProjection vp;
 * vp.Initialize(...);
 * vp.Resize(10);
 * VectorOps::Fill(vp.GetColumn(0), ...);
 * @endcode
 *
 * To remove any selection vector, call VectorProjection::Reset(). This returns the projection to
 * the a state as immediately following initialization.
 *
 * @code
 * VectorProjection vp;
 * vp.Initialize(...);
 * vp.Resize(10);
 * // vp size and selected is 10
 * vp.Reset();
 * // vp size and selected are 0
 * vp.Resize(20);
 * // vp size and selected is 20
 * @endcode
 */
class EXPORT VectorProjection {
  friend class VectorProjectionIterator;

 public:
  /**
   * A view into a row of the ProjectedColumns that has the almost same interface as a ProjectedRow.
   * This should only be used by DataTable::Scan.
   */
  class RowView {
   public:
    /**
     * @return number of columns stored in the ProjectedColumns
     */
    uint16_t NumColumns() const { return underlying_->GetColumnCount(); }

    /**
     * @return pointer to the start of the array of column ids
     */
    const std::vector<storage::col_id_t> &ColumnIds() const { return underlying_->ColumnIds(); }

    /**
     * Set the attribute in the row to be null using the internal bitmap
     * @param projection_list_index The 0-indexed element to access in this RowView
     */
    void SetNull(const uint16_t projection_list_index) {
      NOISEPAGE_ASSERT(projection_list_index < NumColumns(), "Column offset out of bounds.");
      underlying_->GetColumn(projection_list_index)->SetNull(row_offset_, true);
    }

    /**
     * Set the attribute in the row to be not null using the internal bitmap
     * @param projection_list_index The 0-indexed element to access in this RowView
     */
    void SetNotNull(const uint16_t projection_list_index) {
      NOISEPAGE_ASSERT(projection_list_index < NumColumns(), "Column offset out of bounds.");
      underlying_->GetColumn(projection_list_index)->SetNull(row_offset_, false);
    }

    /**
     * Check if the attribute in the RowView is null
     * @param projection_list_index The 0-indexed element to access in this RowView
     * @return true if null, false otherwise
     */
    bool IsNull(const uint16_t projection_list_index) const {
      NOISEPAGE_ASSERT(projection_list_index < NumColumns(), "Column offset out of bounds.");
      return underlying_->GetColumn(projection_list_index)->IsNull(row_offset_);
    }

    /**
     * Access a single attribute within the RowView with a check of the null bitmap first for nullable types
     * @param projection_list_index The 0-indexed element to access in this RowView
     * @return byte pointer to the attribute. reinterpret_cast and dereference to access the value. if attribute is
     * nullable and set to null, then return value is nullptr
     */
    byte *AccessWithNullCheck(const uint16_t projection_list_index) {
      NOISEPAGE_ASSERT(projection_list_index < NumColumns(), "Column offset out of bounds.");
      if (IsNull(projection_list_index)) return nullptr;
      return underlying_->GetColumn(projection_list_index)->GetValuePointer(row_offset_);
    }

    /**
     * Access a single attribute within the RowView with a check of the null bitmap first for nullable types
     * @param projection_list_index The 0-indexed element to access in this RowView
     * @return byte pointer to the attribute. reinterpret_cast and dereference to access the value. if attribute is
     * nullable and set to null, then return value is nullptr
     */
    const byte *AccessWithNullCheck(const uint16_t projection_list_index) const {
      NOISEPAGE_ASSERT(projection_list_index < NumColumns(), "Column offset out of bounds.");
      if (IsNull(projection_list_index)) return nullptr;
      return underlying_->GetColumn(projection_list_index)->GetValuePointer(row_offset_);
    }

    /**
     * Access a single attribute within the RowView without a check of the null bitmap first
     * @param projection_list_index The 0-indexed element to access in this RowView
     * @return byte pointer to the attribute. reinterpret_cast and dereference to access the value
     */
    byte *AccessForceNotNull(const uint16_t projection_list_index) {
      NOISEPAGE_ASSERT(projection_list_index < NumColumns(), "Column offset out of bounds.");
      if (IsNull(projection_list_index)) SetNotNull(projection_list_index);
      return underlying_->GetColumn(projection_list_index)->GetValuePointer(row_offset_);
    }

    /** Associate the current row offset with the provided tuple slot. */
    void SetTupleSlot(const storage::TupleSlot &slot) { underlying_->SetTupleSlot(slot, row_offset_); }

   private:
    friend class VectorProjection;
    RowView(VectorProjection *underlying, uint32_t row_offset) : underlying_(underlying), row_offset_(row_offset) {}
    VectorProjection *const underlying_;
    const uint32_t row_offset_;
  };

  /**
   * Create an empty and uninitialized vector projection. Users must call @em Initialize() or
   * @em InitializeEmpty() to appropriately initialize the projection with the correct columns.
   *
   * @see Initialize()
   * @see InitializeEmpty()
   */
  VectorProjection();

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY(VectorProjection);

  /**
   * Move constructor.
   */
  VectorProjection(VectorProjection &&other) = default;

  /**
   * Move assignment.
   * @param other The vector projection whose contents to move into this instance.
   * @return This vector projection instance.
   */
  VectorProjection &operator=(VectorProjection &&other) = default;

  /**
   * Set the storage col ids that correspond to the elements of this vector projection.
   */
  void SetStorageColIds(std::vector<storage::col_id_t> storage_col_ids);

  /**
   * Initialize a vector projection with column vectors of the provided types. This will create one
   * vector for each type provided in the column metadata list @em column_info. All vectors will
   * will be initialized with a maximum capacity of kDefaultVectorSize (e.g., 2048), are empty, and
   * will reference data owned by this vector projection.
   * @param col_types Metadata for columns in the projection.
   */
  void Initialize(const std::vector<TypeId> &col_types);

  /**
   * Initialize an empty vector projection with columns of the provided types. This will create an
   * empty vector of the specified type for each type provided in the column metadata list
   * @em column_info. Column vectors may only reference external data set and refreshed through
   * @em ResetColumn().
   *
   * @see VectorProjection::ResetColumn()
   *
   * @param col_types Metadata for columns in the projection.
   */
  void InitializeEmpty(const std::vector<TypeId> &col_types);

  /**
   * @return True if the projection has no tuples; false otherwise.
   */
  bool IsEmpty() const { return GetSelectedTupleCount() == 0; }

  /**
   * @return True if the projection is filtered; false otherwise.
   */
  bool IsFiltered() const { return filter_ != nullptr; }

  /**
   * @return The list of active TIDs in the projection; NULL if no tuples have been filtered out.
   */
  const TupleIdList *GetFilteredTupleIdList() { return filter_; }

  /**
   * Filter elements from the projection based on the tuple IDs in the input list @em tid_list.
   * @param tid_list The input TID list of valid tuples.
   */
  void SetFilteredSelections(const TupleIdList &tid_list);

  /**
   * Copy the full list of active TIDs in this projection into the provided TID list.
   * @param[out] tid_list The list where active TIDs in this projection are written to.
   */
  void CopySelectionsTo(TupleIdList *tid_list) const;

  /**
   * @return The metadata for the column at index @em col_idx in the projection.
   */
  TypeId GetColumnType(const uint32_t col_idx) const {
    NOISEPAGE_ASSERT(col_idx < GetColumnCount(), "Out-of-bounds column access");
    return GetColumn(col_idx)->GetTypeId();
  }

  /**
   * @return The column vector at index @em col_idx as it appears in the projection.
   */
  const Vector *GetColumn(const uint32_t col_idx) const {
    NOISEPAGE_ASSERT(col_idx < GetColumnCount(), "Out-of-bounds column access");
    return columns_[col_idx].get();
  }

  /**
   * @return The column vector at index @em col_idx as it appears in the projection.
   */
  Vector *GetColumn(const uint32_t col_idx) {
    NOISEPAGE_ASSERT(col_idx < GetColumnCount(), "Out-of-bounds column access");
    return columns_[col_idx].get();
  }

  /**
   * Reset the count of each child vector to @em num_tuples and reset the data pointer of each child
   * vector to point to their data chunk in this projection, if it owns any.
   * @param num_tuples The number of tuples that each child vector should now contain.
   */
  void Reset(uint64_t num_tuples);

  /**
   * Packing (or compressing) a projection rearranges contained vector data by contiguously storing
   * only active vector elements, removing any filtered TID list.
   */
  void Pack();

  /**
   * Project in the columns whose indexes are in the provided input vector into the provided result
   * vector projection. The output result projection is guaranteed to have the same shape (size and
   * filter status) as this projection, but all column vectors are **references** to this vector
   * projection. Any existing data in the output vector projection is cleaned up.
   * @param cols The indexes of the columns to project into the output projection.
   * @param[out] result The output vector projection.
   */
  void ProjectColumns(const std::vector<uint32_t> &cols, VectorProjection *result) const;

  /**
   * Hash the columns whose indexes are in @em cols and store the result in @em result.
   * @param cols The indexes of the columns to hash.
   * @param[out] result The output vector containing the result of the hash computation.
   */
  void Hash(const std::vector<uint32_t> &cols, Vector *result) const;

  /**
   * Hash all column data storing the result in @em result.
   * @param[out] result The output vector containing the result of the hash computation.
   */
  void Hash(Vector *result) const;

  /**
   * @return The number of columns in the projection.
   */
  uint32_t GetColumnCount() const { return columns_.size(); }

  /**
   * @return The number of active, i.e., externally visible, tuples in this projection. The selected
   *         tuple count is always <= the total tuple count.
   */
  uint64_t GetSelectedTupleCount() const { return columns_.empty() ? 0 : columns_[0]->GetCount(); }

  /**
   * @return The total number of tuples in the projection, including those that may have been
   *         filtered out by a selection vector, if one exists. The total tuple count is >= the
   *         selected tuple count.
   */
  uint64_t GetTotalTupleCount() const { return columns_.empty() ? 0 : columns_[0]->GetSize(); }

  /**
   * @return The maximum capacity of this projection. In other words, the maximum number of tuples
   *         the vectors constituting this projection can store.
   */
  uint64_t GetTupleCapacity() const { return columns_.empty() ? 0 : columns_[0]->GetCapacity(); }

  /**
   * @return The selectivity of this projection, i.e., the fraction of **total** tuples that have
   *         passed any filters (through the selection vector), and are externally visible. The
   *         selectivity is a floating-point number in the range [0.0, 1.0].
   */
  double ComputeSelectivity() const { return IsEmpty() ? 0 : owned_tid_list_.ComputeSelectivity(); }

  /**
   * Return a string representation of this vector projection.
   * @return A string representation of the projection's contents.
   */
  std::string ToString() const;

  /**
   * Print a string representation of this vector projection to the provided output stream.
   * @param os The stream where the string representation of this projection is written to.
   */
  void Dump(std::ostream &os) const;

  /**
   * Perform an integrity check on this vector projection instance. This is used in debug mode.
   */
  void CheckIntegrity() const;

  /**
   * @warning don't use these above the storage layer, they have no meaning
   * @return pointer to the start of the array of column ids
   */
  const std::vector<storage::col_id_t> &ColumnIds() const { return storage_col_ids_; }

  /**
   * Set the specified row offset to contain the given tuple slot.
   * TODO(WAN): how does this interact with filtering?
   * @param slot The tuple slot.
   * @param row_offset The row to set the tuple slot for.
   */
  void SetTupleSlot(const storage::TupleSlot &slot, uint32_t row_offset) { tuple_slots_[row_offset] = slot; }

  /** @return The tuple slot at the specified row offset. */
  storage::TupleSlot GetTupleSlot(uint32_t row_offset) { return tuple_slots_[row_offset]; }

 private:
  // Propagate the active TID list to child vectors, if necessary.
  void RefreshFilteredTupleIdList();

  friend class storage::DataTable;

  /**
   * Should only be used by storage::DataTable.
   * @param row_offset the row offset within the ProjectedColumns to look at
   * @return a view into the desired row within the ProjectedColumns
   */
  RowView InterpretAsRow(uint32_t row_offset) { return {this, row_offset}; }

  // Vector containing column data for all columns in this projection.
  std::vector<std::unique_ptr<Vector>> columns_;

  /** The storage column IDs. */
  std::vector<storage::col_id_t> storage_col_ids_;

  // The list of active TIDs in the projection. Non-null only when tuples have
  // been filtered out.
  const TupleIdList *filter_{nullptr};

  // The list of active TIDs in the projection. This is the source of truth and
  // will always represent the list of visible TIDs.
  TupleIdList owned_tid_list_;

  // If the vector projection allocates memory for all contained vectors, this
  // pointer owns that memory.
  std::unique_ptr<byte[]> owned_buffer_;

  // The tuple slots in this vector projection.
  std::vector<storage::TupleSlot> tuple_slots_;
};

}  // namespace noisepage::execution::sql
