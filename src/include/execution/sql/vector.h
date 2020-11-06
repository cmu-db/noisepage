#pragma once

#include <iosfwd>
#include <memory>
#include <string>
#include <utility>

#include "common/constants.h"
#include "common/macros.h"
#include "execution/sql/generic_value.h"
#include "execution/sql/sql.h"
#include "execution/sql/tuple_id_list.h"
#include "execution/util/bit_vector.h"
#include "execution/util/string_heap.h"

namespace noisepage::execution::exec {
class ExecutionSettings;
}

namespace noisepage::execution::sql {

/**
 * A Vector represents a contiguous chunk of values of a single type. A vector may allocate and own
 * its data, or <b>reference</b> data owned by some other entity, e.g., base table column data, data
 * within another vector, or a constant value.
 *
 * All Vectors have a maximum capacity (see Vector::GetCapacity()) determined by the global constant
 * DEFAULT_VECTOR_SIZE usually set to 2048 elements. Vectors also have a <b>size</b> (see
 * Vector::GetSize()) that reflects the number of physically contiguous elements <b>currently</b> in
 * the vector. A vector's size can fluctuate through its life, but will always be less than its
 * capacity. Finally, a vector has an <b>active count</b> (see Vector::GetCount()) that represents
 * the number of externally visible elements. Elements may become inactive if they have been
 * filtered out through predicates. The visibility of elements in the vector is controlled through
 * a <b>tuple ID (TID) list</b>.
 *
 * Vectors can be logically filtered through a TID list. A TID list is an array containing the
 * indexes of the <i>active</i> vector elements. If a non-NULL filtered TID list is available from
 * Vector::GetFilteredTupleIdList(), it must be used to access the vector's data since the vector
 * may hold otherwise invalid data in unselected positions (e.g., null pointers). This functionality
 * is provided for you through VectorOps::Exec(), but can be done manually as the below example
 * illustrates:
 *
 * @code
 * Vector vec = ...
 * auto *tids = vec.GetFilteredTupleIdList();
 * uint64_t x = 0;
 * for (auto i : *tids) {
 *   x += vec.GetData()[i];
 * }
 * @endcode
 *
 * The TID list is used primarily to activate and deactivate elements in the vector without having
 * to copy or move data. Each vector maintains the invariant: active count <= size <= capacity.
 * If there is no filtering TID list, the active element count and size will match. Otherwise, the
 * active element count equals the size (i.e., number of TIDs in) the filtering TID list.
 *
 * <h2>Usage:</h2>
 *
 * <h3>Referencing-vectors</h3>
 * Referencing-vectors do not allocate any memory and can only reference externally-owned data. To
 * create a referencing-vector, create a Vector of the appropriate type followed by a call to
 * Vector::Reference() with the raw data as shown below:
 * @code
 * int16_t data[] = {0,2,4,6,8};          // data = [0,2,4,6,8]
 * auto nulls = [0,1,0,1,0]               // nulls = [false,true,false,true,false]
 * Vector vec(TypeId::SmallInt);          // vec = []
 * vec.Reference(data, null_mask, size);  // vec = [0,NULL,4,NULL,8]
 * @endcode
 *
 * When creating a referencing-vector, the underlying data is not allowed to be NULL. The NULL bit
 * mask, however, can be NULL to indicate that <b>no</b> elements are NULL. The <i>size</i>
 * attribute indicates the number of physically contiguous elements in the vector, and must be
 * smaller than the maximum capacity of the vector. After creation, the count and size are equal.
 *
 * Referencing-vectors enable users to lift externally stored data into the vector-processing
 * ecosystem; the rich library of vector operations in tpl::sql::VectorOps can be executed on native
 * arrays.
 *
 * <h3>Owning-vectors</h3>
 * Owning vectors, as their name implies, create and own the underlying vector data upon creation.
 * Owning vectors are created empty (i.e., with a size of 0) and must be explicitly resized after
 * construction:
 * @code
 * Vector vec(TypeId::Double, true, true);  // vec = []
 * vec.Resize(100);                         // vec = [0,0,0,0...]
 * vec.SetNull(10, true);                   // vec = [0,0,0,0,0,0,0,0,0,NULL,0,...]
 * // ...
 * @endcode
 *
 * <h3>Caution:</h3>
 *
 * While there are methods to get/set individual vector elements, this should be used sparingly. If
 * you find yourself invoking this is in a hot-loop, or very often, reconsider your interaction
 * pattern with Vector, and think about writing a new vector primitive to achieve your objective.
 */
class EXPORT Vector {
  friend class VectorOps;
  friend class VectorProjectionIterator;

 public:
  /** The null mask for the vector indicates which entries are NULL. */
  using NullMask = util::BitVector<uint64_t>;

  /**
   * Scope object that temporary sets the filter for a vector over the lifetime of the scope. When
   * the object goes out of scope, the input vector's previous filter status is restored.
   */
  class TempFilterScope {
   public:
    /**
     * Create a new temporary filter scope.
     * @param vector The vector to be filtered.
     * @param tid_list The filtered tuple ID list for the vector.
     * @param count The number of active elements.
     */
    TempFilterScope(Vector *vector, const TupleIdList *tid_list, const uint64_t count)
        : vector_(vector), prev_tid_list_(vector->GetFilteredTupleIdList()), prev_count_(vector->GetCount()) {
      vector_->SetFilteredTupleIdList(tid_list, count);
    }

    /** Restore the previous state of the vector prior to filtering. */
    ~TempFilterScope() { vector_->SetFilteredTupleIdList(prev_tid_list_, prev_count_); }

   private:
    /** The vector to filter. */
    Vector *vector_;
    /** The previous filter in the vector. Can be NULL. */
    const TupleIdList *prev_tid_list_;
    /** The previous count of the vector. */
    uint64_t prev_count_;
  };

  /**
   * Create an empty vector.
   * @param type The type of the elements in the vector.
   */
  explicit Vector(TypeId type);

  /**
   * Create a new owning vector with a maximum capacity of kDefaultVectorSize. If @em create_data
   * is set, the vector will allocate memory for the vector's contents. If @em clear is set, the
   * memory will be zeroed out after allocation.
   * @param type The primitive type ID of the elements in this vector.
   * @param create_data Should the vector allocate space for the contents?
   * @param clear Should the vector zero the data if it allocates any?
   */
  Vector(TypeId type, bool create_data, bool clear);

  /**
   * Vector's cannot be implicitly copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(Vector);

  /**
   * Destructor.
   */
  ~Vector();

  /**
   * @return The type of the elements contained in this vector.
   */
  TypeId GetTypeId() const noexcept { return type_; }

  /**
   * @return The number of active, i.e., externally visible, elements in the vector. Active elements
   *         are those that have survived any filters in the selection vector. The count of a vector
   *         is guaranteed to be <= the size of the vector.
   */
  uint64_t GetCount() const noexcept { return count_; }

  /**
   * @return The total number of tuples currently in the vector, including those that may have been
   *         filtered out by the selection vector, if one exists. The size of a vector is always
   *         greater than or equal to the selected count.
   */
  uint64_t GetSize() const noexcept { return num_elements_; }

  /**
   * @return The maximum capacity of this vector.
   */
  uint64_t GetCapacity() const noexcept { return common::Constants::K_DEFAULT_VECTOR_SIZE; }

  /**
   * @return The raw untyped data pointer.
   */
  byte *GetData() const noexcept { return data_; }

  /**
   * @return The list of active TIDs in the vector. If all TIDs are visible, the list is NULL.
   */
  const TupleIdList *GetFilteredTupleIdList() const noexcept { return tid_list_; }

  /**
   * @return An immutable view of this vector's NULL indication bit mask.
   */
  const NullMask &GetNullMask() const noexcept { return null_mask_; }

  /**
   * @return A mutable pointer to this vector's NULL indication bit mask.
   */
  NullMask *GetMutableNullMask() noexcept { return &null_mask_; }

  /**
   * @return A mutable pointer to this vector's string heap.
   */
  VarlenHeap *GetMutableStringHeap() noexcept { return &varlen_heap_; }

  /**
   * Set the (optional) list of filtered TIDs in the vector and the new count of vector. A null
   * @em tid_list indicates that the vector is unfiltered in which case @em count must match the
   * current vector size. A non-null TID list contains the TIDs of active vector elements, and
   * @em count must match the size of the TID list.
   *
   * Note: For non-null input TID lists, we don't necessarily need the count since we can derive it
   *       using TupleIdList::GetTupleCount(). We don't do that here for performance. This method is
   *       called either during vector operations which simply propagate the TID list by reference,
   *       or through a VectorProjection. In the former case, the cached count value in the source
   *       vector is also propagated. In the latter case, the count is computed once for the
   *       projection and sent to each child vector.
   *
   * TODO(pmenon): Is the above optimization valid?
   *
   * @param tid_list The list of active TIDs in the vector.
   * @param count The number of elements in the selection vector.
   */
  void SetFilteredTupleIdList(const TupleIdList *tid_list, const uint64_t count) {
    NOISEPAGE_ASSERT(tid_list == nullptr || tid_list->GetCapacity() == num_elements_,
                     "TID list too small to capture all vector elements");
    NOISEPAGE_ASSERT(tid_list == nullptr || tid_list->GetTupleCount() == count, "TID list size and count do not match");
    NOISEPAGE_ASSERT(count <= num_elements_, "TID list count must be smaller than vector size");
    tid_list_ = tid_list;
    count_ = count;
  }

  /**
   * @return True if this vector is holding a single constant value; false otherwise.
   */
  bool IsConstant() const noexcept { return num_elements_ == 1 && tid_list_ == nullptr; }

  /**
   * @return True if this vector is empty; false otherwise.
   */
  bool IsEmpty() const noexcept { return num_elements_ == 0; }

  /**
   * @return The computed selectivity of this vector, i.e., the fraction of tuples that are
   *         externally visible.
   */
  float ComputeSelectivity() const noexcept { return IsEmpty() ? 0 : static_cast<float>(count_) / num_elements_; }

  /**
   * @return True if the value at index @em index is NULL; false otherwise.
   */
  bool IsNull(const uint64_t index) const {
    // TODO(WAN): poke Prashanth to see if this makes sense
    return index >= GetCount() ? false : (null_mask_[tid_list_ != nullptr ? (*tid_list_)[index] : index]);
  }

  /**
   * Set the value at position @em index to @em null.
   * @param index The index of the element to modify.
   * @param null Whether the element is NULL.
   */
  void SetNull(const uint64_t index, const bool null) {
    null_mask_[tid_list_ != nullptr ? (*tid_list_)[index] : index] = null;
  }

  /**
   * Returns the value of the element at the given position in the vector.
   *
   * NOTE: This shouldn't be used in performance-critical code. It's mostly for debugging and
   * validity checks, or for read-once-per-vector type operations.
   *
   * @param index The position in the vector to read.
   * @return The element at the specified position.
   */
  GenericValue GetValue(uint64_t index) const;

  /**
   * Set the value at position @em index in the vector to the value @em value.
   *
   * NOTE: This shouldn't be used in performance-critical code. It's mostly for debugging and
   * validity checks, or for read-once-per-vector type operations.
   *
   * @param index The (zero-based) index in the element to modify.
   * @param val The value to set the element to.
   */
  void SetValue(uint64_t index, const GenericValue &val);

  /**
   * Resize the vector to the given size. Resizing REMOVES any existing selection vector, reverts
   * the selected count and total count to the provided size.
   *
   * @pre The new size must be less than the capacity.
   *
   * @param size The size to set the vector to.
   */
  void Resize(uint32_t size);

  /**
   * Cast this vector to a different type. If the target type is the same as the current type,
   * nothing is done.
   * @param exec_settings The execution settings for this query.
   * @param new_type The type to cast this vector into.
   */
  void Cast(const exec::ExecutionSettings &exec_settings, TypeId new_type);

  /**
   * Append the contents of the provided vector @em other into this vector.
   * @param other The vector whose contents will be copied and appended to the end of this vector.
   */
  void Append(const Vector &other);

  /**
   * Create a clone of this vector into the target vector. The clone will have a copy of all data
   * and will retain the same shape of this vector.
   * @param[out] target Where the clone is stored.
   */
  void Clone(Vector *target);

  /**
   * Copies the ACTIVE vector elements in this vector into another vector. Vectors storing complex
   * objects will invoke copy constructors of these objects when creating new instances in the
   * target vector.Callers can optionally specify at what offset to begin copying at. The default
   * offset is 0.
   * @param other The vector to copy into.
   * @param offset The offset in this vector to begin copying.
   */
  void CopyTo(Vector *other, uint64_t offset = 0);

  /**
   * Move the data from this vector into another vector, and empty initialize this vector.
   * @param other The vector that will take ownership of all our data, if any.
   */
  void MoveTo(Vector *other);

  /**
   * Reference a single value.
   * @param value The value to reference.
   */
  void Reference(GenericValue *value);

  /**
   * Reference a specific chunk of data.
   * @param data The data.
   * @param null_mask The NULL bitmap.
   * @param size The number of elements in the array.
   */
  void Reference(byte *data, const uint32_t *null_mask, uint64_t size);

  /**
   * Reference a specific chunk of data.
   * @param data The data.
   * @param null_mask The NULL bitmap.
   * @param size The number of elements in the array.
   */
  void ReferenceNullMask(byte *data, const NullMask *null_mask, uint64_t size);

  /**
   * Change this vector to reference data held (and potentially owned) by the provided vector.
   * @param other The vector to reference.
   */
  void Reference(const Vector *other);

  /**
   * Packing (or compressing) a vector rearranges this vector's contents by contiguously storing
   * all active vector elements, removing any TID filter list.
   */
  void Pack();

  /**
   * Populate the input TID lists with the TIDs of all non-NULL active vector elements and active
   * NULL vector elements, respectively.
   * @param[out] non_null_tids The list of non-NULL TIDs in this vector.
   * @param[out] null_tids The list of NULL TIDs in this vector.
   */
  void GetNonNullSelections(TupleIdList *non_null_tids, TupleIdList *null_tids) const;

  /**
   * Return a string representation of this vector.
   * @return A string representation of the vector's contents.
   */
  std::string ToString() const;

  /**
   * Print a string representation of this vector to the output stream.
   * @param os The stream where the string representation of this vector is written to.
   */
  void Dump(std::ostream &os) const;

  /**
   * Perform an integrity check on this vector. This is used in debug mode for sanity checks.
   */
  void CheckIntegrity() const;

 private:
  // Create a new vector with the provided type. Any existing data is destroyed.
  void Initialize(TypeId new_type, bool clear);

  // Destroy the vector, delete any owned data, and reset it to an empty vector.
  void Destroy();

  friend class VectorProjection;
  byte *GetValuePointer(uint64_t index) { return GetData() + index * GetTypeIdSize(type_); }

 private:
  // The type of the elements stored in the vector.
  TypeId type_;

  // The number of elements in the vector.
  uint64_t count_;

  // The number of physically contiguous elements in the vector.
  uint64_t num_elements_;

  // A pointer to the data.
  byte *data_;

  // The list of active tuple IDs in the vector. If all TIDs are active, the
  // list is NOT used and will be NULL.
  const TupleIdList *tid_list_;

  // The null mask used to indicate if an element in the vector is NULL.
  NullMask null_mask_;

  // Heap container for strings owned by this vector.
  VarlenHeap varlen_heap_;

  // If the vector holds allocated data, this field manages it.
  std::unique_ptr<byte[]> owned_data_;
};

}  // namespace noisepage::execution::sql
