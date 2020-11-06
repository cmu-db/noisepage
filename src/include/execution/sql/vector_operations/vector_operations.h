#pragma once

#include <algorithm>

#include "common/constants.h"
#include "execution/sql/generic_value.h"
#include "execution/sql/vector.h"

namespace noisepage::execution::exec {
class ExecutionSettings;
}  // namespace noisepage::execution::exec

namespace noisepage::execution::sql {

class TupleIdList;

/**
 * A utility class containing several core vectorized operations.
 */
class EXPORT VectorOps {
 public:
  /** This class cannot be instantiated. */
  DISALLOW_INSTANTIATION(VectorOps);
  /** This class cannot be copied or moved. */
  DISALLOW_COPY_AND_MOVE(VectorOps);

  /**
   * Copy @em element_count elements from @em source starting at offset @em offset into the (opaque)
   * array @em target.
   * @param source The source vector to copy from.
   * @param target The target vector to copy into.
   * @param offset The index into the source vector to begin copying from.
   * @param element_count The number of elements to copy.
   */
  static void Copy(const Vector &source, void *target, uint64_t offset = 0, uint64_t element_count = 0);

  /**
   * Copy all elements from @em source to the target vector @em target, starting at offset
   * @em offset in the source vector.
   * @param source The vector to copy from.
   * @param target The vector to copy into.
   * @param offset The offset in the source vector to begin reading.
   */
  static void Copy(const Vector &source, Vector *target, uint64_t offset = 0);

  /**
   * Cast all elements in the source vector @em source into elements of the type the target vector
   * @em target supports, and write them into the target vector.
   * @param exec_settings The execution settings.
   * @param source The vector to cast from.
   * @param target The vector to cast and write into.
   */
  static void Cast(const exec::ExecutionSettings &exec_settings, const Vector &source, Vector *target);

  /**
   * Cast all elements in the source vector @em source whose SQL type is @em source_type into the
   * target SQL type @em target_type and write the results into the target vector @em target.
   * @param exec_settings The execution settings.
   * @param source The vector to read from.
   * @param target The vector to write into.
   * @param source_type The SQL type of elements in the source vector.
   * @param target_type The SQL type of elements in the target vector.
   */
  static void Cast(const exec::ExecutionSettings &exec_settings, const Vector &source, Vector *target,
                   SqlTypeId source_type, SqlTypeId target_type);

  /**
   * Fill the input vector @em vector with sequentially increasing values beginning at @em start and
   * incrementing by @em increment.
   * @param vector The vector to fill.
   * @param start The first element to insert.
   * @param increment The amount to jump.
   */
  static void Generate(Vector *vector, int64_t start, int64_t increment);

  /**
   * Fill the input vector @em vector with a given non-null value @em value.
   * @param vector The vector to modify.
   * @param value The value to fill the vector with.
   */
  static void Fill(Vector *vector, const GenericValue &value);

  /**
   * Fill the input vector with NULL values.
   * @param vector The vector to modify.
   */
  static void FillNull(Vector *vector);

  // -------------------------------------------------------
  //
  // Projections
  //
  // -------------------------------------------------------

  /**
   * Add vector elements in @em left with @em right and store the result into @em result:
   *
   * result = left + right
   *
   * @param exec_settings The current execution settings
   * @param left The left input into the addition.
   * @param right The right input into the addition.
   * @param[out] result The result of the addition.
   */
  static void Add(const exec::ExecutionSettings &exec_settings, const Vector &left, const Vector &right,
                  Vector *result);

  /**
   * Subtract vector elements in @em right from @em left and store the result into @em result:
   *
   * result = left - right
   *
   * @param exec_settings The current execution settings
   * @param left The left input into the subtraction.
   * @param right The right input into the subtraction.
   * @param[out] result The result of the subtraction.
   */
  static void Subtract(const exec::ExecutionSettings &exec_settings, const Vector &right, Vector *result,
                       const Vector &left);

  /**
   * Multiply vector elements in @em left with @em right and store the result into @em result:
   *
   * result = left * right
   *
   * @param exec_settings The current execution settings
   * @param left The left input into the multiplication.
   * @param right The right input into the multiplication.
   * @param[out] result The result of the multiplication.
   */
  static void Multiply(const exec::ExecutionSettings &exec_settings, const Vector &left, const Vector &right,
                       Vector *result);

  /**
   * Divide vector elements in @em left by @em right and store the result into @em result:
   *
   * result = left / right
   *
   * @param exec_settings The current execution settings
   * @param left The left input into the division.
   * @param right The right input into the division.
   * @param[out] result The result of the division.
   */
  static void Divide(const exec::ExecutionSettings &exec_settings, const Vector &left, const Vector &right,
                     Vector *result);

  /**
   * Modulo vector elements in @em left by @em right and store the result into @em result:
   *
   * result = left % right
   *
   * @param left The left input into the modulus.
   * @param right The right input into the modulus.
   * @param[out] result The result of the modulus.
   */
  static void Modulo(const Vector &left, const Vector &right, Vector *result);

  /**
   * Add vector elements in @em left with @em right and store the result back into @em left:
   *
   * left += right
   *
   * @param exec_settings The execution settings.
   * @param[in,out] left The left input into the addition.
   * @param right The right input into the addition.
   */
  static void AddInPlace(const exec::ExecutionSettings &exec_settings, Vector *left, const Vector &right);

  /**
   * Bitwise AND elements in @em left with @em right and store the result back into @em left:
   *
   * left &= right
   *
   * @param exec_settings The execution settings.
   * @param[in,out] left The left input into the bitwise operation.
   * @param right The right input into the bitwise operation.
   */
  static void BitwiseAndInPlace(const exec::ExecutionSettings &exec_settings, Vector *left, const Vector &right);

  // -------------------------------------------------------
  //
  // Selections
  //
  // -------------------------------------------------------

  /**
   * Filter the TID list @em tid_list with all elements in @em left that are equal to elements in
   * @em right.
   * @param exec_settings The execution settings.
   * @param left The left input into the selection.
   * @param right The right input into the selection
   * @param[in,out] tid_list The list of TIDs to read and update.
   */
  static void SelectEqual(const exec::ExecutionSettings &exec_settings, const Vector &left, const Vector &right,
                          TupleIdList *tid_list);

  /**
   * Filter the TID list @em tid_list with all elements in @em left that are strictly greater than
   * elements in @em right.
   * @param exec_settings The execution settings.
   * @param left The left input into the selection.
   * @param right The right input into the selection
   * @param[in,out] tid_list The list of TIDs to read and update.
   */
  static void SelectGreaterThan(const exec::ExecutionSettings &exec_settings, const Vector &left, const Vector &right,
                                TupleIdList *tid_list);

  /**
   * Filter the TID list @em tid_list with all elements in @em left that are greater than or equal
   * to elements @em right.
   * @param exec_settings The execution settings.
   * @param left The left input into the selection.
   * @param right The right input into the selection
   * @param[in,out] tid_list The list of TIDs to read and update.
   */
  static void SelectGreaterThanEqual(const exec::ExecutionSettings &exec_settings, const Vector &left,
                                     const Vector &right, TupleIdList *tid_list);

  /**
   * Filter the TID list @em tid_list with all elements in @em left that are strictly less than
   * elements in @em right.
   * @param exec_settings The execution settings.
   * @param left The left input into the selection.
   * @param right The right input into the selection
   * @param[in,out] tid_list The list of TIDs to read and update.
   */
  static void SelectLessThan(const exec::ExecutionSettings &exec_settings, const Vector &left, const Vector &right,
                             TupleIdList *tid_list);

  /**
   * Filter the TID list @em tid_list with all elements in @em left that are less than or equal to
   * elements in @em right.
   * @param exec_settings The execution settings.
   * @param left The left input into the selection.
   * @param right The right input into the selection
   * @param[in,out] tid_list The list of TIDs to read and update.
   */
  static void SelectLessThanEqual(const exec::ExecutionSettings &exec_settings, const Vector &left, const Vector &right,
                                  TupleIdList *tid_list);

  /**
   * Filter the TID list @em tid_list with all elements in @em left that are not equal to elements
   * in @em right.
   * @param exec_settings The execution settings.
   * @param left The left input into the selection.
   * @param right The right input into the selection
   * @param[in,out] tid_list The list of TIDs to read and update.
   */
  static void SelectNotEqual(const exec::ExecutionSettings &exec_settings, const Vector &left, const Vector &right,
                             TupleIdList *tid_list);

  // -------------------------------------------------------
  //
  // NULL check operations
  //
  // -------------------------------------------------------

  /**
   * Check TIDs in the list @em tid_list are NULL in the input vector @em input, storing the result
   * back into @em tid_list.
   * @param input The input vector whose elements are checked.
   * @param[in,out] tid_list The list of TIDs to check, and the output of the check.
   */
  static void IsNull(const Vector &input, TupleIdList *tid_list);

  /**
   * Check TIDs in the list @em tid_list are NOT NULL in the input vector @em input, storing the
   * result back into @em tid_list.
   * @param input The input vector whose elements are checked.
   * @param[in,out] tid_list The list of TIDs to check, and the output of the check.
   */
  static void IsNotNull(const Vector &input, TupleIdList *tid_list);

  // -------------------------------------------------------
  //
  // String operations
  //
  // -------------------------------------------------------

  /**
   * Store the TIDs of all string elements in @em a that are LIKE their counterparts in @em b in the
   * output Tuple ID list @em tid_list. Only elements whose TIDs appear in @em tid_list are
   * read and processed.
   * @param exec_settings The execution settings.
   * @param a The vector of strings to compare.
   * @param b The vector of strings to compare with.
   * @param[in,out] tid_list The list of TIDs to check, and the output of the check.
   */
  static void SelectLike(const exec::ExecutionSettings &exec_settings, const Vector &a, const Vector &b,
                         TupleIdList *tid_list);

  /**
   * Store the TIDs of all string elements in @em a that are NOT LIKE their counterparts in @em b in
   * the output Tuple ID list @em tid_list. Only elements whose TIDs appear in @em tid_list are
   * read and processed.
   * @param exec_settings The execution settings.
   * @param a The vector of strings to compare.
   * @param b The vector of strings to compare with.
   * @param[in,out] tid_list The list of TIDs to check, and the output of the check.
   */
  static void SelectNotLike(const exec::ExecutionSettings &exec_settings, const Vector &a, const Vector &b,
                            TupleIdList *tid_list);

  // -------------------------------------------------------
  //
  // Hashing
  //
  // -------------------------------------------------------

  /**
   * Hash vector elements from @em input into @em result.
   * @param input The input to hash.
   * @param[out] result The vector where hash results are stored.
   */
  static void Hash(const Vector &input, Vector *result);

  /**
   * Like VectorOps::Hash(), but combines the hashes stored in @em result with the computed hashes
   * of vector elements in @em input. The hashes in @em result serve as seed hashes.
   * @param input The input to hash.
   * @param[out] result The vector of seed hash values combined with the input; also stores result.
   */
  static void HashCombine(const Vector &input, Vector *result);

  // -------------------------------------------------------
  //
  // Gather / Scatter
  //
  // -------------------------------------------------------

  /**
   * Read the values pointed to by elements in the provided pointers vector into the result vector.
   * A byte-offset value is added to each pointer element before de-referencing the pointer. NULL
   * pointer elements are skipped, and the NULL bit is set in the result.
   * @param pointers The vector of pointers to read.
   * @param[out] result The vector containing the values read from de-referencing each pointer.
   * @param offset The byte offset to apply to each pointer before it is de-referenced.
   */
  static void Gather(const Vector &pointers, Vector *result, std::size_t offset);

  /**
   * Perform a fused gather+select operation. Filter elements in @em tid_list where elements in
   * @em input equal elements pointed to by @em pointers after applying the provided byte offset.
   * @param input The input elements to compare with.
   * @param pointers The vector of pointers.
   * @param offset The byte offset to apply to each valid pointer element.
   * @param[in,out] tid_list The list of TIDs to read and update.
   */
  static void GatherAndSelectEqual(const Vector &input, const Vector &pointers, std::size_t offset,
                                   TupleIdList *tid_list);

  /**
   * Perform a fused gather+select operation. Filter elements in @em tid_list where elements in
   * @em input are greater than elements pointed to by @em pointers after applying the provided byte
   * offset.
   * @param input The input elements to compare with.
   * @param pointers The vector of pointers.
   * @param offset The byte offset to apply to each valid pointer element.
   * @param[in,out] tid_list The list of TIDs to read and update.
   */
  static void GatherAndSelectGreaterThan(const Vector &input, const Vector &pointers, std::size_t offset,
                                         TupleIdList *tid_list);

  /**
   * Perform a fused gather+select operation. Filter elements in @em tid_list where elements in
   * @em input are greater than or equal to elements pointed to by @em pointers after applying the
   * provided byte offset.
   * @param input The input elements to compare with.
   * @param pointers The vector of pointers.
   * @param offset The byte offset to apply to each valid pointer element.
   * @param[in,out] tid_list The list of TIDs to read and update.
   */
  static void GatherAndSelectGreaterThanEqual(const Vector &input, const Vector &pointers, std::size_t offset,
                                              TupleIdList *tid_list);

  /**
   * Perform a fused gather+select operation. Filter elements in @em tid_list where elements in
   * @em input are less than elements pointed to by @em pointers after applying the provided byte
   * offset.
   * @param input The input elements to compare with.
   * @param pointers The vector of pointers.
   * @param offset The byte offset to apply to each valid pointer element.
   * @param[in,out] tid_list The list of TIDs to read and update.
   */
  static void GatherAndSelectLessThan(const Vector &input, const Vector &pointers, std::size_t offset,
                                      TupleIdList *tid_list);

  /**
   * Perform a fused gather+select operation. Filter elements in @em tid_list where elements in
   * @em input are less than or equal to elements pointed to by @em pointers after applying the
   * provided byte offset.
   * @param input The input elements to compare with.
   * @param pointers The vector of pointers.
   * @param offset The byte offset to apply to each valid pointer element.
   * @param[in,out] tid_list The list of TIDs to read and update.
   */
  static void GatherAndSelectLessThanEqual(const Vector &input, const Vector &pointers, std::size_t offset,
                                           TupleIdList *tid_list);

  /**
   * Perform a fused gather+select operation. Filter elements in @em tid_list where elements in
   * @em input are not equal to elements pointed to by @em pointers after applying the provided byte
   * offset.
   * @param input The input elements to compare with.
   * @param pointers The vector of pointers.
   * @param offset The byte offset to apply to each valid pointer element.
   * @param[in,out] tid_list The list of TIDs to read and update.
   */
  static void GatherAndSelectNotEqual(const Vector &input, const Vector &pointers, std::size_t offset,
                                      TupleIdList *tid_list);

  // -------------------------------------------------------
  //
  // Sort-ish
  //
  // -------------------------------------------------------

  /**
   * Sort the input vector and store the resulting re-ordering selection index vector in @em result.
   *
   * @param input The vector to sort.
   * @param[out] result The output result vector.
   */
  static void Sort(const Vector &input, sel_t result[]);

  // -------------------------------------------------------
  //
  // Vector Iteration Logic
  //
  // -------------------------------------------------------

  /**
   * Apply a function to active elements in the input vector @em vector. If the vector has a
   * filtered TID list, the callback @em f is only applied to TIDs in this list, but in the range
   * [offset,count). For example, if the TID list is [0,2,4,6,8,10], offset is 2, and count is 5,
   * the callback @em f will receive the argument pairs [(4,0),(6,1),(8,2)].
   *
   * If input vector is unfiltered, the callback @em f is applied to all TIDs in the range
   * [offset, count).
   *
   * The callback function receives two arguments:
   * i = the current index from the selection vector.
   * k = position in TID list.
   *
   * @tparam F Functor accepting two integer arguments.
   * @param vector The vector to iterate.
   * @param f The function to call on each element.
   * @param offset The (optional) offset from the beginning to begin iteration.
   * @param count The (optional) count indicating the end of the iteration range. A zero count
   *              implies scanning to end of the vector.
   */
  template <typename F>
  static void Exec(const Vector &vector, F &&f, uint64_t offset = 0, uint64_t count = 0) {
    if (count == 0) {
      count = vector.GetCount();
    } else {
      count += offset;
    }

    const TupleIdList *tid_list = vector.GetFilteredTupleIdList();

    if (tid_list != nullptr) {
      // If we're scanning all TIDs in the list, it's faster to use ForEach().
      // If we're scanning only a subset range of TIDs in the list, we convert
      // the list into a selection vector.
      // TODO(pmenon): Pull optimization into TupleIdList ?

      if (offset == 0 && count == vector.GetCount()) {
        uint64_t k = 0;
        tid_list->ForEach([&](const uint64_t i) { f(i, k++); });
      } else {
        NOISEPAGE_ASSERT(tid_list->GetCapacity() <= common::Constants::K_DEFAULT_VECTOR_SIZE, "TID list too large");
        alignas(common::Constants::CACHELINE_SIZE) sel_t sel_vector[common::Constants::K_DEFAULT_VECTOR_SIZE];
        UNUSED_ATTRIBUTE uint64_t size = tid_list->ToSelectionVector(sel_vector);
        for (uint64_t i = offset; i < count; i++) {
          f(sel_vector[i], i);
        }
      }
    } else {
      for (uint64_t i = offset; i < count; i++) {
        f(i, i);
      }
    }
  }

  /**
   * Apply a function to all TIDs in the input vector, bypassing any filtered TID list.
   *
   * The callback function receives two arguments:
   * i = the current index from the selection vector.
   * k = position in TID list.
   *
   * @tparam F Functor accepting two integer arguments.
   * @param vector The vector to iterate.
   * @param f The function to call on each element.
   */
  template <typename F>
  static void ExecIgnoreFilter(const Vector &vector, F &&f) {
    const uint64_t count = vector.GetSize();
    for (uint64_t i = 0; i < count; i++) {
      f(i, i);
    }
  }

  /**
   * Apply a function to all active elements in the input vector assuming elements have the
   * templated C++ type. Each element is passed by const-reference.
   *
   * The callback function receives three arguments:
   * v = a const-reference to the element at the current TID.
   * i = the current tuple ID.
   * k = position in TID list.
   *
   * @tparam T The type to cast each vector element into.
   * @tparam F Functor accepting three arguments outlined in the function description.
   * @param vector The vector whose contents to iterate over.
   * @param f The callback function invoked for each active vector element.
   */
  template <typename T, typename F>
  static void ExecTyped(const Vector &vector, F &&f) {
    const auto *RESTRICT data = reinterpret_cast<const T *>(vector.GetData());
    Exec(vector, [&](const uint64_t i, const uint64_t k) { f(data[i], i, k); });
  }
};

}  // namespace noisepage::execution::sql
