#pragma once

#include <immintrin.h>

#include <algorithm>
#include <limits>
#include <memory>
#include <string>
#include <type_traits>
#include <vector>

#include "common/constants.h"
#include "common/math_util.h"
#include "execution/util/bit_util.h"
#include "execution/util/vector_util.h"

namespace noisepage::execution::util {

/**
 * A BitVector represents a set of bits. It provides access to individual bits through
 * BitVector::operator[], along with a collection of operations commonly performed on bit vectors
 * such as intersection (operator&), union (operator|), and difference (operator-).
 *
 * There are a few important differences between BitVector and std::bitset<>:
 * 1. The size of a BitVector is provided at run-time during construction and is resizable over its
 *    lifetime, whereas the size of a std::bitset<> is a compile-time constant provided through a
 *    template argument.
 * 2. BitVectors allow using wider underlying "storage blocks" as the unit of bit storage. For
 *    sparse bit vectors, this enables faster iteration while not compromising on dense vectors.
 * 3. BitVectors support bulk-update features that can leverage SIMD instructions.
 * 4. BitVectors allow faster conversion to selection index vectors.
 *
 * @tparam WordType An unsigned integral type where the bits of the bit vector are stored.
 */
template <typename WordType = uint64_t, typename Allocator = std::allocator<WordType>>
class BitVector {
  static_assert(std::is_integral_v<WordType> && std::is_unsigned_v<WordType>,
                "Template type 'Word' must be an unsigned integral");

  // The size of a word (in bytes) used to store a contiguous set of bits. This
  // is the smallest granularity we store bits at.
  static constexpr uint32_t WORD_SIZE_BYTES = sizeof(WordType);

  // The size of a word in bits.
  static constexpr uint32_t WORD_SIZE_BITS = WORD_SIZE_BYTES * common::Constants::K_BITS_PER_BYTE;

  // Word value with all ones.
  static constexpr WordType ALL_ONES_WORD = ~static_cast<WordType>(0);

  // Ensure the size is a power of two so all the division and modulo math we do
  // is optimized into bit shifts.
  static_assert(common::MathUtil::IsPowerOf2(WORD_SIZE_BITS), "Word size in bits expected to be a power of two");

 public:
  /** Used to indicate an invalid bit position. */
  static constexpr const uint32_t INVALID_POS = std::numeric_limits<uint32_t>::max();

  /**
   * Return the number of words required to store at least @em num_bits number if bits in a bit
   * vector. Note that this may potentially over allocate.
   * @param num_bits The number of bits.
   * @return The number of words required to store the given number of bits.
   */
  static constexpr uint32_t NumNeededWords(uint32_t num_bits) {
    return common::MathUtil::DivRoundUp(num_bits, WORD_SIZE_BITS);
  }

  /**
   * Abstracts a reference to one bit in the bit vector.
   */
  class BitReference {
   public:
    /**
     * Test the value of the bit.
     * @return True if the bit is 1; false otherwise.
     */
    operator bool() const noexcept { return ((*word_) & mask_) != 0; }  // NOLINT

    /**
     * Set the value of this bit to @em val. Set to 1 if @em val is true; 0 otherwise.
     * @param val The value to assign the bit.
     * @return This bit.
     */
    BitReference &operator=(bool val) noexcept {
      Assign(val);
      return *this;
    }

    /**
     * Set the value of this bit to the provided bit.
     * @param that The bit to assign from.
     * @return This bit.
     */
    BitReference &operator=(const BitReference &that) noexcept {
      if (this == &that) {
        return *this;
      }
      Assign(that);
      return *this;
    }

   private:
    friend class BitVector<WordType>;

    // Create a reference to the given bit position in the word array. Only bit
    // vectors can create references to the bits they own.
    BitReference(WordType *word, uint32_t bit_pos) : word_(word), mask_(WordType(1) << bit_pos) {}

    // Assign this bit to the given value
    void Assign(const bool val) noexcept {
      // Turns out that using an explicit-if here is faster than a branch-free
      // implementation:
      // (*word_) ^= (-static_cast<WordType>(val) ^ *word_) & mask_;

      if (val) {
        (*word_) |= mask_;
      } else {
        (*word_) &= ~mask_;
      }
    }

   private:
    WordType *word_;
    WordType mask_;
  };

  /**
   * Create an empty bit vector. Users must call Resize() before interacting with it.
   * @ref BitVector::Resize()
   */
  explicit BitVector(Allocator allocator = Allocator()) : words_(allocator) {}

  /**
   * Create a new bit vector with the specified number of bits, all initially unset.
   * @param num_bits The number of bits in the vector.
   * @param allocator The allocator to use.
   */
  explicit BitVector(uint32_t num_bits, Allocator allocator = Allocator())
      : num_bits_(num_bits), words_(NumNeededWords(num_bits), WordType(0), allocator) {
    NOISEPAGE_ASSERT(num_bits_ > 0, "Cannot create bit vector with zero bits");
  }

  /**
   * @return True if the bit at the provided position is set; false otherwise.
   */
  bool Test(const uint32_t position) const {
    NOISEPAGE_ASSERT(position < GetNumBits(), "Index out of range");
    const WordType mask = WordType(1) << (position % WORD_SIZE_BITS);
    return words_[position / WORD_SIZE_BITS] & mask;
  }

  /**
   * Blindly set the bit at the given index to 1.
   * @param position The index of the bit to set.
   * @return This bit vector.
   */
  BitVector &Set(const uint32_t position) {
    NOISEPAGE_ASSERT(position < GetNumBits(), "Index out of range");
    words_[position / WORD_SIZE_BITS] |= WordType(1) << (position % WORD_SIZE_BITS);
    return *this;
  }

  /**
   * Set the bit at the given position to a given value.
   * @param position The index of the bit to set.
   * @param v The value to set the bit to.
   * @return This bit vector.
   */
  BitVector &Set(const uint32_t position, const bool v) {
    NOISEPAGE_ASSERT(position < GetNumBits(), "Index out of range");
    WordType mask = static_cast<WordType>(1) << (position % WORD_SIZE_BITS);
    words_[position / WORD_SIZE_BITS] ^= (-static_cast<WordType>(v) ^ words_[position / WORD_SIZE_BITS]) & mask;
    return *this;
  }

  /**
   * Efficiently set all bits in the range [start, end).
   *
   * @pre start <= end <= num_bits()
   *
   * @param start The start bit position.
   * @param end The end bit position.
   * @return This bit vector.
   */
  BitVector &SetRange(uint32_t start, uint32_t end) {
    NOISEPAGE_ASSERT(start <= end, "Cannot set backward range");
    NOISEPAGE_ASSERT(end <= GetNumBits(), "End position out of range");

    if (start == end) {
      return *this;
    }

    const auto start_word_idx = start / WORD_SIZE_BITS;
    const auto end_word_idx = end / WORD_SIZE_BITS;

    if (start_word_idx == end_word_idx) {
      const WordType prefix_mask = ALL_ONES_WORD << (start % WORD_SIZE_BITS);
      const WordType postfix_mask = ~(ALL_ONES_WORD << (end % WORD_SIZE_BITS));
      words_[start_word_idx] |= (prefix_mask & postfix_mask);
      return *this;
    }

    // Prefix
    words_[start_word_idx] |= ALL_ONES_WORD << (start % WORD_SIZE_BITS);

    // Middle
    for (uint32_t i = start_word_idx + 1; i < end_word_idx; i++) {
      words_[i] = ALL_ONES_WORD;
    }

    // Postfix
    words_[end_word_idx] |= ~(ALL_ONES_WORD << (end % WORD_SIZE_BITS));

    return *this;
  }

  /**
   * Set all bits to 1.
   * @return This bit vector.
   */
  BitVector &SetAll() {
    for (uint32_t i = 0; i < GetNumWords(); i++) {
      words_[i] = ALL_ONES_WORD;
    }
    ZeroUnusedBits();
    return *this;
  }

  /**
   * Blindly set the bit at the given index to 0.
   * @param position The index of the bit to set.
   * @return This bit vector.
   */
  BitVector &Unset(const uint32_t position) {
    NOISEPAGE_ASSERT(position < GetNumBits(), "Index out of range");
    words_[position / WORD_SIZE_BITS] &= ~(WordType(1) << (position % WORD_SIZE_BITS));
    return *this;
  }

  /**
   * Set all bits in the bit vector to 0.
   * @return This bit vector.
   */
  BitVector &Reset() {
    for (uint32_t i = 0; i < GetNumWords(); i++) {
      words_[i] = WordType(0);
    }
    return *this;
  }

  /**
   * Flip the bit at the given bit position, i.e., change the bit to 1 if it's 0 and to 0 if it's 1.
   * @param position The index of the bit to flip.
   * @return This bit vector.
   */
  BitVector &Flip(const uint32_t position) {
    NOISEPAGE_ASSERT(position < GetNumBits(), "Index out of range");
    words_[position / WORD_SIZE_BITS] ^= WordType(1) << (position % WORD_SIZE_BITS);
    return *this;
  }

  /**
   * Flip all bits in the bit vector.
   * @return This bit vector.
   */
  BitVector &FlipAll() {
    for (uint32_t i = 0; i < GetNumWords(); i++) {
      words_[i] = ~words_[i];
    }
    ZeroUnusedBits();
    return *this;
  }

  /**
   * Get the word at the given word index.
   * @param word_position The index of the word to set.
   * @return Value of the word.
   */
  WordType GetWord(const uint32_t word_position) const {
    NOISEPAGE_ASSERT(word_position < GetNumWords(), "Index out of range");
    return words_[word_position];
  }

  /**
   * Set the value of the word at the given word index to the provided value. If the size of the bit
   * vector is not a multiple of the word size, the tail bits are masked off.
   * @param word_position The index of the word to set.
   * @param word_val The value to set.
   * @return This bit vector.
   */
  BitVector &SetWord(const uint32_t word_position, const WordType word_val) {
    NOISEPAGE_ASSERT(word_position < GetNumWords(), "Index out of range");
    words_[word_position] = word_val;
    if (word_position == GetNumWords() - 1) ZeroUnusedBits();
    return *this;
  }

  /**
   * @return True if all bits are set, i.e., 1; false otherwise.
   */
  bool All() const {
    const uint32_t extra_bits = GetNumExtraBits();

    const uint32_t num_full_words = (extra_bits == 0 ? GetNumWords() : GetNumWords() - 1);

    for (uint32_t i = 0; i < num_full_words; i++) {
      if (words_[i] != ALL_ONES_WORD) {
        return false;
      }
    }

    if (extra_bits != 0) {
      const WordType mask = ~(ALL_ONES_WORD << extra_bits);
      return words_[GetNumWords() - 1] == mask;
    }

    return true;
  }

  /**
   * @return True if all bits are unset, i.e., 0; false otherwise.
   */
  bool None() const {
    for (uint32_t i = 0; i < GetNumWords(); i++) {
      if (words_[i] != static_cast<WordType>(0)) {
        return false;
      }
    }
    return true;
  }

  /**
   * @return True if any bit in the vector is set, i.e., 1; false otherwise.
   */
  bool Any() const { return !None(); }

  /**
   * Count the 1-bits in the bit vector.
   * @return The number of 1-bits in the bit vector.
   */
  uint32_t CountOnes() const {
    uint32_t count = 0;
    for (uint32_t i = 0; i < GetNumWords(); i++) {
      count += BitUtil::CountPopulation(words_[i]);
    }
    return count;
  }

  /**
   * @return The index of the n-th 1-bit. If there are fewer than n bits, return the size.
   */
  uint32_t NthOne(uint32_t n) const {
    for (uint32_t i = 0; i < GetNumWords(); i++) {
      const WordType word = words_[i];
      const uint32_t count = BitUtil::CountPopulation(word);
      if (n < count) {
        const WordType mask = _pdep_u64(static_cast<WordType>(1) << n, word);
        const uint32_t pos = BitUtil::CountTrailingZeros(mask);
        return std::min(GetNumBits(), (i * WORD_SIZE_BITS) + pos);
      }
      n -= count;
    }
    return GetNumBits();
  }

  /**
   * Copy the contents of the provided bit vector into this bit vector.
   *
   * @pre The sizes of the bit vectors must be the same.
   *
   * @param other The bit vector to read and copy from.
   * @return This bit vector.
   */
  BitVector &Copy(const BitVector &other) {
    NOISEPAGE_ASSERT(GetNumBits() == other.GetNumBits(), "Mismatched bit vector size");
    for (uint32_t i = 0; i < GetNumWords(); i++) {
      words_[i] = other.words_[i];
    }
    return *this;
  }

  /**
   * Perform the bitwise intersection of this bit vector with the provided bit vector.
   *
   * @pre The sizes of the bit vectors must be the same.
   *
   * @param other The bit vector to intersect with. Lengths must match exactly.
   * @return This modified bit vector.
   */
  BitVector &Intersect(const BitVector &other) {
    NOISEPAGE_ASSERT(GetNumBits() == other.GetNumBits(), "Mismatched bit vector size");
    for (uint32_t i = 0; i < GetNumWords(); i++) {
      words_[i] &= other.words_[i];
    }
    return *this;
  }

  /**
   * Perform the bitwise union of this bit vector with the provided bit vector.
   *
   * @pre The sizes of the bit vectors must be the same.
   *
   * @param other The bit vector to union with. Lengths must match exactly.
   * @return This modified bit vector.
   */
  BitVector &Union(const BitVector &other) {
    NOISEPAGE_ASSERT(GetNumBits() == other.GetNumBits(), "Mismatched bit vector size");
    for (uint32_t i = 0; i < GetNumWords(); i++) {
      words_[i] |= other.words_[i];
    }
    return *this;
  }

  /**
   * Clear all bits in this bit vector whose corresponding bit is set in the provided bit vector.
   *
   * @pre The sizes of the bit vectors must be the same.
   *
   * @param other The bit vector to diff with. Lengths must match exactly.
   * @return This modified bit vector.
   */
  BitVector &Difference(const BitVector &other) {
    NOISEPAGE_ASSERT(GetNumBits() == other.GetNumBits(), "Mismatched bit vector size");
    for (uint32_t i = 0; i < GetNumWords(); i++) {
      words_[i] &= ~other.words_[i];
    }
    return *this;
  }

  /**
   * Perform a bitwise XOR of this bit vector with the provided bit vector.
   *
   * @pre The sizes of the bit vectors must be the same.
   *
   * @param other The bit vector to XOR with. Lengths must match.
   * @return This modified bit vector.
   */
  BitVector &Xor(const BitVector &other) {
    NOISEPAGE_ASSERT(GetNumBits() == other.GetNumBits(), "Mismatched bit vector size");
    for (uint32_t i = 0; i < GetNumWords(); i++) {
      words_[i] ^= other.words_[i];
    }
    return *this;
  }

  /**
   * Reserve enough space in the bit vector to store the provided bits. This does not change the
   * size of the bit vector, but may allocate additional memory.
   *
   * @param num_bits The desired number of bits to reserve for.
   */
  void Reserve(const uint32_t num_bits) { words_.reserve(NumNeededWords(num_bits)); }

  /**
   * Resize the bit vector to store exactly the provided number of bits. If the new size is larger
   * than the existing size (i.e., num_bits > GetNumBits()) then the bits in the range
   * [0, GetNumBits()) are unchanged, and the remaining bits are set to zero. If the new size is
   * smaller than the existing size (i.e., num_bits < GetNumBits()) then the bits in the range
   * [0, num_bits) are unchanged the remaining bits are discarded.
   *
   * @param num_bits The number of bits to resize this bit vector to.
   */
  void Resize(const uint32_t num_bits) {
    uint32_t new_num_words = NumNeededWords(num_bits);
    words_.resize(new_num_words, WordType(0));
    num_bits_ = num_bits;
    ZeroUnusedBits();
  }

  /**
   * Retain all set bits in the bit vector for which the predicate returns true.
   * @tparam P A predicate functor that accepts an unsigned 32-bit integer and returns a boolean.
   * @param p The predicate to apply to each set bit position.
   */
  template <typename P>
  void UpdateSetBits(P p) {
    static_assert(std::is_invocable_r_v<bool, P, uint32_t>,
                  "Predicate must be accept an unsigned 32-bit index and return a bool");

    for (WordType i = 0, num_words = GetNumWords(); i < num_words; i++) {
      WordType word = words_[i];
      WordType word_result = 0;
      while (word != 0) {
        const auto t = word & -word;
        const auto r = BitUtil::CountTrailingZeros(word);
        word_result |= static_cast<WordType>(p(i * WORD_SIZE_BITS + r)) << r;
        word ^= t;
      }
      words_[i] &= word_result;
    }
  }

  /**
   * Like BitVector::UpdateSetBits(), this function will also retain all set bits in the bit vector
   * for which the predicate returns true. The important difference is that this function will
   * invoke the predicate function on ALL bit positions, not just those positions that are set to 1.
   * This optimization takes advantage of SIMD to enable up-to 4x faster execution times, but the
   * caller must safely tolerate operating on both set and unset bit positions.
   *
   * @tparam P A predicate functor that accepts an unsigned 32-bit integer and returns a boolean.
   * @param p The predicate to apply to each bit position.
   */
  template <typename P>
  void UpdateFull(P p) {
    static_assert(std::is_invocable_r_v<bool, P, uint32_t>,
                  "Predicate must be accept an unsigned 32-bit index and return a bool");
    if (GetNumBits() == 0) {
      return;
    }

    const uint32_t num_full_words = GetNumExtraBits() == 0 ? GetNumWords() : GetNumWords() - 1;

    // This first loop processes all FULL words in the bit vector. It should be
    // fully vectorized if the predicate function can also vectorized.
    for (WordType i = 0; i < num_full_words; i++) {
      WordType word_result = 0;
      for (WordType j = 0; j < WORD_SIZE_BITS; j++) {
        word_result |= static_cast<WordType>(p(i * WORD_SIZE_BITS + j)) << j;
      }
      words_[i] &= word_result;
    }

    // If the last word isn't full, process it using a scalar loop.
    for (WordType i = num_full_words * WORD_SIZE_BITS; i < GetNumBits(); i++) {
      if (!p(i)) {
        Unset(i);
      }
    }
  }

  /**
   * Iterate all bits in this vector and invoke the callback with the index of set bits only.
   * @tparam F Functor object whose signature is equivalent to:
   *           @code
   *           void f(uint32_t index);
   *           @endcode
   * @param f Callback functor applied on the index of each set bit in this bit vector.
   */
  template <typename F>
  void IterateSetBits(F f) const {
    static_assert(std::is_invocable_v<F, uint32_t>,
                  "Callback must be a single-argument functor accepting an unsigned 32-bit index");

    for (WordType i = 0, num_words = GetNumWords(); i < num_words; i++) {
      WordType word = words_[i];
      while (word != 0) {
        const auto t = word & -word;
        const auto r = BitUtil::CountTrailingZeros(word);
        f(i * WORD_SIZE_BITS + r);
        word ^= t;
      }
    }
  }

  /**
   * Populate this bit vector from the values stored in the given bytes. The byte array is assumed
   * to be a "saturated" match vector, i.e., true values are all 1's
   * (255 = 11111111 = std::numeric_limits<uint8_t>::max()), and false values are all 0.
   * @param bytes The array of saturated bytes to read.
   * @param num_bytes The number of bytes in the input array.
   */
  void SetFromBytes(const uint8_t *const bytes, const uint32_t num_bytes) {
    NOISEPAGE_ASSERT(bytes != nullptr, "Null input");
    NOISEPAGE_ASSERT(num_bytes == GetNumBits(), "Byte vector too small");
    VectorUtil::ByteVectorToBitVector(bytes, num_bytes, words_.data());
  }

  /**
   * Return a string representation of this bit vector.
   * @return String representation of this vector.
   */
  std::string ToString() const {
    std::string result = "BitVector(#bits=" + std::to_string(GetNumBits()) + ")=[";
    bool first = true;
    for (uint32_t i = 0; i < GetNumBits(); i++) {
      if (!first) result += ",";
      first = false;
      result += Test(i) ? "1" : "0";
    }
    result += "]";
    return result;
  }

  /**
   * Find the index of the first set bit in the bit vector. If there are no set bits, return the
   * invalid bit position BitVector::kInvalidPos.
   * @return The index of the first set bit in the bit vector.
   */
  uint32_t FindFirst() const noexcept { return FindFrom(0); }

  /**
   * Find the index of the first set bit <b>after</b> the provided bit position. If no bit is set
   * after the position, return the invalid bit position BitVector::kInvalidPos.
   * @param position The position to anchor the search.
   * @return The index of the first set bit after the given position in the bit vector.
   */
  uint32_t FindNext(uint32_t position) const noexcept {
    if (GetNumBits() == 0 || position >= GetNumBits() - 1) {
      return INVALID_POS;
    }

    position++;

    const uint32_t word_index = position / WORD_SIZE_BITS;
    const uint32_t bit_idx = position % WORD_SIZE_BITS;

    // Does the current word have any remaining 1 bits?
    if (WordType word = words_[word_index] >> bit_idx; word != WordType(0)) {
      return position + BitUtil::CountTrailingZeros(word);
    }

    // Find the next bit in the following set of words
    return FindFrom(word_index + 1);
  }

  /**
   * @return The density of set/one bits in this bit vector.
   */
  float ComputeDensity() const noexcept {
    return GetNumBits() == 0 ? 0.0 : static_cast<float>(CountOnes()) / GetNumBits();
  }

  // -------------------------------------------------------
  // C++ operator overloads.
  // -------------------------------------------------------

  /**
   * Return the value of the bit at the provided position in the bit vector. Used for testing the
   * value of a bit:
   *
   * @code
   * BitVector<> bv(20);
   * if (bv[10]) {
   *   // work
   * }
   * @endcode
   *
   * @param position The position/index of the bit to check.
   * @return True if the bit at the input position is set; false otherwise.
   */
  bool operator[](const uint32_t position) const { return Test(position); }

  /**
   * Return a reference to the bit at the provided position in the bit vector. The reference can be
   * modified, but is invalid if the bit vector is resized.
   *
   * @code
   * BitVector<> bv(20);
   * bv[10] = true;
   * bv[20] = bv[10];
   * @endcode
   *
   * @param position The position/index of the bit to check.
   * @return A reference to the bit at the input position.
   */
  BitReference operator[](const uint32_t position) {
    NOISEPAGE_ASSERT(position < GetNumBits(), "Out-of-range access");
    return BitReference(&words_[position / WORD_SIZE_BITS], position % WORD_SIZE_BITS);
  }

  /**
   * @return True if this bit-vector equals the provided bit vector, bit-for-bit.
   */
  bool operator==(const BitVector &that) const noexcept {
    return GetNumBits() == that.GetNumBits() && words_ == that.words_;
  }

  /**
   * @return True if this bit-vector is not equal to the provided bit vector.
   */
  bool operator!=(const BitVector &that) const noexcept { return !(*this == that); }

  /**
   * Perform a bitwise difference between this bit vector and the provided bit vector storing the
   * result in this bit vector.
   *
   * @pre The two vectors must be the same size.
   *
   * @param that The bit vector to difference with.
   * @return This bit vector after all <b>common</b> between this and @em that have been removed.
   */
  BitVector &operator-=(const BitVector &that) { return Difference(that); }

  /**
   * Perform an intersection of this bit vector the provided bit vector storing the result in this
   * bit vector.
   *
   * @pre The two vectors must be the same size.
   *
   * @param that The bit vector to intersect with.
   * @return This bit vector after an intersection with @em that bit vector.
   */
  BitVector &operator&=(const BitVector &that) { return Intersect(that); }

  /**
   * Perform a union of this bit vector and the provided bit vector storing the result in this bit
   * vector.
   *
   * @pre The two vectors must be the same size.
   *
   * @param that The bit vector to union with.
   * @return This bit vector after a union with @em that bit vector.
   */
  BitVector &operator|=(const BitVector &that) { return Union(that); }

  /**
   * Perform a bitwise XOR of this vector and the provided bit vector, storing the result in this
   * bit vector.
   *
   * @pre The two vectors must be the same size.
   *
   * @param that The bit vector to XOR with.
   * @return This bit vector after XORing with @em that bit vector.
   */
  BitVector &operator^=(const BitVector &that) { return Xor(that); }

  // -------------------------------------------------------
  // Accessors.
  // -------------------------------------------------------

  /**
   * @return The number of bits in the bit vector.
   */
  uint32_t GetNumBits() const noexcept { return num_bits_; }

  /**
   * @return The number of "words" in the bit vector. Recall that the word type is a template arg.
   */
  uint32_t GetNumWords() const noexcept { return words_.size(); }

  /**
   * @return A const-view of the words making up the bit vector.
   */
  const WordType *GetWords() const noexcept { return words_.data(); }

 private:
  // The number of bits in the last word
  uint32_t GetNumExtraBits() const { return num_bits_ % WORD_SIZE_BITS; }

  // Zero unused bits in the last word
  void ZeroUnusedBits() {
    const uint32_t extra_bits = GetNumExtraBits();
    if (extra_bits != 0) {
      words_[GetNumWords() - 1] &= ~(ALL_ONES_WORD << extra_bits);
    }
  }

  // Find the first set bit in the vector starting at the given word position.
  uint32_t FindFrom(uint32_t word_index) const noexcept {
    for (uint32_t i = word_index; i < GetNumWords(); i++) {
      if (words_[i] != WordType(0)) {
        return i * WORD_SIZE_BITS + BitUtil::CountTrailingZeros(words_[i]);
      }
    }
    return INVALID_POS;
  }

 private:
  // The number of bits in the bit vector.
  uint32_t num_bits_{0};
  // The array of words making up the bit vector.
  std::vector<WordType, Allocator> words_;
};

/**
 * Bit vector difference. Returns a - b, the difference of the bit vectors @em a and @em b.
 * NOTE: The left input @em a is copied on purpose to ensure RVO.
 * @pre Both inputs must have the same size.
 * @tparam T The word size used by both bit vectors.
 * @param a The left bit vector to the operation.
 * @param b The right bit vector to the operation.
 * @return The difference between a and b.
 */
template <typename T>
inline BitVector<T> operator-(BitVector<T> a, const BitVector<T> &b) {
  a -= b;
  return a;
}

/**
 * Bit vector intersection. Returns a & b, the intersection of the bit vectors @em a and @em b.
 * NOTE: The left input @em a is copied on purpose to ensure RVO.
 * @pre Both inputs must have the same size.
 * @tparam T The word size used by both bit vectors.
 * @param a The left bit vector to the operation.
 * @param b The right bit vector to the operation.
 * @return The intersection between a and b.
 */
template <typename T>
inline BitVector<T> operator&(BitVector<T> a, const BitVector<T> &b) {
  a &= b;
  return a;
}

/**
 * Free bit vector union. Returns a & b, the union of the bit vectors @em a and @em b.
 * NOTE: The left input @em a is copied on purpose to ensure RVO.
 * @pre Both inputs must have the same size.
 * @tparam T The word size used by both bit vectors.
 * @param a The left bit vector to the operation.
 * @param b The right bit vector to the operation.
 * @return The union between a and b.
 */
template <typename T>
inline BitVector<T> operator|(BitVector<T> a, const BitVector<T> &b) {
  a |= b;
  return a;
}

/**
 * Bit vector XOR operation. Returns a ^ b, the XOR result of the bit vectors @em a and @em b.
 * NOTE: The left input @em a is copied on purpose to ensure RVO.
 * @pre Both inputs must have the same size.
 * @tparam T The word size used by both bit vectors.
 * @param a The left bit vector to the operation.
 * @param b The right bit vector to the operation.
 * @return The XOR result between a and b.
 */
template <typename T>
inline BitVector<T> operator^(BitVector<T> a, const BitVector<T> &b) {
  a ^= b;
  return a;
}

}  // namespace noisepage::execution::util
