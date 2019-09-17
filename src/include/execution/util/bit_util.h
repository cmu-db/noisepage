#pragma once

#include <memory>
#include <utility>

#include "common/constants.h"
#include "common/macros.h"
#include "common/math_util.h"
#include "execution/util/execution_common.h"

namespace terrier::execution::util {

/**
 * Utility class to deal with bit-level operations.
 */
class BitUtil {
 public:
  /**
   * The number of bits in one word
   */
  static constexpr const uint32_t K_BIT_WORD_SIZE = sizeof(uint32_t) * common::Constants::K_BITS_PER_BYTE;

  // Make sure the number of bits in a word is a power of two to make all these
  // bit operations cheap
  static_assert(common::MathUtil::IsPowerOf2(K_BIT_WORD_SIZE));

  /**
   * Count the number of zeroes from the most significant bit to the first 1 in
   * the input number @em val
   * @tparam T The data type of the input value
   * @param val The input number
   * @return The number of leading zeros
   */
  template <typename T>
  ALWAYS_INLINE static uint64_t CountLeadingZeros(T val) {
    return llvm::countLeadingZeros(val);
  }

  /**
   * Calculate the number of 32-bit words are needed to store a bit vector of
   * the given size
   * @param num_bits The size of the bit vector, in bits
   * @return The number of words needed to store a bit vector of the given size
   */
  ALWAYS_INLINE static uint64_t Num32BitWordsFor(uint64_t num_bits) {
    return common::MathUtil::DivRoundUp(num_bits, K_BIT_WORD_SIZE);
  }

  /**
   * Test if the bit at index \a idx is set in the bit vector
   * @param bits The bit vector
   * @param idx The index of the bit to check
   * @return True if set; false otherwise
   */
  ALWAYS_INLINE static bool Test(const uint32_t bits[], const uint32_t idx) {
    const uint32_t mask = 1u << (idx % K_BIT_WORD_SIZE);
    return (bits[idx / K_BIT_WORD_SIZE] & mask) != 0;
  }

  /**
   * Set the bit at index \a idx to 1 in the bit vector \a bits
   * @param bits The bit vector
   * @param idx The index of the bit to set to 1
   */
  ALWAYS_INLINE static void Set(uint32_t bits[], const uint32_t idx) {
    bits[idx / K_BIT_WORD_SIZE] |= 1u << (idx % K_BIT_WORD_SIZE);
  }

  /**
   * Set the bit at index @em idx to the boolean indicated by @em val
   * @param bits The bit vector
   * @param idx The index of the bit to set or unset
   * @param val The value to set the bit to
   */
  ALWAYS_INLINE static void SetTo(uint32_t bits[], const uint32_t idx, const bool val) {
    if (val) {
      Set(bits, idx);
    } else {
      Unset(bits, idx);
    }
  }

  /**
   * Set the bit at index idx to 0 in the bit vector
   * @param bits bit vector to be modified
   * @param idx index of the bit to be set to 0
   */
  ALWAYS_INLINE static void Unset(uint32_t bits[], const uint32_t idx) {
    bits[idx / K_BIT_WORD_SIZE] &= ~(1u << (idx % K_BIT_WORD_SIZE));
  }

  /**
   * Flip the bit at index idx in the bit vector
   * @param bits bit vector to be modified
   * @param idx index of the bit to flip
   */
  ALWAYS_INLINE static void Flip(uint32_t bits[], const uint32_t idx) {
    bits[idx / K_BIT_WORD_SIZE] ^= 1u << (idx % K_BIT_WORD_SIZE);
  }

  /**
   * Clear all the bits in the bit vector, setting them to 0
   * @param bits bit vector to be cleared
   * @param num_bits size of the bit vector, in bits
   */
  ALWAYS_INLINE static void Clear(uint32_t bits[], const uint64_t num_bits) {
    auto num_words = Num32BitWordsFor(num_bits);
    std::memset(bits, 0, num_words * sizeof(uint32_t));
  }

  /**
   * Return the number of set bits in the given value
   */
  template <typename T>
  static uint32_t CountBits(T val) {
    return llvm::countPopulation(val);
  }
};

/**
 * Common class to bit vectors that are inlined or stored externally to the class. Uses CRTP to properly access bits
 * and bit-vector size. Subclasses must implement Bits() and NumBits() and provide raw access to the bit vector data
 * and the number of bits in the bit vector, respectively.
 */
template <typename Subclass>
class BitVectorBase {
 public:
  /**
   * @return true if the bit at the given index is 1, false otherwise
   */
  bool Test(uint32_t idx) const {
    TERRIER_ASSERT(idx < Impl()->NumBits(), "Index out of range");
    return BitUtil::Test(Impl()->Bits(), idx);
  }

  /**
   * Set the bit at index idx in the bitvector to 1
   */
  void Set(uint32_t idx) {
    TERRIER_ASSERT(idx < Impl()->NumBits(), "Index out of range");
    return BitUtil::Set(Impl()->Bits(), idx);
  }

  /**
   * Set the bit at the given index to the given value
   */
  void SetTo(const uint32_t idx, const bool val) {
    TERRIER_ASSERT(idx < Impl()->NumBits(), "Index out of range");
    return BitUtil::SetTo(Impl()->Bits(), idx, val);
  }

  /**
   * Set the bit at index idx in the bitvector to 0
   */
  void Unset(uint32_t idx) {
    TERRIER_ASSERT(idx < Impl()->NumBits(), "Index out of range");
    return BitUtil::Unset(Impl()->Bits(), idx);
  }

  /**
   * Flip the bit at index idx in the bitvector
   */
  void Flip(uint32_t idx) {
    TERRIER_ASSERT(idx < Impl()->NumBits(), "Index out of range");
    return BitUtil::Flip(Impl()->Bits(), idx);
  }

  /**
   * Sets all the bits in the bitvector to 0
   */
  void ClearAll() { return BitUtil::Clear(Impl()->Bits(), Impl()->NumBits()); }

  /**
   * @return true if the bit at the given index is 1, false otherwise
   */
  bool operator[](uint32_t idx) const { return Test(idx); }

 private:
  Subclass *Impl() { return static_cast<Subclass *>(this); }
  const Subclass *Impl() const { return static_cast<const Subclass *>(this); }
};

/**
 * A bitvector that either owns its bitset, or can interpret an externally allocated bitvector
 */
class BitVector : public BitVectorBase<BitVector> {
 public:
  /**
   * Create an uninitialized bit vector. Callers **must** use Init() before interacting with the BitVector.
   */
  BitVector() = default;

  /**
   * Create a new BitVector with the specified number of bits
   */
  explicit BitVector(uint32_t num_bits)
      : owned_bits_(std::make_unique<uint32_t[]>(BitUtil::Num32BitWordsFor(num_bits))),
        bits_(owned_bits_.get()),
        num_bits_(num_bits) {
    ClearAll();
  }

  /**
   * Create a new BitVector which takes over the given bits
   */
  BitVector(std::unique_ptr<uint32_t[]> bits, uint32_t num_bits)
      : owned_bits_(std::move(bits)), bits_(owned_bits_.get()), num_bits_(num_bits) {}

  /**
   * Create a new BitVector which provides bitvector access to the given bits, not taking ownership of them
   */
  BitVector(uint32_t unowned_bits[], uint32_t num_bits)
      : owned_bits_(nullptr), bits_(unowned_bits), num_bits_(num_bits) {}

  /**
   * Initializes the BitVector to contain and own num_bits of bits
   * @param num_bits number of bits to be created
   */
  void Init(uint32_t num_bits) {
    owned_bits_ = std::make_unique<uint32_t[]>(BitUtil::Num32BitWordsFor(num_bits));
    bits_ = owned_bits_.get();
    num_bits_ = num_bits;
  }

  /**
   * @return number of bits in the bitvector
   */
  uint32_t NumBits() const { return num_bits_; }

  /**
   * @return pointer to bits in the bitvector
   */
  uint32_t *Bits() const { return bits_; }

 private:
  std::unique_ptr<uint32_t[]> owned_bits_{nullptr};

  uint32_t *bits_{nullptr};

  uint32_t num_bits_{0};
};

/** A bit vector that stores the bitset data inline in the class. */
template <uint32_t Size>
class InlinedBitVector : public BitVectorBase<InlinedBitVector<Size>> {
  static_assert(Size % BitUtil::K_BIT_WORD_SIZE == 0,
                "Inlined bit vectors only support vectors that are a multiple "
                "of the word size (i.e., 32 bits, 64 bits, 128 bits, etc.");

 public:
  /**
   * Create a new BitVector with all bits initialized to 0
   */
  InlinedBitVector() : bits_{0} {}

  /**
   * @return number of bits in the bitvector
   */
  uint32_t NumBits() const { return Size; }

  /**
   * @return pointer to bits in the bitvector
   */
  uint32_t *Bits() { return bits_; }

  /**
   * @return const pointer to bits in the bitvector
   */
  const uint32_t *Bits() const { return bits_; }

 private:
  uint32_t bits_[Size / BitUtil::K_BIT_WORD_SIZE];
};

}  // namespace terrier::execution::util
