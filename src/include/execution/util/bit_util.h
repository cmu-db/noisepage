#pragma once

#include <memory>
#include <utility>

#include "execution/util/common.h"
#include "execution/util/macros.h"
#include "execution/util/math_util.h"

namespace terrier::util {

/**
 * Utility class to deal with bit-level operations.
 */
class BitUtil {
 public:
  /**
   * The number of bits in one word
   */
  static constexpr const u32 kBitWordSize = sizeof(u32) * kBitsPerByte;

  // Make sure the number of bits in a word is a power of two to make all these
  // bit operations cheap
  static_assert(util::MathUtil::IsPowerOf2(kBitWordSize));

  /**
   * Count the number of zeroes from the most significant bit to the first 1 in
   * the input number @em val
   * @tparam T The data type of the input value
   * @param val The input number
   * @return The number of leading zeros
   */
  template <typename T>
  ALWAYS_INLINE static u64 CountLeadingZeros(T val) {
    return llvm::countLeadingZeros(val);
  }

  /**
   * Calculate the number of 32-bit words are needed to store a bit vector of
   * the given size
   * @param num_bits The size of the bit vector, in bits
   * @return The number of words needed to store a bit vector of the given size
   */
  ALWAYS_INLINE static u64 Num32BitWordsFor(u64 num_bits) { return MathUtil::DivRoundUp(num_bits, kBitWordSize); }

  /**
   * Test if the bit at index \a idx is set in the bit vector
   * @param bits The bit vector
   * @param idx The index of the bit to check
   * @return True if set; false otherwise
   */
  ALWAYS_INLINE static bool Test(const u32 bits[], const u32 idx) {
    const u32 mask = 1u << (idx % kBitWordSize);
    return (bits[idx / kBitWordSize] & mask) != 0;
  }

  /**
   * Set the bit at index \a idx to 1 in the bit vector \a bits
   * @param bits The bit vector
   * @param idx The index of the bit to set to 1
   */
  ALWAYS_INLINE static void Set(u32 bits[], const u32 idx) { bits[idx / kBitWordSize] |= 1u << (idx % kBitWordSize); }

  /**
   * Set the bit at index @em idx to the boolean indicated by @em val
   * @param bits The bit vector
   * @param idx The index of the bit to set or unset
   * @param val The value to set the bit to
   */
  ALWAYS_INLINE static void SetTo(u32 bits[], const u32 idx, const bool val) {
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
  ALWAYS_INLINE static void Unset(u32 bits[], const u32 idx) {
    bits[idx / kBitWordSize] &= ~(1u << (idx % kBitWordSize));
  }

  /**
   * Flip the bit at index idx in the bit vector
   * @param bits bit vector to be modified
   * @param idx index of the bit to flip
   */
  ALWAYS_INLINE static void Flip(u32 bits[], const u32 idx) { bits[idx / kBitWordSize] ^= 1u << (idx % kBitWordSize); }

  /**
   * Clear all the bits in the bit vector, setting them to 0
   * @param bits bit vector to be cleared
   * @param num_bits size of the bit vector, in bits
   */
  ALWAYS_INLINE static void Clear(u32 bits[], const u64 num_bits) {
    auto num_words = Num32BitWordsFor(num_bits);
    std::memset(bits, 0, num_words * sizeof(u32));
  }

  /**
   * Return the number of set bits in the given value
   */
  template <typename T>
  static u32 CountBits(T val) {
    return llvm::countPopulation(val);
  }
};

/**
 * Common class to bit vectors that are inlined or stored externally to the class. Uses CRTP to properly access bits
 * and bit-vector size. Subclasses must implement bits() and num_bits() and provide raw access to the bit vector data
 * and the number of bits in the bit vector, respectively.
 */
template <typename Subclass>
class BitVectorBase {
 public:
  /**
   * @return true if the bit at the given index is 1, false otherwise
   */
  bool Test(u32 idx) const {
    TPL_ASSERT(idx < impl()->num_bits(), "Index out of range");
    return BitUtil::Test(impl()->bits(), idx);
  }

  /**
   * Set the bit at index idx in the bitvector to 1
   */
  void Set(u32 idx) {
    TPL_ASSERT(idx < impl()->num_bits(), "Index out of range");
    return BitUtil::Set(impl()->bits(), idx);
  }

  /**
   * Set the bit at the given index to the given value
   */
  void SetTo(const u32 idx, const bool val) {
    TPL_ASSERT(idx < impl()->num_bits(), "Index out of range");
    return BitUtil::SetTo(impl()->bits(), idx, val);
  }

  /**
   * Set the bit at index idx in the bitvector to 0
   */
  void Unset(u32 idx) {
    TPL_ASSERT(idx < impl()->num_bits(), "Index out of range");
    return BitUtil::Unset(impl()->bits(), idx);
  }

  /**
   * Flip the bit at index idx in the bitvector
   */
  void Flip(u32 idx) {
    TPL_ASSERT(idx < impl()->num_bits(), "Index out of range");
    return BitUtil::Flip(impl()->bits(), idx);
  }

  /**
   * Sets all the bits in the bitvector to 0
   */
  void ClearAll() { return BitUtil::Clear(impl()->bits(), impl()->num_bits()); }

  /**
   * @return true if the bit at the given index is 1, false otherwise
   */
  bool operator[](u32 idx) const { return Test(idx); }

 private:
  Subclass *impl() { return static_cast<Subclass *>(this); }
  const Subclass *impl() const { return static_cast<const Subclass *>(this); }
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
  explicit BitVector(u32 num_bits)
      : owned_bits_(std::make_unique<u32[]>(BitUtil::Num32BitWordsFor(num_bits))),
        bits_(owned_bits_.get()),
        num_bits_(num_bits) {
    ClearAll();
  }

  /**
   * Create a new BitVector which takes over the given bits
   */
  BitVector(std::unique_ptr<u32[]> bits, u32 num_bits)
      : owned_bits_(std::move(bits)), bits_(owned_bits_.get()), num_bits_(num_bits) {}

  /**
   * Create a new BitVector which provides bitvector access to the given bits, not taking ownership of them
   */
  BitVector(u32 unowned_bits[], u32 num_bits) : owned_bits_(nullptr), bits_(unowned_bits), num_bits_(num_bits) {}

  /**
   * Initializes the BitVector to contain and own num_bits of bits
   * @param num_bits number of bits to be created
   */
  void Init(u32 num_bits) {
    owned_bits_ = std::make_unique<u32[]>(BitUtil::Num32BitWordsFor(num_bits));
    bits_ = owned_bits_.get();
    num_bits_ = num_bits;
  }

  /**
   * @return number of bits in the bitvector
   */
  u32 num_bits() const { return num_bits_; }

  /**
   * @return pointer to bits in the bitvector
   */
  u32 *bits() const { return bits_; }

 private:
  std::unique_ptr<u32[]> owned_bits_{nullptr};

  u32 *bits_{nullptr};

  u32 num_bits_{0};
};

/** A bit vector that stores the bitset data inline in the class. */
template <u32 NumBits>
class InlinedBitVector : public BitVectorBase<InlinedBitVector<NumBits>> {
  static_assert(NumBits % BitUtil::kBitWordSize == 0,
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
  u32 num_bits() const { return NumBits; }

  /**
   * @return pointer to bits in the bitvector
   */
  u32 *bits() { return bits_; }

  /**
   * @return const pointer to bits in the bitvector
   */
  const u32 *bits() const { return bits_; }

 private:
  u32 bits_[NumBits / BitUtil::kBitWordSize];
};

}  // namespace terrier::util
