#pragma once

#include <memory>
#include <utility>

#include "execution/util/common.h"
#include "execution/util/macros.h"
#include "execution/util/math_util.h"

namespace tpl::util {

class BitUtil {
 public:
  // The number of bits in one word
  static constexpr const u32 kBitWordSize = sizeof(u32) * kBitsPerByte;

  // Make sure the number of bits in a word is a power of two to make all these
  // bit operations cheap
  static_assert(util::MathUtil::IsPowerOf2(kBitWordSize));

  /// Count the number of zeroes from the most significant bit to the first 1 in
  /// the input number @ref val
  /// \tparam T The data type of the input value
  /// \param val The input number
  /// \return The number of leading zeros
  template <typename T>
  ALWAYS_INLINE static u64 CountLeadingZeros(T val) {
    return llvm::countLeadingZeros(val);
  }

  /// Calculate the number of 32-bit words are needed to store a bit vector of
  /// the given size
  /// \param num_bits The size of the bit vector, in bits
  /// \return The number of words needed to store a bit vector of the given size
  ALWAYS_INLINE static u64 Num32BitWordsFor(u64 num_bits) {
    return MathUtil::DivRoundUp(num_bits, kBitWordSize);
  }

  /// Test if the bit at index \a idx is set in the bit vector
  /// \param bits The bit vector
  /// \param idx The index of the bit to check
  /// \return True if set; false otherwise
  ALWAYS_INLINE static bool Test(const u32 bits[], const u32 idx) {
    const u32 mask = 1u << (idx % kBitWordSize);
    return bits[idx / kBitWordSize] & mask;
  }

  /// Set the bit at index \a idx to 1 in the bit vector \a bits
  /// \param bits The bit vector
  /// \param idx The index of the bit to set to 1
  ALWAYS_INLINE static void Set(u32 bits[], const u32 idx) {
    bits[idx / kBitWordSize] |= 1u << (idx % kBitWordSize);
  }

  /// Set the bit at index \a idx to 0 in the bit vector \a bits
  /// \param bits The bit vector
  /// \param idx The index of the bit to unset
  ALWAYS_INLINE static void Unset(u32 bits[], const u32 idx) {
    bits[idx / kBitWordSize] &= ~(1u << (idx % kBitWordSize));
  }

  /// Flip the value of the bit at index \a idx in the bit vector
  /// \param bits The bit vector
  /// \param idx The index of the bit to flip
  ALWAYS_INLINE static void Flip(u32 bits[], const u32 idx) {
    bits[idx / kBitWordSize] ^= 1u << (idx % kBitWordSize);
  }

  /// Clear all bits in the bit vector
  /// \param bits The bit vector
  /// \param num_bits The number of elements in the bit vector
  ALWAYS_INLINE static void Clear(u32 bits[], const u64 num_bits) {
    auto num_words = Num32BitWordsFor(num_bits);
    std::memset(bits, 0, num_words * sizeof(u32));
  }

  /// Count the number of set bits in the given value
  template <typename T>
  static u32 CountBits(T val) {
    return llvm::countPopulation(val);
  }
};

/// Common class to bit vectors that are inlined or stored externally to the
/// class. Uses CRTP to properly access bits and bit-vector size. Subclasses
/// must implement bits() and num_bits() and provide raw access to the bit
/// vector data and the number of bits in the bit vector, respectively.
template <typename Subclass>
class BitVectorBase {
 public:
  /// Test the value of the bit at index \a idx in the bit-vector
  bool Test(u32 idx) const {
    TPL_ASSERT(idx < impl()->num_bits(), "Index out of range");
    return BitUtil::Test(impl()->bits(), idx);
  }

  void Set(u32 idx) {
    TPL_ASSERT(idx < impl()->num_bits(), "Index out of range");
    return BitUtil::Set(impl()->bits(), idx);
  }

  void Unset(u32 idx) {
    TPL_ASSERT(idx < impl()->num_bits(), "Index out of range");
    return BitUtil::Unset(impl()->bits(), idx);
  }

  void Flip(u32 idx) {
    TPL_ASSERT(idx < impl()->num_bits(), "Index out of range");
    return BitUtil::Flip(impl()->bits(), idx);
  }

  void ClearAll() { return BitUtil::Clear(impl()->bits(), impl()->num_bits()); }

  bool operator[](u32 idx) const { return Test(idx); }

 private:
  Subclass *impl() { return static_cast<Subclass *>(this); }
  const Subclass *impl() const { return static_cast<const Subclass *>(this); }
};

/// A bit vector that either owns the bit set, or can interpret an externally
/// allocate bit vector
class BitVector : public BitVectorBase<BitVector> {
 public:
  // Create an uninitialized bit vector. Callers **must** use Init() before
  // interacting with the BitVector
  BitVector() : owned_bits_(nullptr), bits_(nullptr), num_bits_(0) {}

  // Create a new BitVector with the specified number of bits
  explicit BitVector(u32 num_bits)
      : owned_bits_(
            std::make_unique<u32[]>(BitUtil::Num32BitWordsFor(num_bits))),
        bits_(owned_bits_.get()),
        num_bits_(num_bits) {
    ClearAll();
  }

  // Take over the given bits
  BitVector(std::unique_ptr<u32[]> bits, u32 num_bits)
      : owned_bits_(std::move(bits)),
        bits_(owned_bits_.get()),
        num_bits_(num_bits) {}

  // Provide bit vector access to the given bits, not taking ownership of them
  BitVector(u32 unowned_bits[], u32 num_bits)
      : owned_bits_(nullptr), bits_(unowned_bits), num_bits_(num_bits) {}

  void Init(u32 num_bits) {
    owned_bits_ = std::make_unique<u32[]>(BitUtil::Num32BitWordsFor(num_bits));
    bits_ = owned_bits_.get();
    num_bits_ = num_bits;
  }

  u32 num_bits() const { return num_bits_; }

  u32 *bits() const { return bits_; }

 private:
  std::unique_ptr<u32[]> owned_bits_;

  u32 *bits_;

  u32 num_bits_;
};

/// A bit vector that stores the bit set data inline in the class
template <u32 NumBits>
class InlinedBitVector : public BitVectorBase<InlinedBitVector<NumBits>> {
  static_assert(NumBits % BitUtil::kBitWordSize == 0,
                "Inlined bit vectors only support vectors that are a multiple "
                "of the word size (i.e., 32 bits, 64 bits, 128 bits, etc.");

 public:
  InlinedBitVector() : bits_{0} {}

  u32 num_bits() const { return NumBits; }

  u32 *bits() { return bits_; }

  const u32 *bits() const { return bits_; }

 private:
  u32 bits_[NumBits / BitUtil::kBitWordSize];
};

}  // namespace tpl::util
