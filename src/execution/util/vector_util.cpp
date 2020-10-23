#include "execution/util/vector_util.h"

#include <immintrin.h>

#include "common/math_util.h"
#include "execution/util/bit_util.h"
#include "execution/util/simd/types.h"

namespace noisepage::execution::util {

uint32_t VectorUtil::IntersectSelected(const sel_t *sel_vector_1, const uint32_t sel_vector_1_len,
                                       const sel_t *sel_vector_2, const uint32_t sel_vector_2_len,
                                       sel_t *out_sel_vector) {
  // No-op if either vector is empty
  if (sel_vector_1_len == 0 || sel_vector_2_len == 0) {
    return 0;
  }

  // Canonical-ize; ensure the first vector is smaller than the second
  if (sel_vector_1_len > sel_vector_2_len) {
    return IntersectSelected(sel_vector_2, sel_vector_2_len, sel_vector_1, sel_vector_1_len, out_sel_vector);
  }

  // Run

  // TODO(pmenon): We can use something faster or adaptive, like Katsov et.al or
  //               Inoue et. al, but there isn't a need for fast sorted-set
  //               intersection at the moment.

  uint32_t i = 0, j = 0, k = 0;
  while (i < sel_vector_1_len && j < sel_vector_2_len) {
    if (sel_vector_1[i] == sel_vector_2[j]) {
      out_sel_vector[k++] = sel_vector_1[i];
      i++;
      j++;
    } else if (sel_vector_1[i] < sel_vector_2[j]) {
      i++;
    } else {
      j++;
    }
  }

  return k;
}

uint32_t VectorUtil::IntersectSelected(const sel_t *sel_vector, const uint32_t sel_vector_len,
                                       const uint64_t *bit_vector, const uint32_t bit_vector_len,
                                       sel_t *out_sel_vector) {
  uint32_t k = 0;
  for (uint32_t i = 0; i < sel_vector_len; i++) {
    const auto index = sel_vector[i];
    const bool bit = static_cast<bool>(bit_vector[index / 64] & (static_cast<uint64_t>(1) << (index % 64)));
    out_sel_vector[k] = index;
    k += static_cast<uint32_t>(bit);
  }
  return k;
}

uint32_t VectorUtil::DiffSelectedScalar(uint32_t n, const sel_t *sel_vector, uint32_t m, sel_t *out_sel_vector) {
  uint32_t i = 0, j = 0, k = 0;
  for (; i < m; i++, j++) {
    while (j < sel_vector[i]) {
      out_sel_vector[k++] = j++;
    }
  }
  while (j < n) {
    out_sel_vector[k++] = j++;
  }

  return n - m;
}

uint32_t VectorUtil::DiffSelectedWithScratchPad(uint32_t n, const sel_t *sel_vector, uint32_t sel_vector_len,
                                                sel_t *out_sel_vector, uint8_t *scratch) {
  NOISEPAGE_ASSERT(n <= common::Constants::K_DEFAULT_VECTOR_SIZE, "Selection vector too large");
  std::memset(scratch, 0, n);
  VectorUtil::SelectionVectorToByteVector(sel_vector, sel_vector_len, scratch);
  for (uint32_t i = 0; i < n; i++) {
    scratch[i] = ~scratch[i];
  }
  return VectorUtil::ByteVectorToSelectionVector(scratch, n, out_sel_vector);
}

uint32_t VectorUtil::DiffSelected(const uint32_t n, const sel_t *sel_vector, const uint32_t sel_vector_len,
                                  sel_t *out_sel_vector) {
  uint8_t scratch[common::Constants::K_DEFAULT_VECTOR_SIZE];
  return DiffSelectedWithScratchPad(n, sel_vector, sel_vector_len, out_sel_vector, scratch);
}

void VectorUtil::SelectionVectorToByteVector(const sel_t *sel_vector, const uint32_t num_elems, uint8_t *byte_vector) {
  for (uint32_t i = 0; i < num_elems; i++) {
    byte_vector[sel_vector[i]] = 0xff;
  }
}

// TODO(pmenon): Consider splitting into dense and sparse implementations.
uint32_t VectorUtil::ByteVectorToSelectionVector(const uint8_t *byte_vector, const uint32_t num_bytes,
                                                 sel_t *sel_vector) {
  // Byte-vector index
  uint32_t i = 0;

  // Selection vector write index
  uint32_t k = 0;

  // Main vector loop
  const auto eight = _mm_set1_epi16(8);
  auto idx = _mm_set1_epi16(0);
  for (; i + 8 <= num_bytes; i += 8) {
    const auto word = *reinterpret_cast<const uint64_t *>(byte_vector + i);
    const auto mask = _pext_u64(word, 0x202020202020202);
    NOISEPAGE_ASSERT(mask < 256, "Out-of-bounds mask");
    const auto match_pos_scaled = _mm_loadl_epi64(reinterpret_cast<const __m128i *>(&simd::K8_BIT_MATCH_LUT[mask]));
    const auto match_pos = _mm_cvtepi8_epi16(match_pos_scaled);
    const auto pos_vec = _mm_add_epi16(idx, match_pos);
    idx = _mm_add_epi16(idx, eight);
    _mm_storeu_si128(reinterpret_cast<__m128i *>(sel_vector + k), pos_vec);
    k += BitUtil::CountPopulation(static_cast<uint32_t>(mask));
  }

  // Tail
  for (; i < num_bytes; i++) {
    sel_vector[k] = i;
    k += static_cast<uint32_t>(byte_vector[i] == 0xFF);
  }

  return k;
}

void VectorUtil::ByteVectorToBitVector(const uint8_t *byte_vector, const uint32_t num_bytes, uint64_t *bit_vector) {
  // Byte-vector index
  uint32_t i = 0;

  // Bit-vector word index
  uint32_t k = 0;

  // Main vector loop
  for (; i + 64 <= num_bytes; i += 64, k++) {
    const auto v_lo = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(byte_vector + i));
    const auto v_hi = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(byte_vector + i + 32));
    const auto hi = static_cast<uint32_t>(_mm256_movemask_epi8(v_hi));
    const auto lo = static_cast<uint32_t>(_mm256_movemask_epi8(v_lo));
    bit_vector[k] = (static_cast<uint64_t>(hi) << 32u) | lo;
  }

  // Tail
  for (; i < num_bytes; i++) {
    const auto val = static_cast<int8_t>(byte_vector[i]);
    const auto mask = static_cast<uint64_t>(1) << (i % 64u);
    bit_vector[k] ^= (static_cast<uint64_t>(val) ^ bit_vector[k]) & mask;
  }
}

void VectorUtil::BitVectorToByteVector(const uint64_t *bit_vector, const uint32_t num_bits, uint8_t *byte_vector) {
  const __m256i shuffle =
      _mm256_setr_epi64x(0x0000000000000000, 0x0101010101010101, 0x0202020202020202, 0x0303030303030303);
  const __m256i bit_mask = _mm256_set1_epi64x(0x7fbfdfeff7fbfdfe);

  // Byte-vector write index
  uint32_t k = 0;

  // Main vector loop processes 64 elements per iteration
  for (uint32_t i = 0; i < num_bits / 64; i++, k += 64) {
    uint64_t word = bit_vector[i];

    // Lower 32-bits first
    __m256i vmask = _mm256_set1_epi32(static_cast<uint32_t>(word));
    vmask = _mm256_shuffle_epi8(vmask, shuffle);
    vmask = _mm256_or_si256(vmask, bit_mask);
    __m256i vbytes = _mm256_cmpeq_epi8(vmask, _mm256_set1_epi64x(-1));
    _mm256_storeu_si256(reinterpret_cast<__m256i *>(byte_vector + k), vbytes);

    // Upper 32-bits
    vmask = _mm256_set1_epi32(static_cast<int32_t>(word >> 32u));
    vmask = _mm256_shuffle_epi8(vmask, shuffle);
    vmask = _mm256_or_si256(vmask, bit_mask);
    vbytes = _mm256_cmpeq_epi8(vmask, _mm256_set1_epi64x(-1));
    _mm256_storeu_si256(reinterpret_cast<__m256i *>(byte_vector + k + 32), vbytes);
  }

  // Process last word in scalar loop
  if (auto tail_size = num_bits % 64; tail_size != 0) {
    uint64_t word = bit_vector[num_bits / 64];
    for (uint32_t i = 0; i < tail_size; i++, k++) {
      byte_vector[k] = -static_cast<uint8_t>((word & 0x1ull) == 1);
      word >>= 1u;
    }
  }
}

uint32_t VectorUtil::BitVectorToSelectionVectorSparse(const uint64_t *bit_vector, uint32_t num_bits,
                                                      sel_t *sel_vector) {
  const uint32_t num_words = common::MathUtil::DivRoundUp(num_bits, 64);

  uint32_t k = 0;
  for (uint32_t i = 0; i < num_words; i++) {
    uint64_t word = bit_vector[i];
    while (word != 0) {
      const uint64_t t = word & -word;
      const uint32_t r = BitUtil::CountTrailingZeros(word);
      sel_vector[k++] = i * 64 + r;
      word ^= t;
    }
  }
  return k;
}

uint32_t VectorUtil::BitVectorToSelectionVectorDenseAvX2(const uint64_t *bit_vector, uint32_t num_bits,
                                                         sel_t *sel_vector) {
  // Vector of '8's = [8,8,8,8,8,8,8]
  const auto eight = _mm_set1_epi16(8);

  // Selection vector write index
  auto idx = _mm_set1_epi16(0);

  // Selection vector size
  uint32_t k = 0;

  const uint32_t num_words = common::MathUtil::DivRoundUp(num_bits, 64);
  for (uint32_t i = 0; i < num_words; i++) {
    uint64_t word = bit_vector[i];
    for (uint32_t j = 0; j < 8; j++) {
      const auto mask = static_cast<uint8_t>(word);
      word >>= 8u;
      const __m128i match_pos_scaled =
          _mm_loadl_epi64(reinterpret_cast<const __m128i *>(&simd::K8_BIT_MATCH_LUT[mask]));
      const __m128i match_pos = _mm_cvtepi8_epi16(match_pos_scaled);
      const __m128i pos_vec = _mm_add_epi16(idx, match_pos);
      idx = _mm_add_epi16(idx, eight);
      _mm_storeu_si128(reinterpret_cast<__m128i *>(sel_vector + k), pos_vec);
      k += BitUtil::CountPopulation(static_cast<uint32_t>(mask));
    }
  }

  return k;
}

#if __AVX512VBMI2__
uint32_t VectorUtil::BitVectorToSelectionVectorDenseAVX512(const uint64_t *bit_vector, uint32_t num_bits,
                                                           sel_t *sel_vector) {
  const __m512i _32 = _mm512_set1_epi16(32);
  __m512i indexes = _mm512_set_epi16(31, 30, 29, 28, 27, 26, 25, 24, 23, 22, 21, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11,
                                     10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0);

  uint32_t k = 0;

  const uint32_t num_words = common::MathUtil::DivRoundUp(num_bits, 64);
  for (uint32_t i = 0; i < num_words; i++) {
    uint64_t word = bit_vector[i];

    // First word
    auto mask = _cvtu32_mask32(static_cast<uint32_t>(word));
    _mm512_mask_compressstoreu_epi16(sel_vector + k, mask, indexes);

    // Bump indexes
    _mm512_add_epi16(indexes, _32);
    k += BitUtil::CountPopulation(mask);

    // Second word
    mask = _cvtu32_mask32(static_cast<uint32_t>(word >> 32u));
    _mm512_mask_compressstoreu_epi16(sel_vector + k, mask, indexes);

    // Bump indexes again
    _mm512_add_epi16(indexes, _32);
    k += BitUtil::CountPopulation(mask);
  }

  return k;
}
#endif

uint32_t VectorUtil::BitVectorToSelectionVectorDense(const uint64_t *bit_vector, uint32_t num_bits, sel_t *sel_vector) {
#if __AVX512VBMI2__
  return BitVectorToSelectionVectorDenseAVX512(bit_vector, num_bits, sel_vector);
#else
  return BitVectorToSelectionVectorDenseAvX2(bit_vector, num_bits, sel_vector);
#endif
}

uint32_t VectorUtil::BitVectorToSelectionVector(const uint64_t *bit_vector, const uint32_t num_bits,
                                                sel_t *sel_vector) {
  // TODO(pmenon): For short bit vectors, like those used in vectorized execution (2048 bits), doing
  //               a full population count to determine sparsity/density is practical. In general,
  //               a full population count isn't necessary, and isn't feasible for long bit vectors.
  //               To solve this, we borrow a page from sample sort; classification only requires a
  //               sample (with an appropriate over sampling rate) of p = O(logn) words and their
  //               population counts. The sample is faster to collect and equally as representative
  //               as a full population count.

  const uint32_t num_words = common::MathUtil::DivRoundUp(num_bits, 64);

  // Calculate the density of the input bit vector in order to direct the algorithm to either the
  // sparse-optimized or dense-optimized unpacking implementation.

  uint64_t count = 0;
  for (uint64_t i = 0; i < num_words; i++) {
    count += util::BitUtil::CountPopulation(bit_vector[i]);
  }

  // TODO(pmenon): Use settings
  // Sparse is defined as < 15% ones
  const float density = static_cast<float>(count) / static_cast<float>(num_bits);
  return density <= 0.15 ? BitVectorToSelectionVectorSparse(bit_vector, num_bits, sel_vector)
                         : BitVectorToSelectionVectorDense(bit_vector, num_bits, sel_vector);
}

}  // namespace noisepage::execution::util
