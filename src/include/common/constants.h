#pragma once

#include <cstdint>

namespace noisepage::common {
/**
 * Declare all system-level constants that cannot change at runtime here.
 */
struct Constants {
  /**
   * Block/RawBlock size, in bytes. Must be a power of 2.
   */
  static const uint32_t BLOCK_SIZE = 1 << 20;
  /**
   * Buffer segment size, in bytes.
   */
  static const uint32_t BUFFER_SEGMENT_SIZE = 1 << 12;
  /**
   * Maximum number of columns a table is allowed to have. It should be sufficiently small such that  if all
   * columns are as large as they can be there is still at last one slot for every block.
   */
  // TODO(Tianyu): This number currently is obtained through empirical experiments.
  static const uint16_t MAX_COL = 12500;

  /**
   * The size of the buffers the log manager uses to buffer serialized logs and "group commit" them when writing to disk
   */
  static const uint32_t LOG_BUFFER_SIZE = (1 << 12);

  /**
   * The cache line size in bytes
   */
  static const uint8_t CACHELINE_SIZE = 64;

  /**
   * The number of bits per byte
   */
  static constexpr const uint32_t K_BITS_PER_BYTE = 8;

  /**
   * The default vector size to use when performing vectorized iteration
   */
  static constexpr const uint32_t K_DEFAULT_VECTOR_SIZE = 2048;

  /**
   * The default prefetch distance to use
   */
  static constexpr const uint32_t K_PREFETCH_DISTANCE = 16;

  // Common memory sizes
  /**
   * KB
   */
  static constexpr const uint32_t KB = 1024;

  /**
   * MB
   */
  static constexpr const uint32_t MB = KB * KB;

  /**
   * GB
   */
  static constexpr const uint32_t GB = KB * KB * KB;

  /**
   * When performing selections, rather than operating only on active elements
   * in a TID list, it may be faster to apply the selection on ALL elements and
   * quickly mask out unselected TIDS. Such an optimization removes branches
   * from the loop and allows the compiler to auto-vectorize the operation.
   * However, this optimization wins only when the selectivity of the input TID
   * list is greater than a threshold value. This threshold can vary between
   * platforms and data types. Thus, we derive the threshold at database startup
   * once using the given function.
   */
  static constexpr const double SELECT_OPT_THRESHOLD = 0.25;

  /**
   * When performing arithmetic operations on vectors, this setting determines
   * the minimum required vector selectivity before switching to a full-compute
   * implementation. A full computation is one that ignores the selection vector
   * or filtered TID list of the input vectors and blindly operators on all
   * vector elements. Though wasteful, the algorithm is amenable to
   * auto-vectorization by the compiler yielding better overall performance.
   */
  static constexpr const double ARITHMETIC_FULL_COMPUTE_THRESHOLD = 0.05;

  /**
   * The minimum bit vector density before using a SIMD decoding algorithm.
   */
  static constexpr const float BIT_DENSITY_THRESHOLD_FOR_AVX_INDEX_DECODE = 0.15;

  /**
   * The frequency at which to sample statistics when adaptively reordering
   * predicate clauses falling in the range [0.0, 1.0]. A low frequency incurs
   * minimal runtime overhead, but is less reactive to changing distributions in
   * the underlying data. A high re-sampling frequency is more adaptive, but
   * incurs higher runtime overhead. Thus, there is a trade-off here.
   */
  static constexpr const float ADAPTIVE_PRED_ORDER_SAMPLE_FREQ = 0.1;

  /**
   * Flag indicating if parallel execution is enabled.
   */
  static constexpr const bool IS_PARALLEL_EXECUTION_ENABLED = true;

  /**
   * Number of threads for parallel execution
   * This value will be overwritten by the SettingsManager (if enabled).
   */
  static constexpr const int NUM_PARALLEL_EXECUTION_THREADS = -1;

  /**
   * Flag indicating if counters is enabled
   * This value will be overwritten by the SettingsManager (if enabled).
   */
  static constexpr const bool IS_COUNTERS_ENABLED = false;

  /**
   * Flag indicating if pipeline metrics are enabled
   * This value will be overwritten by the SettingsManager (if enabled).
   */
  static constexpr const bool IS_PIPELINE_METRICS_ENABLED = true;

  /**
   * Flag indicating if static partitioner is used
   */
  static constexpr const bool IS_STATIC_PARTITIONER_ENABLED = false;
};
}  // namespace noisepage::common
