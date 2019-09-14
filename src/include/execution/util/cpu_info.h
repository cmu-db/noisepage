#pragma once

#include <string>

#include "common/macros.h"
#include "execution/util/bit_util.h"
#include "execution/util/execution_common.h"

namespace llvm {
class StringRef;
}  // namespace llvm

namespace terrier::execution {

/**
 * Info about the CPU.
 */
class CpuInfo {
 public:
  /**
   * CPU Features
   */
  enum Feature : uint8_t {
    SSE_4_2 = 0,
    AVX = 1,
    AVX2 = 2,
    AVX512 = 3,
  };

  // -------------------------------------------------------
  // Caches
  // -------------------------------------------------------

  /**
   * Cache levels
   */
  enum CacheLevel : uint8_t { L1_CACHE = 0, L2_CACHE = 1, L3_CACHE = 2 };

  /**
   * Number of cache levels
   */
  static constexpr const uint32_t K_NUM_CACHE_LEVELS = CacheLevel::L3_CACHE + 1;

  // -------------------------------------------------------
  // Main API
  // -------------------------------------------------------

  /**
   * Singletons are bad blah blah blah
   */
  static CpuInfo *Instance() {
    static CpuInfo instance;
    return &instance;
  }

  /**
   * Return the number of logical cores in the system
   */
  uint32_t GetNumCores() const noexcept { return num_cores_; }

  /**
   * Return the size of the cache at level \a level in bytes
   */
  uint32_t GetCacheSize(CacheLevel level) const noexcept { return cache_sizes_[level]; }

  /**
   * Return the size of a cache line at level \a level
   */
  uint32_t GetCacheLineSize(CacheLevel level) const noexcept { return cache_line_sizes_[level]; }

  /**
   * Does the CPU have the given hardware feature?
   */
  bool HasFeature(Feature feature) const noexcept { return hardware_flags_.Test(feature); }

  /**
   * Pretty print CPU information to a string
   */
  std::string PrettyPrintInfo() const;

 private:
  // Initialize cpu info
  void InitCpuInfo();
  // Initialize cache info
  void InitCacheInfo();
  // Parse cpu flags
  void ParseCpuFlags(llvm::StringRef flags);

 private:
  // Constructor
  CpuInfo();

 private:
  uint32_t num_cores_;
  std::string model_name_;
  double cpu_mhz_;
  uint32_t cache_sizes_[K_NUM_CACHE_LEVELS];
  uint32_t cache_line_sizes_[K_NUM_CACHE_LEVELS];
  util::InlinedBitVector<64> hardware_flags_;
};

}  // namespace terrier::execution
