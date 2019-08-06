#pragma once

#include <string>

#include "execution/util/bit_util.h"
#include "execution/util/common.h"
#include "execution/util/macros.h"

namespace llvm {
class StringRef;
}  // namespace llvm

namespace terrier {

/**
 * Info about the CPU.
 */
class CpuInfo {
 public:
  /**
   * CPU Features
   */
  enum Feature : u8 {
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
  enum CacheLevel : u8 { L1_CACHE = 0, L2_CACHE = 1, L3_CACHE = 2 };

  /**
   * Number of cache levels
   */
  static constexpr const u32 kNumCacheLevels = CacheLevel::L3_CACHE + 1;

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
  u32 GetNumCores() const noexcept { return num_cores_; }

  /**
   * Return the size of the cache at level \a level in bytes
   */
  u32 GetCacheSize(CacheLevel level) const noexcept { return cache_sizes_[level]; }

  /**
   * Return the size of a cache line at level \a level
   */
  u32 GetCacheLineSize(CacheLevel level) const noexcept { return cache_line_sizes_[level]; }

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
  u32 num_cores_;
  std::string model_name_;
  double cpu_mhz_;
  u32 cache_sizes_[kNumCacheLevels];
  u32 cache_line_sizes_[kNumCacheLevels];
  util::InlinedBitVector<64> hardware_flags_;
};

}  // namespace terrier
