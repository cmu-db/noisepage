#pragma once

#include <bitset>
#include <string>

namespace llvm {
class StringRef;
}  // namespace llvm

namespace noisepage::execution {

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

    // Don't add any features below this comment. If you add more features, remember to modify the value of MAX below.
    MAX,
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
   * @return The current CPU's ID.
   */
  static int GetCpuId();

  /**
   * @return The total number of physical processor packages in the system.
   */
  uint32_t GetNumProcessors() const noexcept { return num_processors_; }

  /**
   * @return The total number of physical cores in the system.
   */
  uint32_t GetNumPhysicalCores() const noexcept { return num_physical_cores_; }

  /**
   * @return The total number of logical cores in the system.
   */
  uint32_t GetNumLogicalCores() const noexcept { return num_logical_cores_; }

  /**
   * @return The size of the cache at level @em level in bytes.
   */
  uint32_t GetCacheSize(const CacheLevel level) const noexcept { return cache_sizes_[level]; }

  /**
y   * @return The size of a cache line at level @em level.
   */
  uint32_t GetCacheLineSize(const CacheLevel level) const noexcept { return cache_line_sizes_[level]; }

  /**
   * @return The number of reference cycles advanced per microsecond.
   */
  uint64_t GetRefCyclesUs() const { return ref_cycles_us_; }

  /**
   * @return The CPU Frequency in MHz recorded at initialization
   */
  double GetCpuFreq() const { return cpu_mhz_; }

  /**
   * @return True if the CPU has the input hardware feature @em feature; false otherwise;
   */
  bool HasFeature(const Feature feature) const noexcept { return hardware_flags_[feature]; }

  /**
   * Pretty print CPU information to a string.
   * @return A string-representation of the CPU information.
   */
  std::string PrettyPrintInfo() const;

 private:
  void InitCpuInfo();
  void InitCacheInfo();
  void ParseCpuFlags(llvm::StringRef flags);

 private:
  CpuInfo();

 private:
  uint32_t num_logical_cores_;
  uint32_t num_physical_cores_;
  uint32_t num_processors_;
  std::string model_name_;
  double cpu_mhz_;
  uint64_t ref_cycles_us_;
  uint32_t cache_sizes_[K_NUM_CACHE_LEVELS];
  uint32_t cache_line_sizes_[K_NUM_CACHE_LEVELS];
  std::bitset<Feature::MAX> hardware_flags_;
};

}  // namespace noisepage::execution
