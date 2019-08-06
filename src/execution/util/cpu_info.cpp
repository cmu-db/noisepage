#include "execution/util/cpu_info.h"

#include <algorithm>
#include <memory>
#include <string>

#include "llvm/ADT/SmallVector.h"
#include "llvm/ADT/StringRef.h"

#if __APPLE__
#include <sys/sysctl.h>
#include <sys/types.h>
#endif

#include <unistd.h>
#include <fstream>
#include <iomanip>
#include <sstream>

#include "loggers/execution_logger.h"

namespace terrier {

struct {
  CpuInfo::Feature feature;
  llvm::SmallVector<const char *, 4> names;
} features[] = {
    {CpuInfo::SSE_4_2, {"sse4_2"}},
    {CpuInfo::AVX, {"avx"}},
    {CpuInfo::AVX2, {"avx2"}},
    {CpuInfo::AVX512, {"avx512f", "avx512cd"}},
};

void CpuInfo::ParseCpuFlags(llvm::StringRef flags) {
  for (const auto &[feature, names] : features) {
    bool has_feature = true;

    // Check if all feature flag names exist in the flags string. Only if all
    // exist do we claim the whole feature exists.
    for (const auto &name : names) {
      if (!flags.contains(name)) {
        has_feature = false;
        break;
      }
    }

    // Set or don't
    if (has_feature) {
      hardware_flags_.Set(feature);
    } else {
      hardware_flags_.Unset(feature);
    }
  }
}

CpuInfo::CpuInfo() {
  InitCpuInfo();
  InitCacheInfo();
}

void CpuInfo::InitCpuInfo() {
#ifdef __APPLE__
  // On MacOS, use sysctl
  {
    size_t size = sizeof(num_cores_);
    if (sysctlbyname("hw.ncpu", &num_cores_, &size, nullptr, 0) < 0) {
      EXECUTION_LOG_ERROR("Cannot read # CPUs: {}", strerror(errno));
    }
  }

  {
    u64 freq = 0;
    size_t size = sizeof(freq);
    if (sysctlbyname("hw.cpufrequency", &freq, &size, nullptr, 0) < 0) {
      EXECUTION_LOG_ERROR("Cannot read CPU Mhz: {}", strerror(errno));
    }
    cpu_mhz_ = static_cast<double>(freq) / 1000000.0;
  }
#else
  // On linux, just read /proc/cpuinfo
  std::string line;
  std::ifstream infile("/proc/cpuinfo");
  while (std::getline(infile, line)) {
    llvm::StringRef str(line);
    // NOLINTNEXTLINE
    auto [name, value] = str.split(":");
    value = value.trim(" ");

    if (name.startswith("processor")) {
      num_cores_++;
    } else if (name.startswith("model")) {
      model_name_ = value.str();
    } else if (name.startswith("cpu MHz")) {
      double cpu_mhz;
      value.getAsDouble(cpu_mhz);
      cpu_mhz_ = std::max(cpu_mhz_, cpu_mhz);
    } else if (name.startswith("flags")) {
      ParseCpuFlags(value);
    }
  }
#endif
}

void CpuInfo::InitCacheInfo() {
#ifdef __APPLE__
  // Lookup cache sizes
  std::size_t len = 0;
  sysctlbyname("hw.cachesize", nullptr, &len, nullptr, 0);
  auto data = std::make_unique<u64[]>(len);
  sysctlbyname("hw.cachesize", data.get(), &len, nullptr, 0);
  TPL_ASSERT(len / sizeof(uint64_t) >= 3, "Expected three levels of cache!");

  // Copy data
  for (u32 idx = 0; idx < kNumCacheLevels; idx++) {
    cache_sizes_[idx] = data[idx];
  }

  // Lookup cache line sizes
  std::size_t linesize;
  std::size_t sizeof_linesize = sizeof(linesize);
  sysctlbyname("hw.cachelinesize", &linesize, &sizeof_linesize, nullptr, 0);
  for (auto &cache_line_size : cache_line_sizes_) {
    cache_line_size = linesize;
  }
#else
  // Use sysconf to determine cache sizes
  cache_sizes_[L1_CACHE] = static_cast<u32>(sysconf(_SC_LEVEL1_DCACHE_SIZE));
  cache_sizes_[L2_CACHE] = static_cast<u32>(sysconf(_SC_LEVEL2_CACHE_SIZE));
  cache_sizes_[L3_CACHE] = static_cast<u32>(sysconf(_SC_LEVEL3_CACHE_SIZE));

  cache_line_sizes_[L1_CACHE] = static_cast<u32>(sysconf(_SC_LEVEL1_DCACHE_LINESIZE));
  cache_line_sizes_[L2_CACHE] = static_cast<u32>(sysconf(_SC_LEVEL2_CACHE_LINESIZE));
  cache_line_sizes_[L3_CACHE] = static_cast<u32>(sysconf(_SC_LEVEL3_CACHE_LINESIZE));
#endif
}

std::string CpuInfo::PrettyPrintInfo() const {
  std::stringstream ss;

  // clang-format off
  ss << "CPU Info: " << std::endl;
  ss << "  Model:  " << model_name_ << std::endl;
  ss << "  Cores:  " << num_cores_ << std::endl;
  ss << "  Mhz:    " << std::fixed << std::setprecision(2) << cpu_mhz_ << std::endl;
  ss << "  Caches: " << std::endl;
  ss << "    L1: " << (cache_sizes_[L1_CACHE] / 1024.0) << " KB (" << cache_line_sizes_[L1_CACHE] << " byte line)" << std::endl;  // NOLINT
  ss << "    L2: " << (cache_sizes_[L2_CACHE] / 1024.0) << " KB (" << cache_line_sizes_[L2_CACHE] << " byte line)" << std::endl;  // NOLINT
  ss << "    L3: " << (cache_sizes_[L3_CACHE] / 1024.0) << " KB (" << cache_line_sizes_[L3_CACHE] << " byte line)" << std::endl;  // NOLINT
  // clang-format on

  ss << "Features: ";
  for (const auto &[feature, names] : features) {
    if (HasFeature(feature)) {
      for (const auto &name : names) {
        ss << name << " ";
      }
    }
  }
  ss << std::endl;

  return ss.str();
}

}  // namespace terrier
