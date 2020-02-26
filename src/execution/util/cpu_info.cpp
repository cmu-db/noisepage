#include "execution/util/cpu_info.h"

#include <algorithm>
#include <memory>
#include <regex>  // NOLINT
#include <string>

#include "llvm/ADT/SmallVector.h"
#include "llvm/ADT/StringRef.h"

#if __APPLE__
#include <cpuid.h>
#include <sys/sysctl.h>
#include <sys/types.h>
#endif

#include <unistd.h>
#include <fstream>
#include <iomanip>
#include <sstream>

#include "loggers/execution_logger.h"

namespace terrier::execution {

struct {
  CpuInfo::Feature feature_;
  llvm::SmallVector<const char *, 4> names_;
} features[] = {
    {CpuInfo::SSE_4_2, {"sse4_2"}},
    {CpuInfo::AVX, {"avx"}},
    {CpuInfo::AVX2, {"avx2"}},
    {CpuInfo::AVX512, {"avx512f", "avx512cd"}},
};

int CpuInfo::GetCpuId() {
#ifdef __APPLE__
  uint32_t cpuinfo[4];
  __cpuid_count(1, 0, cpuinfo[0], cpuinfo[1], cpuinfo[2], cpuinfo[3]);
  if ((cpuinfo[3] & (1 << 9)) == 0) {
    return -1;
  }

  return (cpuinfo[3] >> 24);
#else
  return sched_getcpu();
#endif
}

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
    uint64_t freq = 0;
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
    } else if (name.startswith("model name")) {
      model_name_ = value.str();
      std::regex cpu_freq_regex("\\s[\\d.]+GHz");
      std::cmatch m;
      std::regex_search(model_name_.c_str(), m, cpu_freq_regex);
      double base_cpu_ghz = std::stod(m[0].str());
      ref_cycles_us_ = static_cast<uint64_t>(base_cpu_ghz * 1000);
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
  // Lookup cache sizes_
  std::size_t len = 0;
  sysctlbyname("hw.cachesize", nullptr, &len, nullptr, 0);
  auto data = std::make_unique<uint64_t[]>(len);
  sysctlbyname("hw.cachesize", data.get(), &len, nullptr, 0);
  TERRIER_ASSERT(len / sizeof(uint64_t) >= 3, "Expected three levels of cache!");

  // Copy data
  for (uint32_t idx = 0; idx < K_NUM_CACHE_LEVELS; idx++) {
    cache_sizes_[idx] = data[idx];
  }

  // Lookup cache line sizes_
  std::size_t linesize;
  std::size_t sizeof_linesize = sizeof(linesize);
  sysctlbyname("hw.cachelinesize", &linesize, &sizeof_linesize, nullptr, 0);
  for (auto &cache_line_size : cache_line_sizes_) {
    cache_line_size = linesize;
  }
#else
  // Use sysconf to determine cache sizes_
  cache_sizes_[L1_CACHE] = static_cast<uint32_t>(sysconf(_SC_LEVEL1_DCACHE_SIZE));
  cache_sizes_[L2_CACHE] = static_cast<uint32_t>(sysconf(_SC_LEVEL2_CACHE_SIZE));
  cache_sizes_[L3_CACHE] = static_cast<uint32_t>(sysconf(_SC_LEVEL3_CACHE_SIZE));

  cache_line_sizes_[L1_CACHE] = static_cast<uint32_t>(sysconf(_SC_LEVEL1_DCACHE_LINESIZE));
  cache_line_sizes_[L2_CACHE] = static_cast<uint32_t>(sysconf(_SC_LEVEL2_CACHE_LINESIZE));
  cache_line_sizes_[L3_CACHE] = static_cast<uint32_t>(sysconf(_SC_LEVEL3_CACHE_LINESIZE));
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
  ss << "    L1: " << (cache_sizes_[L1_CACHE] / 1024.0) << " common::Constants::KB (" << cache_line_sizes_[L1_CACHE] << " byte line)" << std::endl;  // NOLINT
  ss << "    L2: " << (cache_sizes_[L2_CACHE] / 1024.0) << " common::Constants::KB (" << cache_line_sizes_[L2_CACHE] << " byte line)" << std::endl;  // NOLINT
  ss << "    L3: " << (cache_sizes_[L3_CACHE] / 1024.0) << " common::Constants::KB (" << cache_line_sizes_[L3_CACHE] << " byte line)" << std::endl;  // NOLINT
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

}  // namespace terrier::execution
