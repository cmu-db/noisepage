#pragma once

namespace terrier::metrics {

/**
 * Metric types
 */
enum class MetricsComponent : uint8_t { LOGGING, TRANSACTION, GARBAGECOLLECTION };

constexpr uint8_t NUM_COMPONENTS = 3;

enum class SamplingMask : uint32_t {
  SAMPLE_DISABLED = 0x0,
  SAMPLE_16 = 0xf,
  SAMPLE_128 = 0x7f,
  SAMPLE_1024 = 0x3ff,
};

}  // namespace terrier::metrics
