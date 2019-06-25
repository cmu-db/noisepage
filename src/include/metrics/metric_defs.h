#pragma once

#include <array>
#include <bitset>

namespace terrier::metrics {

/**
 * Metric types
 */
enum class MetricsComponent : uint8_t { TRANSACTION, LOGGING };

constexpr uint8_t NUM_COMPONENTS = 2;

/**
 * Triggering events for stats collection
 */
enum class MetricsEventType {
  TXN_BEGIN,
  TXN_COMMIT,
  TXN_ABORT,
  TUPLE_READ,
  TUPLE_UPDATE,
  TUPLE_INSERT,
  TUPLE_DELETE,
  LOG_SERIALIZE,
  LOG_CONSUME
};

constexpr uint8_t NUM_EVENTS = 9;

static constexpr std::array<std::bitset<NUM_COMPONENTS>, NUM_EVENTS> event_dispatches = {
    0x1,  // TXN_BEGIN:     TRANSACTION
    0x1,  // TXN_COMMIT:    TRANSACTION
    0x1,  // TXN_ABORT:     TRANSACTION
    0x1,  // TUPLE_READ:    TRANSACTION
    0x1,  // TUPLE_UPDATE:  TRANSACTION
    0x1,  // TUPLE_INSERT:  TRANSACTION
    0x1,  // TUPLE_DELETE:  TRANSACTION
    0x2,  // LOG_SERIALIZE: LOGGING
    0x2   // LOG_CONSUME:   LOGGING
};

inline bool MetricSupportsEvent(const MetricsEventType event,  // NOLINT: correct use in modern C++
                                const MetricsComponent component) {
  return event_dispatches[static_cast<uint8_t>(event)].test(static_cast<uint8_t>(component));
}

}  // namespace terrier::metrics
