#pragma once

#include <array>
#include <bitset>

namespace terrier::metric {

/**
 * Metric types
 */
enum class MetricsComponent : uint8_t { TRANSACTION, LOGGING };

constexpr uint8_t NUM_COMPONENTS = 2;

/**
 * Triggering events for stats collection
 */
enum class MetricsEventType { TXN_BEGIN, TXN_COMMIT, TXN_ABORT, TUPLE_READ, TUPLE_UPDATE, TUPLE_INSERT, TUPLE_DELETE };

constexpr uint8_t NUM_EVENTS = 7;

static constexpr std::array<std::bitset<NUM_COMPONENTS>, NUM_EVENTS> event_dispatches = {
    0x1,  // TXN_BEGIN:     TRANSACTION
    0x1,  // TXN_COMMIT:    TRANSACTION
    0x1,  // TXN_ABORT:     TRANSACTION
    0x1,  // TUPLE_READ:    TRANSACTION
    0x1,  // TUPLE_UPDATE:  TRANSACTION
    0x1,  // TUPLE_INSERT:  TRANSACTION
    0x1   // TUPLE_DELETE:  TRANSACTION
};

inline bool MetricSupportsEvent(const MetricsEventType event, const MetricsComponent component) {
  return event_dispatches[static_cast<uint8_t>(event)].test(static_cast<uint8_t>(component));
}

}  // namespace terrier::metric
