#pragma once

#include <array>
#include <bitset>

namespace terrier::metric {

/**
 * Metric types
 */
enum class MetricsComponent : uint8_t { SYSTEM, DATABASE, TABLE, INDEX, TRANSACTION, LOGGING };

constexpr uint8_t num_components = 6;

/**
 * Triggering events for stats collection
 */
enum class MetricsEventType { TXN_BEGIN, TXN_COMMIT, TXN_ABORT, TUPLE_READ, TUPLE_UPDATE, TUPLE_INSERT, TUPLE_DELETE };

constexpr uint8_t num_events = 7;

constexpr std::array<std::bitset<num_components>, num_events> event_dispatches UNUSED_ATTRIBUTE = {
    0x10,  // TXN_BEGIN:     TRANSACTION
    0x10,  // TXN_COMMIT:    TRANSACTION
    0x10,  // TXN_ABORT:     TRANSACTION
    0x10,  // TUPLE_READ:    TRANSACTION
    0x10,  // TUPLE_UPDATE:  TRANSACTION
    0x10,  // TUPLE_INSERT:  TRANSACTION
    0x10   // TUPLE_DELETE:  TRANSACTION
};

inline bool MetricSupportsEvent(const MetricsEventType event, const MetricsComponent component) {
  return event_dispatches[static_cast<uint8_t>(event)].test(static_cast<uint8_t>(component));
}

}  // namespace terrier::metric