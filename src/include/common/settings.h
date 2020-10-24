#pragma once

#include <cstdint>

namespace noisepage::common {
/**
 * Declare all system-level settings here.
 * There should be no hard-coded values in the code.
 *
 * NOTE: it is expected that all values defined here will eventually
 *   be supplied by a settings manager. Values handled by a settings manager
 *   should be removed from this file.
 */

struct Settings {
  /**
   * Server connection backlog.
   */
  static const int CONNECTION_BACKLOG = 12;
};
}  // namespace noisepage::common
