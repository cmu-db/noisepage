#pragma once

#include <memory>

#include "storage/recovery/abstract_log_provider.h"

namespace noisepage::storage {

/**
 * TODO(Tianlei/Gus): Replace this file with Gus's version
 */
class ReplicationLogProvider {
 public:
  /**
   * Passes the content of the buffer to traffic cop
   * @param buffer content to pass to traffic cop
   */
  virtual void HandBufferToReplication(std::unique_ptr<network::ReadBuffer> buffer);
};
}  // namespace noisepage::storage
