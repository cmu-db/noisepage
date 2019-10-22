#pragma once

#include <memory>
#include "storage/recovery/abstract_log_provider.h"

namespace terrier::storage {

/**
 * TODO(Tianlei/Gus): Replace this file with Gus's version
 */
class ReplicationLogProvider {
 public:
  ReplicationLogProvider() {}

  virtual ~ReplicationLogProvider() = default;

  virtual void HandBufferToReplication(std::unique_ptr<network::ReadBuffer> buffer);
};
}  // namespace terrier::storage
