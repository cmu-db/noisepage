#pragma once

#include "storage/recovery/abstract_log_provider.h"

namespace terrier::storage {

class ReplicationLogProvider {
 public:
  ReplicationLogProvider() {}

  virtual ~ReplicationLogProvider() = default;

  virtual void HandBufferToReplication(std::unique_ptr<network::ReadBuffer> buffer);
};
}  // namespace terrier::storage
