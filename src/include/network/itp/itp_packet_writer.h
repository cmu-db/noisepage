#pragma once

#include <memory>

#include "network/abstract_packet_writer.h"

namespace terrier::network {
/**
 * Wrapper around an I/O layer WriteQueue to provide ITP-sprcific
 * helper methods.
 */
class ITPPacketWriter : AbstractPacketWriter {
 public:
  /**
   * Instantiates a new ITPPacketWriter backed by the given WriteQueue
   */
  explicit ITPPacketWriter(const std::shared_ptr<WriteQueue> &write_queue) : AbstractPacketWriter(write_queue) {}
};

}  // namespace terrier::network
