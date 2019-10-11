#pragma once

#include <memory>
#include <string>
#include <vector>

#include "network/packet_writer.h"

namespace terrier::network {
/**
 * Wrapper around an I/O layer WriteQueue to provide ITP-specific
 * helper methods.
 */
class ITPPacketWriter : public PacketWriter {
 public:
  /**
   * Instantiates a new ITPPacketWriter backed by the given WriteQueue
   */
  explicit ITPPacketWriter(const std::shared_ptr<WriteQueue> &write_queue) : PacketWriter(write_queue) {}

  /**
   * Create a Replication command
   * --------------------------------------------------------------------------------------------------
   * | message type (char) | message id (uint64_t) | data size (uint64_t) | replication data (varlen) |
   * --------------------------------------------------------------------------------------------------
   * @param message_id message id
   * @param data_size total size of replication data to be added
   */
  void BeginReplicationCommand(uint64_t message_id, uint64_t data_size) {
    BeginPacket(NetworkMessageType::ITP_REPLICATION_COMMAND);
    AppendValue<uint64_t>(message_id);
    AppendValue<uint64_t>(data_size);
  }

  /**
   * End the Replication command
   */
  void EndReplicationCommnad() {
    EndPacket();
  }

};

}  // namespace terrier::network
