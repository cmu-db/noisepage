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
  void BeginReplicationCommand(uint64_t message_id) { BeginPacket(NetworkMessageType::ITP_REPLICATION_COMMAND); }

  /**
   * End the Replication command
   */
  void EndReplicationCommand() { EndPacket(); }

  /**
   * Writes a Stop Replication packet
   */
  void StopReplicationCommand() { BeginPacket(NetworkMessageType::ITP_STOP_REPLICATION_COMMAND); }


  /**
   * Tells the client that the command is complete.
   */
  void WriteCommandComplete() {
    BeginPacket(NetworkMessageType::ITP_REPLICATION_COMPLETE).EndPacket();
  }
};

}  // namespace terrier::network
