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
   * Writes a Replication command
   * @param replication_data
   */
  void WriteReplicationCommand(const std::string& replication_data) {
    BeginPacket(NetworkMessageType::ITP_REPLICATION_COMMAND).AppendString(replication_data).EndPacket();
  }

};

}  // namespace terrier::network
