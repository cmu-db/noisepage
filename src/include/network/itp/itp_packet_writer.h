#pragma once

#include <memory>
#include <string>
#include <vector>

#include "network/packet_writer.h"

namespace noisepage::network {
/**
 * Wrapper around an I/O layer WriteQueue to provide ITP-specific
 * helper methods.
 */
class ITPPacketWriter : public PacketWriter {
 public:
  /**
   * Instantiates a new ITPPacketWriter backed by the given WriteQueue
   */
  explicit ITPPacketWriter(common::ManagedPointer<WriteQueue> write_queue) : PacketWriter(write_queue) {}

  /**
   * Create a Replication command
   * --------------------------------------------------------------------------------------------------
   * | message type (char) | message id (uint64_t) | data size (uint64_t) | replication data (varlen) |
   * --------------------------------------------------------------------------------------------------
   * This begins the creation of the Replication command. After this is called , we can append further
   * bytes to the packet and call EndReplicationCommand when we want to finish the current command.
   * @param message_id message id
   */
  void BeginReplicationCommand(uint64_t message_id) { BeginPacket(NetworkMessageType::ITP_REPLICATION_COMMAND); }

  /**
   * End the Replication command
   */
  void EndReplicationCommand() { EndPacket(); }

  /**
   * Writes a Stop Replication packet
   */
  void StopReplicationCommand() { BeginPacket(NetworkMessageType::ITP_STOP_REPLICATION_COMMAND).EndPacket(); }

  /**
   * Tells the client that the command is complete.
   */
  void WriteCommandComplete() { BeginPacket(NetworkMessageType::ITP_COMMAND_COMPLETE).EndPacket(); }
};

}  // namespace noisepage::network
