#pragma once
#include <functional>
#include <memory>
#include "common/managed_pointer.h"
#include "network/connection_context.h"
#include "network/network_io_utils.h"
#include "network/network_types.h"
//
namespace terrier::network {

class ConnectionHandle;

/**
 * Interface to communicate with a client via a certain network protocol
 */
class ProtocolInterpreter {
 public:
  /**
   * A Provider interface is a strategy object for construction.
   *
   * It encapsulates creation logic that can be passed around as polymorphic objects. Inject the
   * approriate subclass of this object to the connection dispatcher in order to bind them to
   * the correct protocol type.
   */
  struct Provider {
    virtual ~Provider() = default;
    /**
     * @return a constructed instance of protocol interpreter
     */
    virtual std::unique_ptr<ProtocolInterpreter> Get() = 0;
  };
  /**
   * Processes client's input that has been fed into the given ReadBufer
   * @param in The ReadBuffer to read input from
   * @param out The WriteQueue to communicate with the client through
   * @param t_cop The traffic cop pointer
   * @param context the connection context
   * @param callback The callback function to trigger on completion
   * @return The next transition for the client's associated state machine
   */
  virtual Transition Process(std::shared_ptr<ReadBuffer> in, std::shared_ptr<WriteQueue> out,
                             common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                             common::ManagedPointer<ConnectionContext> context, NetworkCallback callback) = 0;

  /**
   * Sends a result
   * @param out
   */
  virtual void GetResult(std::shared_ptr<WriteQueue> out) = 0;

  /**
   * Default destructor for ProtocolInterpreter
   */
  virtual ~ProtocolInterpreter() = default;

 protected:

  virtual size_t GetPacketHeaderSize() = 0;

  virtual void SetPacketMessageType(InputPacket& curr_input_packet) = 0;
  
  /**
   * Reads the header of the packet to see if it is valid
   */
  bool TryReadPacketHeader(const std::shared_ptr<ReadBuffer> &in, InputPacket& curr_input_packet) {
    if (curr_input_packet.header_parsed_) return true;

    // Header format: 1 byte message type (only if non-startup)
    //              + 4 byte message size (inclusive of these 4 bytes)
    size_t header_size = GetPacketHeaderSize(); 
    // Make sure the entire header is readable
    if (!in->HasMore(header_size)) return false;

    // The header is ready to be read, fill in fields accordingly
    SetPacketMessageType(curr_input_packet);
    curr_input_packet.len_ = in->ReadValue<uint32_t>() - sizeof(uint32_t);
    if (curr_input_packet.len_ > PACKET_LEN_LIMIT) {
      NETWORK_LOG_ERROR("Packet size {} > limit {}", curr_input_packet.len_, PACKET_LEN_LIMIT);
      throw NETWORK_PROCESS_EXCEPTION("Packet too large");
    }

    // Extend the buffer as needed
    if (curr_input_packet.len_ > in->Capacity()) {
      // Allocate a larger buffer and copy bytes off from the I/O layer's buffer
      curr_input_packet.buf_ = std::make_shared<ReadBuffer>(curr_input_packet.len_);
      NETWORK_LOG_TRACE("Extended Buffer size required for packet of size {0}", curr_input_packet.len_);
      curr_input_packet.extended_ = true;
    } else {
      curr_input_packet.buf_ = in;
    }

    curr_input_packet.header_parsed_ = true;
    return true;
  }

  /**
   * Build the packet if it is valid
   */
  bool TryBuildPacket(const std::shared_ptr<ReadBuffer>& in, InputPacket& curr_input_packet) {
    if (!TryReadPacketHeader(in)) return false;

    size_t size_needed = curr_input_packet.extended_
                            ? curr_input_packet.len_ - curr_input_packet.buf_->BytesAvailable()
                            : curr_input_packet.len_;

    size_t can_read = std::min(size_needed, in->BytesAvailable());
    size_t remaining_bytes = size_needed - can_read;

    // copy bytes only if the packet is longer than the read buffer,
    // otherwise we can use the read buffer to save space
    if (curr_input_packet.extended_) {
      curr_input_packet.buf_->FillBufferFrom(*in, can_read);
    }

    return remaining_bytes <= 0;
  }
};
//
}  // namespace terrier::network
