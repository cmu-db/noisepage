#pragma once
#include <algorithm>
#include <functional>
#include <memory>

#include "common/managed_pointer.h"
#include "loggers/network_logger.h"
#include "network/connection_context.h"
#include "network/network_io_utils.h"
#include "network/network_types.h"
//
namespace noisepage::network {

class ConnectionHandle;

/**
 * Interface to communicate with a client via a certain network protocol
 */
class ProtocolInterpreter {
 public:
  /**
   * Processes client's input that has been fed into the given ReadBufer
   * @param in The ReadBuffer to read input from
   * @param out The WriteQueue to communicate with the client through
   * @param t_cop The traffic cop pointer
   * @param context the connection context
   * @return The next transition for the client's associated state machine
   */
  virtual Transition Process(common::ManagedPointer<ReadBuffer> in, common::ManagedPointer<WriteQueue> out,
                             common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                             common::ManagedPointer<ConnectionContext> context) = 0;

  /**
   * Tear down any state and perform any connection closing duties for this protocol
   * @param in The ReadBuffer to read input from
   * @param out The WriteQueue to communicate with the client through
   * @param t_cop The traffic cop pointer
   * @param context the connection context
   * @return The next transition for the client's associated state machine
   */
  virtual void Teardown(common::ManagedPointer<ReadBuffer> in, common::ManagedPointer<WriteQueue> out,
                        common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                        common::ManagedPointer<ConnectionContext> context) = 0;

  /**
   * Sends a result
   * @param out The WriteQueue to communicate with the client through
   */
  virtual void GetResult(common::ManagedPointer<WriteQueue> out) = 0;

  /**
   * Default destructor for ProtocolInterpreter
   */
  virtual ~ProtocolInterpreter() = default;

 protected:
  /**
   * Where the data is being forwarded to
   */
  InputPacket curr_input_packet_{};

  /**
   * Return the size of the packet header
   * @return size of header
   */
  virtual size_t GetPacketHeaderSize() = 0;

  /**
   * Sets the message type of the current packet
   * @param in ReadBuffer to read input from
   */
  virtual void SetPacketMessageType(common::ManagedPointer<ReadBuffer> in) = 0;

  /**
   * Reads the header of the packet to see if it is valid
   * @param in The ReadBuffer to read input from
   * @return whether the packet header is valid or not
   */
  bool TryReadPacketHeader(const common::ManagedPointer<ReadBuffer> in) {
    if (curr_input_packet_.header_parsed_) return true;

    // Header format: 1 byte message type (only if non-startup)
    //              + 4 byte message size (inclusive of these 4 bytes)
    size_t header_size = GetPacketHeaderSize();
    // Make sure the entire header is readable
    if (!in->HasMore(header_size)) return false;

    // The header is ready to be read, fill in fields accordingly
    SetPacketMessageType(in);
    curr_input_packet_.len_ = in->ReadValue<uint32_t>() - sizeof(uint32_t);
    if (curr_input_packet_.len_ > PACKET_LEN_LIMIT) {
      NETWORK_LOG_ERROR("Packet size {} > limit {}", curr_input_packet_.len_, PACKET_LEN_LIMIT);
      throw NETWORK_PROCESS_EXCEPTION("Packet too large");
    }

    NOISEPAGE_ASSERT(!curr_input_packet_.extended_,
                     "InputPacket shouldn't already be extended before beginning parsing of the body.");

    // Extend the buffer as needed
    if (curr_input_packet_.len_ > in->Capacity()) {
      // Allocate a larger buffer and copy bytes off from the I/O layer's buffer
      curr_input_packet_.buf_ = new ReadBuffer(curr_input_packet_.len_);
      NETWORK_LOG_TRACE("Extended Buffer size required for packet of size {0}", curr_input_packet_.len_);
      curr_input_packet_.extended_ = true;
    } else {
      curr_input_packet_.buf_ = in.Get();
    }

    curr_input_packet_.header_parsed_ = true;
    return true;
  }

  /**
   * Build the packet if it is valid
   * @param in The ReadBuffer to read input from
   * @return whether the packet is valid or not
   */
  bool TryBuildPacket(const common::ManagedPointer<ReadBuffer> in) {
    if (!TryReadPacketHeader(in)) return false;

    size_t size_needed = curr_input_packet_.extended_
                             ? curr_input_packet_.len_ - curr_input_packet_.buf_->BytesAvailable()
                             : curr_input_packet_.len_;

    size_t can_read = std::min(size_needed, in->BytesAvailable());
    size_t remaining_bytes = size_needed - can_read;

    // copy bytes only if the packet is longer than the read buffer,
    // otherwise we can use the read buffer to save space
    if (curr_input_packet_.extended_) {
      NOISEPAGE_ASSERT(
          curr_input_packet_.len_ == curr_input_packet_.buf_->Capacity(),
          "The buffer should have been extended to support the packet length. Otherwise, there's a mismatch "
          "between the extended_ flag.");
      curr_input_packet_.buf_->FillBufferFrom(in, can_read);
    }

    return remaining_bytes <= 0;
  }
};

/**
 * A Provider interface is a strategy object for construction.
 *
 * It encapsulates creation logic that can be passed around as polymorphic objects. Inject the
 * approriate subclass of this object to the connection dispatcher in order to bind them to
 * the correct protocol type.
 */
class ProtocolInterpreterProvider {
 public:
  virtual ~ProtocolInterpreterProvider() = default;
  /**
   * @return a constructed instance of protocol interpreter
   */
  virtual std::unique_ptr<ProtocolInterpreter> Get() = 0;
};

}  // namespace noisepage::network
