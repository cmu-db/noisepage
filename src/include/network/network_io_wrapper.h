#pragma once

#include <memory>
#include <utility>

#include "common/exception.h"
#include "common/utility.h"

#include "network/network_io_utils.h"
#include "network/network_types.h"

namespace terrier::network {

/**
 * A network io wrapper implements an interface for interacting with a client
 * connection.
 *
 * Underneath the hood the wrapper buffers read and write, and supports posix
 * and ssl reads and writes to the socket.
 *
 * Because the buffers are large and expensive to allocate on fly, they are
 * reused. Consequently, initialization of this class is handled by a factory
 * class.
 */

class NetworkIoWrapper {
 public:
  /**
   * Barring copying and moving of PosixSocketIoWrapper instances
   */
  DISALLOW_COPY_AND_MOVE(NetworkIoWrapper);

  /**
   * @brief Constructor for a PosixSocketIoWrapper
   * @param sock_fd The fd this IoWrapper communicates on
   * @param in The ReadBuffer this NetworkIOWrapper uses for reads
   * @param out The WriteQueue this NetworkIOWrapper uses for writes
   */
  explicit NetworkIoWrapper(int sock_fd, std::shared_ptr<ReadBuffer> in = std::make_shared<ReadBuffer>(),
                            std::shared_ptr<WriteQueue> out = std::make_shared<WriteQueue>())
      : sock_fd_(sock_fd), in_(std::move(in)), out_(std::move(out)) {
    in_->Reset();
    out_->Reset();
    RestartState();
  }

  /**
   * @return whether or not SSL is able to be handled by this IOWrapper
   */
  bool SslAble() const { return false; }

  /**
   * @brief Fills the read buffer of this IOWrapper from the assigned fd
   * @return The next transition for this client's state machine
   */
  Transition FillReadBuffer();

  /**
   * @return Whether or not this IOWrapper is configured to flush its writes when this is called
   */
  bool ShouldFlush() { return out_->ShouldFlush(); }

  /**
   * @brief Flushes the write buffer of this IOWrapper to the assigned fd
   * @return The next transition for this client's state machine
   */
  Transition FlushWriteBuffer(WriteBuffer *wbuf);

  /**
   * @brief Flushes all writes to this IOWrapper
   * @return The next transition for this client's state machine
   */
  Transition FlushAllWrites();

  /**
   * @brief Closes this IOWrapper
   * @return The next transition for this client's state machine
   */
  Transition Close() {
    TerrierClose(sock_fd_);
    return Transition::PROCEED;
  }

  /**
   * @brief Restarts this IOWrapper
   */
  void Restart();

  /**
   * @return The socket file descriptor this IOWrapper communciates on
   */
  int GetSocketFd() { return sock_fd_; }

  /**
   * @return The ReadBuffer for this IOWrapper
   */
  std::shared_ptr<ReadBuffer> GetReadBuffer() { return in_; }

  /**
   * @return The WriteQueue for this IOWrapper
   */
  std::shared_ptr<WriteQueue> GetWriteQueue() { return out_; }

 private:
  // The file descriptor associated with this NetworkIoWrapper
  int sock_fd_;
  // The ReadBuffer associated with this NetworkIoWrapper
  std::shared_ptr<ReadBuffer> in_;
  // The WriteQueue associated with this NetworkIoWrapper
  std::shared_ptr<WriteQueue> out_;

  void RestartState();
};
}  // namespace terrier::network
