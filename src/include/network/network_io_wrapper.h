#pragma once

#include <memory>
#include <utility>
#include <string>

#include "common/error/exception.h"
#include "common/utility.h"
#include "loggers/network_logger.h"
#include "network/network_io_utils.h"
#include "network/network_types.h"

namespace terrier::network {

/**
 * A network io wrapper implements an interface for interacting with a client
 * connection.
 *
 * Underneath the hood the wrapper buffers read and write, and supports posix reads and writes to the socket.
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
   */
  explicit NetworkIoWrapper(const int sock_fd)
      : sock_fd_(sock_fd), in_(std::make_unique<ReadBuffer>()), out_(std::make_unique<WriteQueue>()) {
    RestartState();
  }

  explicit NetworkIoWrapper(const std::string &ip_address, uint16_t port)
      : sock_fd_(socket(AF_INET, SOCK_STREAM, 0)),
        in_(std::make_unique<ReadBuffer>()),
        out_(std::make_unique<WriteQueue>()) {
    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = inet_addr(ip_address.c_str());
    serv_addr.sin_port = htons(port);

    int64_t ret UNUSED_ATTRIBUTE = connect(sock_fd_, reinterpret_cast<sockaddr *>(&serv_addr), sizeof(serv_addr));
    // TODO(Gus): we need better exception handling here
    TERRIER_ASSERT(ret >= 0, "Connection to replica failed");

    in_->Reset();
    out_->Reset();
    RestartState();
  }

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
  Transition FlushWriteBuffer(common::ManagedPointer<WriteBuffer> wbuf);

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
   * @return The socket file descriptor this IOWrapper communicates on
   */
  int GetSocketFd() { return sock_fd_; }

  /**
   * @return The ReadBuffer for this IOWrapper
   */
  common::ManagedPointer<ReadBuffer> GetReadBuffer() { return common::ManagedPointer<ReadBuffer>(in_); }

  /**
   * @return The WriteQueue for this IOWrapper
   */
  common::ManagedPointer<WriteQueue> GetWriteQueue() { return common::ManagedPointer<WriteQueue>(out_); }

 private:
  // The file descriptor associated with this NetworkIoWrapper
  const int sock_fd_;
  // The ReadBuffer associated with this NetworkIoWrapper
  std::unique_ptr<ReadBuffer> in_;
  // The WriteQueue associated with this NetworkIoWrapper
  std::unique_ptr<WriteQueue> out_;

  void RestartState();
};
}  // namespace terrier::network
