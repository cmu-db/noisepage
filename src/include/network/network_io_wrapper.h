#pragma once

#include <memory>

#include "common/macros.h"
#include "common/managed_pointer.h"
#include "network/network_types.h"

namespace terrier::network {

class ReadBuffer;
class WriteBuffer;
class WriteQueue;

/**
 * A network io wrapper implements an interface for interacting with a client connection.
 *
 * Underneath the hood the wrapper buffers read and write, and supports posix reads and writes to the socket.
 *
 * Because the buffers are large and expensive to allocate on fly, they are
 * reused. Consequently, initialization of this class is handled by a factory
 * class.
 */

class NetworkIoWrapper {
 public:
  /** This class cannot be copied or moved. */
  DISALLOW_COPY_AND_MOVE(NetworkIoWrapper);

  /** Construct a wrapper around the provided socket file descriptor. */
  explicit NetworkIoWrapper(int sock_fd);

  /**
   * @brief Fills the read buffer of this IOWrapper from the assigned fd.
   * @return The next transition for this client's state machine.
   */
  Transition FillReadBuffer();

  /**
   * @return Whether or not this IOWrapper is configured to flush its writes when this is called
   */
  bool ShouldFlush();

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
  Transition Close();

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
