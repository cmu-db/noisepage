#pragma once

#include <memory>
#include <utility>

#include "common/exception.h"
#include "common/utility.h"

#include "network/network_io_utils.h"
#include "network/network_types.h"

namespace terrier::network {

/**
 * A network io wrapper provides an interface for interacting with a client
 * connection.
 *
 * Underneath the hood the wrapper buffers read and write, and can support posix
 * and ssl reads and writes to the socket, depending on the concrete type at
 * runtime.
 *
 * Because the buffers are large and expensive to allocate on fly, they are
 * reused. Consequently, initialization of this class is handled by a factory
 * class. @see NetworkIoWrapperFactory
 */
class NetworkIoWrapper {
 public:

  /**
   * @return whether or not SSL is able to be handled by this IOWrapper
   */
  virtual bool SslAble() const = 0;
  // TODO(Tianyu): Change and document after we refactor protocol handler

  /**
   * @brief Fills the read buffer of this IOWrapper from the assigned fd
   * @return The next transition for this client's state machine
   */
  virtual Transition FillReadBuffer() = 0;

  /**
   * @brief Flushes the write buffer of this IOWrapper to the assigned fd
   * @return The next transition for this client's state machine
   */
  virtual Transition FlushWriteBuffer(WriteBuffer *wbuf) = 0;

  /**
   * @brief Closes this IOWrapper
   * @return The next transition for this client's state machine
   */
  virtual Transition Close() = 0;

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

  // TODO(tanujnay112) not sure what the difference is between this an FlushWriteBuffer()
  /**
   * @brief Flushes all writes to this IOWrapper
   * @return The next transition for this client's state machine
   */
  Transition FlushAllWrites();

  /**
   * @return Whether or not this IOWrapper is configured to flush its writes when this is called
   */
  bool ShouldFlush() { return out_->ShouldFlush(); }
  // TODO(Tianyu): Make these protected when protocol handler refactor is
  // complete

  /**
   * @brief Constructor for a NetworkIOWrapper
   * @param sock_fd The fd this IOWrapper communicates on
   * @param in The ReadBuffer this NetworkIOWrapper uses for reads
   * @param out The WriteQueue this NetworkIOWrapper uses for writes
   */
  NetworkIoWrapper(int sock_fd, std::shared_ptr<ReadBuffer> in, std::shared_ptr<WriteQueue> out)
      : sock_fd_(sock_fd), in_(std::move(in)), out_(std::move(out)) {
    in_->Reset();
    out_->Reset();
  }

  virtual ~NetworkIoWrapper() = default;

  /**
   * Barred NetworkIoWrapper instances from being copied
   */
  DISALLOW_COPY(NetworkIoWrapper);

  /**
   * Constructs a NetworkIoWrapper from another instance
   * @param other The instance to copy from
   */
  NetworkIoWrapper(NetworkIoWrapper &&other) noexcept
      : NetworkIoWrapper(other.sock_fd_, std::move(other.in_), std::move(other.out_)) {}

  /**
   * The file descriptor associated with this NetworkIoWrapper
   */
  int sock_fd_;

  /**
   * The ReadBuffer associated with this NetworkIoWrapper
   */
  std::shared_ptr<ReadBuffer> in_;

  /**
 * The WriteQueue associated with this NetworkIoWrapper
 */
  std::shared_ptr<WriteQueue> out_;
};

/**
 * A Network IoWrapper specialized for dealing with posix sockets.
 */
class PosixSocketIoWrapper : public NetworkIoWrapper {
 public:

  /**
   * @see NetworkIoWrapper
   * @param sock_fd
   * @param in
   * @param out
   */
  explicit PosixSocketIoWrapper(int sock_fd, std::shared_ptr<ReadBuffer> in = std::make_shared<ReadBuffer>(),
                                std::shared_ptr<WriteQueue> out = std::make_shared<WriteQueue>());

  /**
   * @see NetworkIoWrapper
   * @param other
   */
  explicit PosixSocketIoWrapper(NetworkIoWrapper &&other)
      : PosixSocketIoWrapper(other.sock_fd_, std::move(other.in_), std::move(other.out_)) {}

  /**
   * Barring copying and moving of PosixSocketIoWrapper instances
   */
  DISALLOW_COPY_AND_MOVE(PosixSocketIoWrapper);

  /**
   * @return Whether or not this IoWrapper is able to support SSL
   */
  bool SslAble() const override { return false; }

  /**
   * @see NetworkIoWrapper::FillReadBuffer
   * @return The next transition for the client state machine
   */
  Transition FillReadBuffer() override;

  /**
   * @see NetworkIoWrapper::FlushWriteBuffer
   * @return The next transition for the client state machine
   */
  Transition FlushWriteBuffer(WriteBuffer *wbuf) override;

  /**
   * @see NetworkIoWrapper::Close
   * @return The next transition for the client state machine
   */
  Transition Close() override {
    peloton_close(sock_fd_);
    return Transition::PROCEED;
  }
};
}  // namespace terrier::network
