#include "network/network_io_wrapper.h"

#include <netinet/tcp.h>

#include "common/utility.h"
#include "loggers/network_logger.h"
#include "network/network_io_utils.h"

namespace terrier::network {

static_assert(EAGAIN == EWOULDBLOCK, "If this trips, you'll have to #if guard all existing EAGAIN usages.");

NetworkIoWrapper::NetworkIoWrapper(const int sock_fd)
    : sock_fd_(sock_fd), in_(std::make_unique<ReadBuffer>()), out_(std::make_unique<WriteQueue>()) {
  RestartState();
}

Transition NetworkIoWrapper::FlushAllWrites() {
  for (auto flush_head = out_->FlushHead(); flush_head != nullptr; out_->MarkHeadFlushed()) {
    const auto result = FlushWriteBuffer(flush_head);
    if (result != Transition::PROCEED) return result;
    flush_head = out_->FlushHead();
  }
  out_->Reset();
  return Transition::PROCEED;
}

Transition NetworkIoWrapper::Close() {
  TerrierClose(sock_fd_);
  return Transition::PROCEED;
}

void NetworkIoWrapper::Restart() { RestartState(); }

Transition NetworkIoWrapper::FillReadBuffer() {
  if (!in_->HasMore()) in_->Reset();
  // If the read buffer still has content and the read buffer is full,
  // then the read buffer's contents is moved to the head.
  if (in_->HasMore() && in_->Full()) in_->MoveContentToHead();

  // By default, the next action to take is to continue to read.
  Transition result = Transition::NEED_READ;
  // While the read buffer is not yet full,
  while (!in_->Full()) {
    auto bytes_read = in_->FillBufferFrom(sock_fd_);

    if (bytes_read > 0) {
      // If bytes were read, then the bytes can be processed.
      result = Transition::PROCEED;
    } else {
      if (bytes_read == 0) {
        if (result == Transition::PROCEED) return result;
        return Transition::TERMINATE;
      }
      switch (errno) {
        case EAGAIN:
          return result;
        case EINTR:
          continue;
        default:
          throw NETWORK_PROCESS_EXCEPTION(fmt::format("Error while filling read buffer: {}", strerror(errno)));
      }
    }
  }
  return result;
}

bool NetworkIoWrapper::ShouldFlush() { return out_->ShouldFlush(); }

Transition NetworkIoWrapper::FlushWriteBuffer(const common::ManagedPointer<WriteBuffer> wbuf) {
  while (wbuf->HasMore()) {
    auto bytes_written = wbuf->WriteOutTo(sock_fd_);
    if (bytes_written < 0) {
      switch (errno) {
        case EINTR:
          continue;
        case EAGAIN:
          return Transition::NEED_WRITE;
        case EPIPE:
          return Transition::TERMINATE;
        default:
          throw NETWORK_PROCESS_EXCEPTION(fmt::format("Fatal error during write: {}", strerror(errno)));
      }
    }
  }
  wbuf->Reset();
  return Transition::PROCEED;
}

void NetworkIoWrapper::RestartState() {
  int err;          // For C-style error codes.
  int enabled = 1;  // For setting socket options.

  // Enable non-blocking sockets.
  // This causes all socket operations to return immediately with errno EWOULDBLOCK instead of blocking.
  {
    auto flags = fcntl(sock_fd_, F_GETFL);
    TERRIER_ASSERT(flags != -1, "If this syscall returned an error, you have bigger problems.");
    flags |= O_NONBLOCK;
    err = fcntl(sock_fd_, F_SETFL, flags);
    if (err < 0) {
      NETWORK_LOG_ERROR("Failed to enable non-blocking sockets. Blocking sockets will be used.");
    }
  }

  // Disable TCP segment buffering.
  // This causes segments to be sent as soon as possible.
  {
    err = setsockopt(sock_fd_, IPPROTO_TCP, TCP_NODELAY, &enabled, sizeof(enabled));
    if (err < 0) {
      NETWORK_LOG_ERROR("Failed to disable TCP segment buffering. Segment buffering will be used.");
    }
  }

  in_->Reset();
  out_->Reset();
}

}  // namespace terrier::network
