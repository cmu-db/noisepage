#include "network/network_io_wrapper.h"

#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <sys/file.h>

#include <memory>
#include <utility>

#include "network/terrier_server.h"

namespace terrier::network {
Transition NetworkIoWrapper::FlushAllWrites() {
  for (auto flush_head = out_->FlushHead(); flush_head != nullptr; out_->MarkHeadFlushed()) {
    const auto result = FlushWriteBuffer(flush_head);
    if (result != Transition::PROCEED) return result;
    flush_head = out_->FlushHead();
  }
  out_->Reset();
  return Transition::PROCEED;
}

Transition NetworkIoWrapper::FillReadBuffer() {
  if (!in_->HasMore()) in_->Reset();
  if (in_->HasMore() && in_->Full()) in_->MoveContentToHead();
  Transition result = Transition::NEED_READ;
  // Normal mode
  while (!in_->Full()) {
    auto bytes_read = in_->FillBufferFrom(sock_fd_);
    if (bytes_read > 0) {
      result = Transition::PROCEED;
    } else {
      if (bytes_read == 0) {
        if (result == Transition::PROCEED) return result;
        return Transition::TERMINATE;
      }
      switch (errno) {
        case EAGAIN:
          // Equal to EWOULDBLOCK
          return result;
        case EINTR:
          continue;
        default:
          NETWORK_LOG_ERROR("Error writing: {0}", strerror(errno));
          throw NETWORK_PROCESS_EXCEPTION("Error when filling read buffer");
      }
    }
  }
  return result;
}

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
          NETWORK_LOG_TRACE("Client closed during write");
          return Transition::TERMINATE;
        default:
          NETWORK_LOG_ERROR("Error writing: %s", strerror(errno));
          throw NETWORK_PROCESS_EXCEPTION("Fatal error during write");
      }
    }
  }
  wbuf->Reset();
  return Transition::PROCEED;
}

void NetworkIoWrapper::RestartState() {
  // Set Non Blocking
  auto flags = fcntl(sock_fd_, F_GETFL);
  flags |= O_NONBLOCK;
  if (fcntl(sock_fd_, F_SETFL, flags) < 0) {
    NETWORK_LOG_ERROR("Failed to set non-blocking socket");
  }
  // Set TCP No Delay
  int one = 1;
  setsockopt(sock_fd_, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

  in_->Reset();
  out_->Reset();
}

void NetworkIoWrapper::Restart() { RestartState(); }
}  // namespace terrier::network
