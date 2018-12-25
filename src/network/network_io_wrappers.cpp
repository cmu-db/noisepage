//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// network_io_wrappers.cpp
//
// Identification: src/network/network_io_wrappers.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "network/network_io_wrappers.h"
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <sys/file.h>
#include "network/peloton_server.h"

namespace terrier {
namespace network {
Transition NetworkIoWrapper::FlushAllWrites() {
  for (; out_->FlushHead() != nullptr; out_->MarkHeadFlushed()) {
    auto result = FlushWriteBuffer(*out_->FlushHead());
    if (result != Transition::PROCEED) return result;
  }
  out_->Reset();
  return Transition::PROCEED;
}

PosixSocketIoWrapper::PosixSocketIoWrapper(int sock_fd, std::shared_ptr<ReadBuffer> in, std::shared_ptr<WriteQueue> out)
    : NetworkIoWrapper(sock_fd, std::move(in), std::move(out)) {
  // Set Non Blocking
  auto flags = fcntl(sock_fd_, F_GETFL);
  flags |= O_NONBLOCK;
  if (fcntl(sock_fd_, F_SETFL, flags) < 0) {
    LOG_ERROR("Failed to set non-blocking socket");
  }
  // Set TCP No Delay
  int one = 1;
  setsockopt(sock_fd_, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
}

Transition PosixSocketIoWrapper::FillReadBuffer() {
  if (!in_->HasMore()) in_->Reset();
  if (in_->HasMore() && in_->Full()) in_->MoveContentToHead();
  Transition result = Transition::NEED_READ;
  // Normal mode
  while (!in_->Full()) {
    auto bytes_read = in_->FillBufferFrom(sock_fd_);
    if (bytes_read > 0)
      return Transition::PROCEED;
    else if (bytes_read == 0)
      return Transition::TERMINATE;
    else
      switch (errno) {
        case EAGAIN:
          // Equal to EWOULDBLOCK
          return result;
        case EINTR:
          continue;
        default:
          LOG_ERROR("Error writing: %s", strerror(errno));
          throw NETWORK_PROCESS_EXCEPTION("Error when filling read buffer");
      }
  }
  return result;
}

Transition PosixSocketIoWrapper::FlushWriteBuffer(WriteBuffer &wbuf) {
  while (wbuf.HasMore()) {
    auto bytes_written = wbuf.WriteOutTo(sock_fd_);
    if (bytes_written < 0) switch (errno) {
        case EINTR:
          continue;
        case EAGAIN:
          return Transition::NEED_WRITE;
        default:
          LOG_ERROR("Error writing: %s", strerror(errno));
          throw NETWORK_PROCESS_EXCEPTION("Fatal error during write");
      }
  }
  wbuf.Reset();
  return Transition::PROCEED;
}

}  // namespace network
}  // namespace terrier
