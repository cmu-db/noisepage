/*
 * This header includes some handy helper functions when testing with manual constructed packets.
 * */

#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "loggers/test_logger.h"
#include "network/connection_handle_factory.h"

namespace terrier::network {

/**
 * Read packet from the server (without parsing) until receiving ReadyForQuery or the connection is closed.
 * @param io_socket
 * @return true if reads ReadyForQuery, false for closed.
 */
bool ReadUntilReadyOrClose(const std::shared_ptr<PosixSocketIoWrapper> &io_socket) {
  while (true) {
    Transition trans = io_socket->FillReadBuffer();
    if (trans == Transition::TERMINATE) return false;

    // Check if the last message is ReadyForQuery, whose length is fixed 6, without parsing the whole packet.
    // Sometimes there are more than one message in one packet, so don't simply check the first character.
    if (io_socket->in_->BytesAvailable() >= 6) {
      io_socket->in_->Skip(io_socket->in_->BytesAvailable() - 6);
      if (io_socket->in_->ReadValue<NetworkMessageType>() == NetworkMessageType::READY_FOR_QUERY) return true;
    }
  }
}

std::shared_ptr<PosixSocketIoWrapper> StartConnection(uint16_t port) {
  // Manually open a socket
  int socket_fd = socket(AF_INET, SOCK_STREAM, 0);

  struct sockaddr_in serv_addr;
  memset(&serv_addr, 0, sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_addr.s_addr = inet_addr("127.0.0.1");
  serv_addr.sin_port = htons(port);

  int64_t ret = connect(socket_fd, reinterpret_cast<sockaddr *>(&serv_addr), sizeof(serv_addr));
  if (ret < 0) TEST_LOG_ERROR("Connection Error");

  auto io_socket = std::make_shared<PosixSocketIoWrapper>(socket_fd);
  PostgresPacketWriter writer(io_socket->out_);

  std::unordered_map<std::string, std::string> params{
      {"user", "postgres"}, {"database", "postgres"}, {"application_name", "psql"}};

  writer.WriteStartupRequest(params);
  io_socket->FlushAllWrites();

  ReadUntilReadyOrClose(io_socket);
  return io_socket;
}

}  // namespace terrier::network
