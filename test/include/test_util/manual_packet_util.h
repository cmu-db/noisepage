#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "catalog/catalog_defs.h"
#include "network/connection_handle_factory.h"
#include "network/network_io_wrapper.h"
#include "test_util/manual_packet_util.h"

namespace noisepage::network {
/*
 * This util class includes some handy helper functions when testing with manual constructed packets.
 * */
class ManualPacketUtil {
 public:
  ManualPacketUtil() = delete;

  /**
   * Read packet from the server (without parsing) until receiving ReadyForQuery or the connection is closed.
   * @param io_socket
   * @param expected_msg_type
   * @return true if reads the expected type message, false for closed.
   */
  static bool ReadUntilMessageOrClose(common::ManagedPointer<NetworkIoWrapper> io_socket,
                                      const NetworkMessageType &expected_msg_type) {
    while (true) {
      io_socket->GetReadBuffer()->Reset();
      Transition trans = io_socket->FillReadBuffer();
      if (trans == Transition::TERMINATE) return false;

      while (io_socket->GetReadBuffer()->HasMore()) {
        auto type = io_socket->GetReadBuffer()->ReadValue<NetworkMessageType>();
        auto size = io_socket->GetReadBuffer()->ReadValue<int32_t>();
        if (size >= 4) io_socket->GetReadBuffer()->Skip(static_cast<size_t>(size - 4));

        if (type == expected_msg_type) return true;
      }
    }
  }

  /**
   * A wrapper for ReadUntilMessageOrClose since most of the times people expect PG_READY_FOR_QUERY.
   * @param io_socket
   * @return
   */
  static bool ReadUntilReadyOrClose(common::ManagedPointer<NetworkIoWrapper> io_socket) {
    return ReadUntilMessageOrClose(io_socket, NetworkMessageType::PG_READY_FOR_QUERY);
  }

  static std::unique_ptr<NetworkIoWrapper> StartConnection(uint16_t port) {
    // Manually open a socket
    int socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    NOISEPAGE_ASSERT(socket_fd >= 0, "Failed to create socket");

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    serv_addr.sin_port = htons(port);

    int64_t ret UNUSED_ATTRIBUTE = connect(socket_fd, reinterpret_cast<sockaddr *>(&serv_addr), sizeof(serv_addr));
    NOISEPAGE_ASSERT(ret >= 0, "Connector Error");

    auto io_socket = std::make_unique<NetworkIoWrapper>(socket_fd);
    PostgresPacketWriter writer(io_socket->GetWriteQueue());

    std::unordered_map<std::string, std::string> params{
        {"user", catalog::DEFAULT_DATABASE}, {"database", catalog::DEFAULT_DATABASE}, {"application_name", "psql"}};

    writer.WriteStartupRequest(params);
    io_socket->FlushAllWrites();

    bool success = ReadUntilReadyOrClose(common::ManagedPointer(io_socket));
    if (!success) {
      return nullptr;
    }
    return io_socket;
  }

  /**
   * Closes connection on socket fd
   * @param socket_fd
   */
  static void TerminateConnection(int socket_fd) {
    char out_buffer[TEST_BUFFER_SIZE] = {};
    // Build a correct query message, "SELECT A FROM B"
    memset(out_buffer, 0, sizeof(out_buffer));
    out_buffer[0] = 'X';
    int len = sizeof(int32_t) + sizeof(char);
    reinterpret_cast<int32_t *>(out_buffer + 1)[0] = htonl(len);
    UNUSED_ATTRIBUTE auto result = write(socket_fd, out_buffer, len + 1);
    NOISEPAGE_ASSERT(result == len + 1, "Unsuccessful write");
  }

 private:
  // The port used to connect a Postgres backend. Useful for debugging.
  static const int POSTGRES_PORT = 5432;
  // Read and write buffer size for the test
  static const uint32_t TEST_BUFFER_SIZE = 1000;
};
}  // namespace noisepage::network
