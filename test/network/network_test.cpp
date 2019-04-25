#include <sys/socket.h>
#include <sys/types.h>
#include <util/test_harness.h>
#include <pqxx/pqxx> /* libpqxx is used to instantiate C++ client */

#include <cstdio>
#include <cstring>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include "common/settings.h"
#include "gtest/gtest.h"
#include "loggers/main_logger.h"
#include "network/connection_handle_factory.h"

#define NUM_THREADS 1

/*
 * Read and write buffer size for the test
 */
#define TEST_BUF_SIZE 1000

namespace terrier::network {

//===--------------------------------------------------------------------===//
// Simple Query Tests
//===--------------------------------------------------------------------===//

class NetworkTests : public TerrierTest {
 protected:
  TerrierServer server;
  uint16_t port = common::Settings::SERVER_PORT;
  std::thread server_thread;

  /**
   * Initialization
   */
  void SetUp() override {
    TerrierTest::SetUp();

    network_logger->set_level(spdlog::level::debug);
    spdlog::flush_every(std::chrono::seconds(1));

    try {
      server.SetPort(port);
      server.SetupServer();
    } catch (NetworkProcessException &exception) {
      TEST_LOG_ERROR("[LaunchServer] exception when launching server");
      throw;
    }
    TEST_LOG_DEBUG("Server initialized");
    server_thread = std::thread([&]() { server.ServerLoop(); });
  }

  void TearDown() override {
    server.Close();
    server_thread.join();
    TEST_LOG_DEBUG("Terrier has shut down");

    TerrierTest::TearDown();
  }
};

/**
 * Use std::thread to initiate peloton server and pqxx client in separate
 * threads
 * Simple query test to guarantee both sides run correctly
 * Callback method to close server after client finishes
 */
// NOLINTNEXTLINE
TEST_F(NetworkTests, SimpleQueryTest) {
  try {
    pqxx::connection C(
        fmt::format("host=127.0.0.1 port={0} user=postgres sslmode=disable application_name=psql", port));

    pqxx::work txn1(C);
    txn1.exec("INSERT INTO employee VALUES (1, 'Han LI');");
    txn1.exec("INSERT INTO employee VALUES (2, 'Shaokun ZOU');");
    txn1.exec("INSERT INTO employee VALUES (3, 'Yilei CHU');");

    pqxx::result R = txn1.exec("SELECT name FROM employee where id=1;");
    txn1.commit();
    EXPECT_EQ(R.size(), 0);
  } catch (const std::exception &e) {
    TEST_LOG_ERROR("[SimpleQueryTest] Exception occurred: {0}", e.what());
    EXPECT_TRUE(false);
  }
  TEST_LOG_DEBUG("[SimpleQueryTest] Client has closed");
}

/**
 * Read packet from the server (without parsing) until receiving ReadyForQuery or the connection is closed.
 * @param io_socket
 * @return true if reads ReadyForQuery, false for closed.
 */
bool ReadUntilReadyOrClose(const std::shared_ptr<NetworkIoWrapper> &io_socket) {
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

/* strlcpy based on OpenBSDs strlcpy.
 * this is a safer version of strcpy.
 * clang-tidy does not accept strcpy so we need this function.
 *
 * Copy src to string dst of size siz.  At most siz-1 characters
 * will be copied.  Always NUL terminates (unless siz == 0).
 * Returns strlen(src); if retval >= siz, truncation occurred.
 */
size_t strlcpy(char *dst, const char *src, size_t siz) {
  char *d = dst;
  const char *s = src;
  size_t n = siz;

  /* Copy as many bytes as will fit */
  if (n != 0 && --n != 0) {
    do {
      if ((*d++ = *s++) == 0) break;
    } while (--n != 0);
  }

  /* Not enough room in dst, add NUL and traverse rest of src */
  if (n == 0) {
    if (siz != 0) *d = '\0'; /* NUL-terminate dst */
    while ((*s++) != 0) {
    }
  }

  return (s - src - 1); /* count does not include NUL */
}

std::shared_ptr<NetworkIoWrapper> StartConnection(uint16_t port) {
  // Manually open a socket
  int socket_fd = socket(AF_INET, SOCK_STREAM, 0);

  struct sockaddr_in serv_addr;
  memset(&serv_addr, 0, sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_addr.s_addr = inet_addr("127.0.0.1");
  serv_addr.sin_port = htons(port);

  int64_t ret = connect(socket_fd, reinterpret_cast<sockaddr *>(&serv_addr), sizeof(serv_addr));
  if (ret < 0) TEST_LOG_ERROR("Connection Error");

  auto io_socket = std::make_shared<NetworkIoWrapper>(socket_fd);
  PostgresPacketWriter writer(io_socket->out_);

  std::unordered_map<std::string, std::string> params{
      {"user", "postgres"}, {"database", "postgres"}, {"application_name", "psql"}};

  writer.WriteStartupRequest(params);
  io_socket->FlushAllWrites();

  ReadUntilReadyOrClose(io_socket);
  return io_socket;
}

void TerminateConnection(int socket_fd) {
  char out_buffer[TEST_BUF_SIZE] = {};
  // Build a correct query message, "SELECT A FROM B"
  memset(out_buffer, 0, sizeof(out_buffer));
  out_buffer[0] = 'X';
  int len = sizeof(int32_t) + sizeof(char);
  reinterpret_cast<int32_t *>(out_buffer + 1)[0] = htonl(len);
  ssize_t res UNUSED_ATTRIBUTE = write(socket_fd, nullptr, len + 1);
}

// NOLINTNEXTLINE
TEST_F(NetworkTests, BadQueryTest) {
  try {
    TEST_LOG_INFO("[BadQueryTest] Starting, expect errors to be logged");
    std::shared_ptr<NetworkIoWrapper> io_socket = StartConnection(port);
    PostgresPacketWriter writer(io_socket->out_);

    // Build a correct query message, "SELECT A FROM B"
    std::string query = "SELECT A FROM B;";
    writer.WriteSimpleQuery(query);
    io_socket->FlushAllWrites();
    bool is_ready = ReadUntilReadyOrClose(io_socket);
    EXPECT_TRUE(is_ready);  // should be okay

    // Send a bad query packet
    std::string bad_query = "a_random_bad_packet";
    io_socket->out_->Reset();
    io_socket->out_->BufferWriteRaw(bad_query.data(), bad_query.length());
    io_socket->FlushAllWrites();

    is_ready = ReadUntilReadyOrClose(io_socket);
    EXPECT_FALSE(is_ready);
    io_socket->Close();
  } catch (const std::exception &e) {
    TEST_LOG_ERROR("[BadQueryTest] Exception occurred: {0}", e.what());
    EXPECT_TRUE(false);
  }
  TEST_LOG_INFO("[BadQueryTest] Completed");
}

// NOLINTNEXTLINE
TEST_F(NetworkTests, NoSSLTest) {
  try {
    pqxx::connection C(fmt::format("host=127.0.0.1 port={0} user=postgres application_name=psql", port));

    pqxx::work txn1(C);
    txn1.exec("INSERT INTO employee VALUES (1, 'Han LI');");
    txn1.exec("INSERT INTO employee VALUES (2, 'Shaokun ZOU');");
    txn1.exec("INSERT INTO employee VALUES (3, 'Yilei CHU');");
  } catch (const std::exception &e) {
    TEST_LOG_ERROR("[NoSSLTest] Exception occurred: {0}", e.what());
    EXPECT_TRUE(false);
  }
}

void TestExtendedQuery(uint16_t port) {
  std::shared_ptr<NetworkIoWrapper> io_socket = StartConnection(port);
  io_socket->out_->Reset();
  std::string stmtName = "preparedTest";
  std::string query = "INSERT INTO foo VALUES($1, $2, $3, $4);";

  PostgresPacketWriter writer(io_socket->out_);
  writer.WriteParseCommand(stmtName, query, {});
  io_socket->FlushAllWrites();
  EXPECT_TRUE(ReadUntilReadyOrClose(io_socket));

  std::string dest;
  std::string source;
  writer.WriteBindCommand(dest, source, {}, {}, {});
  io_socket->FlushAllWrites();
  EXPECT_TRUE(ReadUntilReadyOrClose(io_socket));

  writer.WriteExecuteCommand(stmtName, 0);
  io_socket->FlushAllWrites();
  EXPECT_TRUE(ReadUntilReadyOrClose(io_socket));

  // DescribeCommand
  writer.WriteDescribeCommand(ExtendedQueryObjectType::PREPARED, stmtName);
  io_socket->FlushAllWrites();
  EXPECT_TRUE(ReadUntilReadyOrClose(io_socket));

  // SyncCommand
  writer.WriteSyncCommand();
  io_socket->FlushAllWrites();
  EXPECT_TRUE(ReadUntilReadyOrClose(io_socket));

  // CloseCommand
  writer.WriteCloseCommand(ExtendedQueryObjectType::PREPARED, stmtName);
  io_socket->FlushAllWrites();
  EXPECT_TRUE(ReadUntilReadyOrClose(io_socket));

  TerminateConnection(io_socket->sock_fd_);
}

// NOLINTNEXTLINE
TEST_F(NetworkTests, PgNetworkCommandsTest) {
  try {
    TestExtendedQuery(port);
  } catch (const std::exception &e) {
    TEST_LOG_ERROR("[PgNetworkCommandsTest] Exception occurred: {0}", e.what());
    EXPECT_TRUE(false);
  }
}

// NOLINTNEXTLINE
TEST_F(NetworkTests, LargePacketsTest) {
  try {
    pqxx::connection C(
        fmt::format("host=127.0.0.1 port={0} user=postgres sslmode=disable application_name=psql", port));

    pqxx::work txn1(C);
    std::string longQueryPacketString(255555, 'a');
    longQueryPacketString[longQueryPacketString.size() - 1] = '\0';
    txn1.exec(longQueryPacketString);
    txn1.commit();
  } catch (const std::exception &e) {
    TEST_LOG_ERROR("[LargePacketstest] Exception occurred: {0}", e.what());
    EXPECT_TRUE(false);
  }
}

}  // namespace terrier::network
