#include <sys/socket.h>
#include <sys/types.h>
#include <util/test_harness.h>
#include <pqxx/pqxx> /* libpqxx is used to instantiate C++ client */

#include <cstdio>
#include <cstring>
#include <string>
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

ssize_t ReadUntilReadyOrClose(char *in_buffer, size_t max_len, int socket_fd) {
  ssize_t n;
  char *current_buff_head = in_buffer;
  while (true) {
    n = read(socket_fd, current_buff_head, max_len);
    if (n < 0) {
      return n;
    }
    if (n == 0 || in_buffer[n - 6] == 'Z')  // Ready for request
      break;
  }
  return n;
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

int StartConnection(uint16_t port) {
  // Manually open a socket
  int socket_fd = socket(AF_INET, SOCK_STREAM, 0);

  struct sockaddr_in serv_addr;
  memset(&serv_addr, 0, sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_addr.s_addr = inet_addr("127.0.0.1");
  serv_addr.sin_port = htons(port);

  int64_t ret = connect(socket_fd, reinterpret_cast<sockaddr *>(&serv_addr), sizeof(serv_addr));
  if (ret < 0) TEST_LOG_ERROR("Connection Error");

  // Build the startup message
  char out_buffer[TEST_BUF_SIZE] = {};
  char in_buffer[TEST_BUF_SIZE] = {};
  // 3: protocol version number
  out_buffer[5] = 3;
  std::vector<std::string> params({"user", "postgres", "database", "postgres", "application_name", "psql"});
  size_t offset = 8;
  for (std::string &str : params) {
    strlcpy(out_buffer + offset, str.c_str(), str.length());
    offset += str.length() + 1;
  }

  out_buffer[3] = static_cast<char>(offset + 1);

  write(socket_fd, out_buffer, offset + 1);
  ReadUntilReadyOrClose(in_buffer, TEST_BUF_SIZE, socket_fd);
  return socket_fd;
}

void TerminateConnection(int socket_fd) {
  char out_buffer[TEST_BUF_SIZE] = {};
  // Build a correct query message, "SELECT A FROM B"
  memset(out_buffer, 0, sizeof(out_buffer));
  out_buffer[0] = 'X';
  int len = sizeof(int32_t) + sizeof(char);
  reinterpret_cast<int32_t *>(out_buffer + 1)[0] = htonl(len);
  write(socket_fd, nullptr, len + 1);
}

// NOLINTNEXTLINE
TEST_F(NetworkTests, BadQueryTest) {
  try {
    TEST_LOG_INFO("[BadQueryTest] Starting, expect errors to be logged");
    int socket_fd = StartConnection(port);
    char out_buffer[TEST_BUF_SIZE] = {};
    char in_buffer[TEST_BUF_SIZE] = {};
    // Build a correct query message, "SELECT A FROM B"
    memset(out_buffer, 0, sizeof(out_buffer));
    out_buffer[0] = 'Q';
    std::string query = "SELECT A FROM B;";
    strlcpy(out_buffer + 5, query.c_str(), query.length());
    size_t len = 5 + query.length();
    out_buffer[4] = static_cast<char>(len);

    // Beware the buffer length should be message length + 1 for query messages
    write(socket_fd, out_buffer, len + 1);
    ssize_t ret = ReadUntilReadyOrClose(in_buffer, TEST_BUF_SIZE, socket_fd);
    EXPECT_GT(ret, 0);  // should be okay

    // Send a bad query packet
    memset(out_buffer, 0, sizeof(out_buffer));
    std::string bad_query = "e_random_bad_packet";
    write(socket_fd, out_buffer, bad_query.length() + 1);
    ret = ReadUntilReadyOrClose(in_buffer, TEST_BUF_SIZE, socket_fd);
    EXPECT_EQ(0, ret);
    TerminateConnection(socket_fd);
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

// TODO(tanujnay112): Change to use a struct instead of this
void TestExtendedQuery(uint16_t port) {
  int socket_fd = StartConnection(port);
  char out_buffer[TEST_BUF_SIZE] = {};
  char in_buffer[TEST_BUF_SIZE] = {};
  // Build a correct query message, "SELECT A FROM B"
  memset(out_buffer, 0, sizeof(out_buffer));
  out_buffer[0] = 'P';
  std::string query = "PREPARE fooplan (int, text, bool, numeric)\0INSERT INTO foo VALUES($1, $2, $3, $4);";

  strlcpy(out_buffer + sizeof(char) + sizeof(int32_t), query.c_str(), query.length());
  size_t len = sizeof(char) + sizeof(int32_t) + sizeof(int16_t) + sizeof(int32_t) + query.length();

  // make conversion safe
  assert(len < UINT32_MAX);
  reinterpret_cast<int32_t *>(out_buffer + 1)[0] = htonl(static_cast<int32_t>(len));

  // Beware the buffer length should be message length + 1 for query messages
  write(socket_fd, out_buffer, len + 1);
  ssize_t ret = ReadUntilReadyOrClose(in_buffer, TEST_BUF_SIZE, socket_fd);

  TerminateConnection(socket_fd);
  EXPECT_GT(ret, 0);  // should be okay

  socket_fd = StartConnection(port);
  memset(out_buffer, 0, sizeof(out_buffer));
  out_buffer[0] = 'B';
  std::string dest;
  std::string source;
  int16_t numFormatCodes = 0;
  int32_t paramLength = 0;
  int16_t numResultFormatCodes = 0;

  size_t offset = sizeof(char) + sizeof(int32_t);

  strlcpy(out_buffer + offset, dest.c_str(), dest.length());

  offset += dest.length() + 1;
  strlcpy(out_buffer + offset, source.c_str(), source.length());

  offset += source.length() + 1;
  reinterpret_cast<int16_t *>(out_buffer + offset)[0] = htons(numFormatCodes);
  offset += sizeof(int16_t);
  reinterpret_cast<int32_t *>(out_buffer + offset)[0] = htonl(paramLength);
  offset += sizeof(int32_t);
  reinterpret_cast<int16_t *>(out_buffer + offset)[0] = htons(numResultFormatCodes);
  offset += sizeof(int16_t);

  len = static_cast<int32_t>(offset);
  reinterpret_cast<int32_t *>(out_buffer + 1)[0] = htonl(static_cast<int32_t>(len));

  // Beware the buffer length should be message length + 1 for query messages
  write(socket_fd, out_buffer, len + 1);
  ret = ReadUntilReadyOrClose(in_buffer, TEST_BUF_SIZE, socket_fd);
  EXPECT_GT(ret, 0);  // should be okay

  memset(out_buffer, 0, sizeof(out_buffer));
  out_buffer[0] = 'E';
  std::string portal;
  int32_t maxRows = 0;

  offset = sizeof(char) + sizeof(int32_t);

  strlcpy(out_buffer + offset, portal.c_str(), portal.length());

  offset += portal.length() + 1;
  reinterpret_cast<int32_t *>(out_buffer + offset)[0] = htonl(maxRows);
  offset += sizeof(int32_t);

  len = static_cast<int32_t>(offset);
  reinterpret_cast<int32_t *>(out_buffer + 1)[0] = htonl(static_cast<int32_t>(len));

  // Beware the buffer length should be message length + 1 for query messages
  write(socket_fd, out_buffer, len + 1);
  ret = ReadUntilReadyOrClose(in_buffer, TEST_BUF_SIZE, socket_fd);
  EXPECT_GT(ret, 0);  // should be okay

  // SyncCommand
  memset(out_buffer, 0, sizeof(out_buffer));
  out_buffer[0] = 'S';
  offset = sizeof(char) + sizeof(int32_t);

  len = static_cast<int32_t>(offset);
  reinterpret_cast<int32_t *>(out_buffer + 1)[0] = htonl(static_cast<int32_t>(len));

  // Beware the buffer length should be message length + 1 for query messages
  write(socket_fd, out_buffer, len + 1);
  ret = ReadUntilReadyOrClose(in_buffer, TEST_BUF_SIZE, socket_fd);
  EXPECT_GT(ret, 0);

  // DescribeCommand
  memset(out_buffer, 0, sizeof(out_buffer));
  out_buffer[0] = 'D';
  char option = 'S';
  std::string prepared;

  offset = sizeof(char) + sizeof(int32_t);

  out_buffer[offset] = option;
  offset += sizeof(char);
  strlcpy(out_buffer + offset, prepared.c_str(), prepared.length());
  offset += prepared.length();

  len = static_cast<int32_t>(offset);
  reinterpret_cast<int32_t *>(out_buffer + 1)[0] = htonl(static_cast<int32_t>(len));

  // Beware the buffer length should be message length + 1 for query messages
  write(socket_fd, out_buffer, len + 1);
  ret = ReadUntilReadyOrClose(in_buffer, TEST_BUF_SIZE, socket_fd);
  EXPECT_GT(ret, 0);

  // closeCommand
  memset(out_buffer, 0, sizeof(out_buffer));
  out_buffer[0] = 'C';
  option = 'S';
  prepared = "";

  offset = sizeof(char) + sizeof(int32_t);

  out_buffer[offset] = option;
  offset += sizeof(char);
  strlcpy(out_buffer + offset, prepared.c_str(), prepared.length());
  offset += prepared.length();

  len = static_cast<int32_t>(offset);
  reinterpret_cast<int32_t *>(out_buffer + 1)[0] = htonl(static_cast<int32_t>(len));

  // Beware the buffer length should be message length + 1 for query messages
  write(socket_fd, out_buffer, len + 1);
  ret = ReadUntilReadyOrClose(in_buffer, TEST_BUF_SIZE, socket_fd);
  EXPECT_GT(ret, 0);
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
