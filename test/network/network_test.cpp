#include <sys/socket.h>
#include <sys/types.h>
#include <util/test_harness.h>
#include <pqxx/pqxx> /* libpqxx is used to instantiate C++ client */

#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include "gtest/gtest.h"
#include "loggers/main_logger.h"
#include "network/connection_handle_factory.h"

#define NUM_THREADS 1

namespace terrier::network {

//===--------------------------------------------------------------------===//
// Simple Query Tests
//===--------------------------------------------------------------------===//

class NetworkTests : public TerrierTest {
 protected:
  TerrierServer server;
  uint16_t port = 2888;
  std::thread server_thread;

  /**
   * Initialization
   */
  void SetUp() override {
    TerrierTest::SetUp();

    LOG_INFO("Server initialized");
    init_network_logger();
    network_logger->set_level(spdlog::level::debug);
    spdlog::flush_every(std::chrono::seconds(1));

    try {
      server.SetPort(port);
      server.SetupServer();
    } catch (NetworkProcessException &exception) {
      LOG_INFO("[LaunchServer] exception when launching server");
    }
    server_thread = std::thread([&]() { server.ServerLoop(); });
  }

  void TearDown() override {
    server.Close();
    server_thread.join();
    LOG_INFO("Terrier has shut down");

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
    LOG_INFO("[SimpleQueryTest] Exception occurred: {0}", e.what());
    EXPECT_TRUE(false);
  }
  LOG_INFO("[SimpleQueryTest] Client has closed");
}

ssize_t ReadUntilReadyOrClose(char *in_buffer, size_t max_len, int socket_fd) {
  ssize_t n;
  while (true) {
    n = read(socket_fd, in_buffer, max_len);
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

// NOLINTNEXTLINE
TEST_F(NetworkTests, BadQueryTest) {
  try {
    // Manually open a socket
    int socket_fd = socket(AF_INET, SOCK_STREAM, 0);

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    serv_addr.sin_port = htons(port);

    int64_t ret = connect(socket_fd, reinterpret_cast<sockaddr *>(&serv_addr), sizeof(serv_addr));
    if (ret < 0) LOG_ERROR("Connection Error");

    // Build the startup message
    char out_buffer[1000] = {};
    char in_buffer[1000] = {};
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
    ReadUntilReadyOrClose(in_buffer, 1000, socket_fd);

    // Build a correct query message, "SELECT A FROM B"
    memset(out_buffer, 0, sizeof(out_buffer));
    out_buffer[0] = 'Q';
    std::string query = "SELECT A FROM B;";
    strlcpy(out_buffer + 5, query.c_str(), query.length());
    size_t len = 5 + query.length();
    out_buffer[4] = static_cast<char>(len);

    // Beware the buffer length should be message length + 1 for query messages
    write(socket_fd, out_buffer, len + 1);
    ret = ReadUntilReadyOrClose(in_buffer, 1000, socket_fd);
    EXPECT_GT(ret, 0);  // should be okay

    // Send a bad query packet
    memset(out_buffer, 0, sizeof(out_buffer));
    std::string bad_query = "e_random_bad_packet";
    write(socket_fd, out_buffer, bad_query.length() + 1);
    ret = ReadUntilReadyOrClose(in_buffer, 1000, socket_fd);
    EXPECT_EQ(0, ret);  // socket should be closed
  } catch (const std::exception &e) {
    LOG_INFO("[BadQueryTest] Exception occurred: {0}", e.what());
    EXPECT_TRUE(false);
  }
  LOG_INFO("[BadQueryTest] Client has closed");
}

}  // namespace terrier::network
