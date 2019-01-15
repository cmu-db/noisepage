#include <util/test_harness.h>
#include <pqxx/pqxx> /* libpqxx is used to instantiate C++ client */
#include "gtest/gtest.h"
#include "loggers/main_logger.h"
#include "network/connection_handle_factory.h"

#define NUM_THREADS 1

namespace terrier::network {

//===--------------------------------------------------------------------===//
// Simple Query Tests
//===--------------------------------------------------------------------===//

class SimpleQueryTests : public TerrierTest {};

/**
 * Simple select query test
 */
void *SimpleQueryTest(int port) {
  try {
    // forcing the factory to generate psql protocol handler
    pqxx::connection C(
        fmt::format("host=127.0.0.1 port=%d user=default_database sslmode=disable application_name=psql", port));
    pqxx::work txn1(C);
    txn1.exec("INSERT INTO employee VALUES (1, 'Han LI');");
    txn1.exec("INSERT INTO employee VALUES (2, 'Shaokun ZOU');");
    txn1.exec("INSERT INTO employee VALUES (3, 'Yilei CHU');");
    pqxx::result R = txn1.exec("SELECT name FROM employee where id=1;");
    txn1.commit();
    EXPECT_EQ(R.size(), 0);
  } catch (const std::exception &e) {
    LOG_INFO("[SimpleQueryTest] Exception occurred: %s", e.what());
    EXPECT_TRUE(false);
  }
  LOG_INFO("[SimpleQueryTest] Client has closed");
  return nullptr;
}

/**
 * rollback test
 * YINGJUN: rewrite wanted.
 */
/*
void *RollbackTest(int port) {
  try {
    pqxx::connection C(StringUtil::Format(
            "host=127.0.0.1 port=%d user=postgres sslmode=disable",port));
    LOG_INFO("[RollbackTest] Connected to %s", C.dbname());
    pqxx::work W(C);

    peloton::network::ClientSocketWrapper *conn =
        peloton::network::NetworkManager::GetConnection(
            peloton::network::NetworkManager::recent_connfd);

    EXPECT_TRUE(conn->protocol_handler_.is_started);
    // EXPECT_EQ(conn->state, peloton::network::READ);
    // create table and insert some data
    W.exec("DROP TABLE IF EXISTS employee;");
    W.exec("CREATE TABLE employee(id INT, name VARCHAR(100));");
    W.exec("INSERT INTO employee VALUES (1, 'Han LI');");

    W.abort();

    W.exec("INSERT INTO employee VALUES (2, 'Shaokun ZOU');");
    W.exec("INSERT INTO employee VALUES (3, 'Yilei CHU');");

    W.commit();


    // pqxx::result R = W.exec("SELECT name FROM employee where id=1;");

    // EXPECT_EQ(R.size(), 1);

    // LOG_INFO("[RollbackTest] Found %lu employees", R.size());
    // W.commit();

  } catch (const std::exception &e) {
    LOG_INFO("[RollbackTest] Exception occurred");
  }

  LOG_INFO("[RollbackTest] Client has closed");
  return NULL;
}
*/

/*
TEST_F(PacketManagerTests, RollbackTest) {
  peloton::PelotonInit::Initialize();
  LOG_INFO("Server initialized");
  int port = 15721;
  peloton::network::NetworkManager network_manager;
  std::thread serverThread(LaunchServer, network_manager,port);
  while (!network_manager.GetIsStarted()) {
    sleep(1);
  }

  RollbackTest(port);

  network_manager.CloseServer();
  serverThread.join();
  LOG_INFO("Thread has joined");
  peloton::PelotonInit::Shutdown();
  LOG_INFO("Peloton has shut down");
}
*/

/**
 * Use std::thread to initiate peloton server and pqxx client in separate
 * threads
 * Simple query test to guarantee both sides run correctly
 * Callback method to close server after client finishes
 */
// NOLINTNEXTLINE
TEST_F(SimpleQueryTests, SimpleQueryTest) {
  LOG_INFO("Server initialized");
  terrier::network::TerrierServer server;

  int port = 2888;
  try {
    server.SetPort(port);
    server.SetupServer();
  } catch (terrier::NetworkProcessException &exception) {
    LOG_INFO("[LaunchServer] exception when launching server");
  }
  std::thread serverThread([&]() { server.ServerLoop(); });

  // server & client running correctly
  SimpleQueryTest(port);

  server.Close();
  serverThread.join();
  LOG_INFO("Terrier has shut down");
}
}  // namespace terrier::network
