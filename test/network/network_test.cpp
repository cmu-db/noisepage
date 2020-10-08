#include <cstring>
#include <memory>
#include <pqxx/pqxx>  // NOLINT
#include <string>
#include <thread>  // NOLINT
#include <vector>

#include "catalog/catalog.h"
#include "common/dedicated_thread_registry.h"
#include "common/managed_pointer.h"
#include "common/settings.h"
#include "gtest/gtest.h"
#include "network/connection_handle_factory.h"
#include "network/postgres/postgres_protocol_interpreter.h"
#include "network/terrier_server.h"
#include "storage/garbage_collector.h"
#include "test_util/manual_packet_util.h"
#include "test_util/test_harness.h"
#include "traffic_cop/traffic_cop.h"
#include "transaction/deferred_action_manager.h"
#include "transaction/transaction_manager.h"

namespace terrier::network {

/*
 * The network tests does not check whether the result is correct. It only checks if the network layer works.
 * So, in network tests, we use a fake command factory to return empty results for every query.
 */
class FakeCommandFactory : public PostgresCommandFactory {
  std::unique_ptr<PostgresNetworkCommand> PacketToCommand(const common::ManagedPointer<InputPacket> packet) override {
    return std::unique_ptr<PostgresNetworkCommand>(
        reinterpret_cast<PostgresNetworkCommand *>(new EmptyCommand(packet)));
  }
};

/**
 * This test should be refactored to use DBMain, since there's a bunch of redundant setup here. However we don't have a
 * way to inject a new CommandFactory.
 */
class NetworkTests : public TerrierTest {
 protected:
  trafficcop::TrafficCop *tcop_;
  catalog::Catalog *catalog_;
  storage::RecordBufferSegmentPool buffer_pool_{100, 100};
  storage::BlockStore block_store_{100, 100};
  transaction::TimestampManager *timestamp_manager_;
  transaction::DeferredActionManager *deferred_action_manager_;
  transaction::TransactionManager *txn_manager_;

  storage::GarbageCollector *gc_;
  std::unique_ptr<TerrierServer> server_;
  std::unique_ptr<ConnectionHandleFactory> handle_factory_;
  common::DedicatedThreadRegistry thread_registry_ = common::DedicatedThreadRegistry(DISABLED);
  uint16_t port_ = 15721;
  std::string socket_directory_ = "/tmp/";
  uint16_t connection_thread_count_ = 4;
  FakeCommandFactory fake_command_factory_;
  PostgresProtocolInterpreter::Provider protocol_provider_{
      common::ManagedPointer<PostgresCommandFactory>(&fake_command_factory_)};

  void SetUp() override {
    timestamp_manager_ = new transaction::TimestampManager;
    deferred_action_manager_ = new transaction::DeferredActionManager(common::ManagedPointer(timestamp_manager_));
    txn_manager_ = new transaction::TransactionManager(common::ManagedPointer(timestamp_manager_),
                                                       common::ManagedPointer(deferred_action_manager_),
                                                       common::ManagedPointer(&buffer_pool_), true, DISABLED);
    gc_ = new storage::GarbageCollector(common::ManagedPointer(timestamp_manager_),
                                        common::ManagedPointer(deferred_action_manager_),
                                        common::ManagedPointer(txn_manager_), DISABLED);

    catalog_ = new catalog::Catalog(common::ManagedPointer(txn_manager_), common::ManagedPointer(&block_store_),
                                    common::ManagedPointer(gc_));

    tcop_ = new trafficcop::TrafficCop(common::ManagedPointer(txn_manager_), common::ManagedPointer(catalog_), DISABLED,
                                       DISABLED, DISABLED, 0, false, execution::vm::ExecutionMode::Interpret);

    auto txn = txn_manager_->BeginTransaction();
    catalog_->CreateDatabase(common::ManagedPointer(txn), catalog::DEFAULT_DATABASE, true);
    txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

    network_logger->set_level(spdlog::level::info);
    spdlog::flush_every(std::chrono::seconds(1));

    try {
      handle_factory_ = std::make_unique<ConnectionHandleFactory>(common::ManagedPointer(tcop_));
      server_ = std::make_unique<TerrierServer>(
          common::ManagedPointer<ProtocolInterpreterProvider>(&protocol_provider_),
          common::ManagedPointer(handle_factory_.get()), common::ManagedPointer(&thread_registry_), port_,
          connection_thread_count_, socket_directory_);
      server_->RunServer();
    } catch (NetworkProcessException &exception) {
      NETWORK_LOG_ERROR("[LaunchServer] exception when launching server");
      throw;
    }

    NETWORK_LOG_DEBUG("Server initialized");
  }

  void TearDown() override {
    server_->StopServer();
    NETWORK_LOG_DEBUG("Terrier has shut down");
    catalog_->TearDown();

    // Run the GC to clean up transactions
    gc_->PerformGarbageCollection();
    gc_->PerformGarbageCollection();
    gc_->PerformGarbageCollection();

    delete catalog_;
    delete tcop_;
    delete gc_;
    delete txn_manager_;
    delete deferred_action_manager_;
    delete timestamp_manager_;
  }

  void TestExtendedQuery(uint16_t port) {
    auto io_socket_unique_ptr = network::ManualPacketUtil::StartConnection(port_);
    auto io_socket = common::ManagedPointer(io_socket_unique_ptr);
    io_socket->GetWriteQueue()->Reset();
    std::string stmt_name = "prepared_test";
    std::string query = "INSERT INTO foo VALUES($1, $2, $3, $4);";

    PostgresPacketWriter writer(io_socket->GetWriteQueue());
    auto type_oid = static_cast<int>(PostgresValueType::INTEGER);
    writer.WriteParseCommand(stmt_name, query, std::vector<int>(4, type_oid));
    io_socket->FlushAllWrites();
    EXPECT_TRUE(ManualPacketUtil::ReadUntilReadyOrClose(io_socket));

    std::string portal_name;
    writer.WriteBindCommand(portal_name, stmt_name, {}, {}, {});
    io_socket->FlushAllWrites();
    EXPECT_TRUE(ManualPacketUtil::ReadUntilReadyOrClose(io_socket));

    writer.WriteExecuteCommand(portal_name, 0);
    io_socket->FlushAllWrites();
    EXPECT_TRUE(ManualPacketUtil::ReadUntilReadyOrClose(io_socket));

    // DescribeCommand
    writer.WriteDescribeCommand(DescribeCommandObjectType::STATEMENT, stmt_name);
    io_socket->FlushAllWrites();
    EXPECT_TRUE(ManualPacketUtil::ReadUntilReadyOrClose(io_socket));

    // SyncCommand
    writer.WriteSyncCommand();
    io_socket->FlushAllWrites();
    EXPECT_TRUE(ManualPacketUtil::ReadUntilReadyOrClose(io_socket));

    // CloseCommand
    writer.WriteCloseCommand(DescribeCommandObjectType::STATEMENT, stmt_name);
    io_socket->FlushAllWrites();
    EXPECT_TRUE(ManualPacketUtil::ReadUntilReadyOrClose(io_socket));

    ManualPacketUtil::TerminateConnection(io_socket->GetSocketFd());
    io_socket->Close();
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
    pqxx::connection c(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql", port_,
                                   catalog::DEFAULT_DATABASE));

    pqxx::work txn1(c);
    txn1.exec("INSERT INTO employee VALUES (1, 'Han LI');");
    txn1.exec("INSERT INTO employee VALUES (2, 'Shaokun ZOU');");
    txn1.exec("INSERT INTO employee VALUES (3, 'Yilei CHU');");

    pqxx::result r = txn1.exec("SELECT name FROM employee where id=1;");
    txn1.commit();
    EXPECT_EQ(r.size(), 0);
  } catch (const std::exception &e) {
    NETWORK_LOG_ERROR("[SimpleQueryTest] Exception occurred: {0}", e.what());
    EXPECT_TRUE(false);
  }
  NETWORK_LOG_DEBUG("[SimpleQueryTest] Client has closed");
}

/**
 * Performs the exact same test as SimpleQueryTest, but using a Unix domain socket instead.
 * This just verifies that the Unix domain socket infrastructure works.
 */
// NOLINTNEXTLINE
TEST_F(NetworkTests, UnixDomainSocketTest) {
  try {
    /*
     * We specify the location of the domain socket (defaults to /tmp/) for PSQL.
     * This is necessary in order to ensure that the Unix domain socket gets used.
     */
    pqxx::connection c(fmt::format("host={0} port={1} user={2} sslmode=disable application_name=psql",
                                   socket_directory_, port_, catalog::DEFAULT_DATABASE));

    pqxx::work txn1(c);
    txn1.exec("INSERT INTO employee VALUES (1, 'Han LI');");
    txn1.exec("INSERT INTO employee VALUES (2, 'Shaokun ZOU');");
    txn1.exec("INSERT INTO employee VALUES (3, 'Yilei CHU');");

    pqxx::result r = txn1.exec("SELECT name FROM employee where id=1;");
    txn1.commit();
    EXPECT_EQ(r.size(), 0);
  } catch (const std::exception &e) {
    NETWORK_LOG_ERROR("[UnixDomainSocketTest] Exception occurred: {0}", e.what());
    EXPECT_TRUE(false);
  }
  NETWORK_LOG_DEBUG("[UnixDomainSocketTest] Client has closed");
}

// NOLINTNEXTLINE
TEST_F(NetworkTests, BadQueryTest) {
  try {
    NETWORK_LOG_INFO("[BadQueryTest] Starting, expect errors to be logged");
    auto io_socket_unique_ptr = network::ManualPacketUtil::StartConnection(port_);
    auto io_socket = common::ManagedPointer(io_socket_unique_ptr);
    PostgresPacketWriter writer(io_socket->GetWriteQueue());

    // Build a correct query message, "SELECT A FROM B"
    std::string query = "SELECT A FROM B;";
    writer.WriteSimpleQuery(query);
    io_socket->FlushAllWrites();
    bool is_ready = ManualPacketUtil::ReadUntilReadyOrClose(io_socket);
    EXPECT_TRUE(is_ready);  // should be okay

    // Send a bad query packet
    std::string bad_query = "a_random_bad_packet";
    io_socket->GetWriteQueue()->Reset();
    io_socket->GetWriteQueue()->BufferWriteRaw(bad_query.data(), bad_query.length());
    io_socket->FlushAllWrites();

    is_ready = ManualPacketUtil::ReadUntilReadyOrClose(io_socket);
    EXPECT_FALSE(is_ready);
    io_socket->Close();
  } catch (const std::exception &e) {
    NETWORK_LOG_ERROR("[BadQueryTest] Exception occurred: {0}", e.what());
    EXPECT_TRUE(false);
  }
  NETWORK_LOG_INFO("[BadQueryTest] Completed");
}

// NOLINTNEXTLINE
TEST_F(NetworkTests, NoSSLTest) {
  try {
    pqxx::connection c(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql", port_,
                                   catalog::DEFAULT_DATABASE));

    pqxx::work txn1(c);
    txn1.exec("INSERT INTO employee VALUES (1, 'Han LI');");
    txn1.exec("INSERT INTO employee VALUES (2, 'Shaokun ZOU');");
    txn1.exec("INSERT INTO employee VALUES (3, 'Yilei CHU');");
  } catch (const std::exception &e) {
    NETWORK_LOG_ERROR("[NoSSLTest] Exception occurred: {0}", e.what());
    EXPECT_TRUE(false);
  }
}

// NOLINTNEXTLINE
TEST_F(NetworkTests, PgNetworkCommandsTest) {
  try {
    TestExtendedQuery(port_);
  } catch (const std::exception &e) {
    NETWORK_LOG_ERROR("[PgNetworkCommandsTest] Exception occurred: {0}", e.what());
    EXPECT_TRUE(false);
  }
}

// NOLINTNEXTLINE
TEST_F(NetworkTests, LargePacketsTest) {
  try {
    pqxx::connection c(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql", port_,
                                   catalog::DEFAULT_DATABASE));

    pqxx::work txn1(c);
    std::string long_query_packet_string(255555, 'a');
    long_query_packet_string[long_query_packet_string.size() - 1] = '\0';
    txn1.exec(long_query_packet_string);
    txn1.commit();
  } catch (const std::exception &e) {
    NETWORK_LOG_ERROR("[LargePacketstest] Exception occurred: {0}", e.what());
    EXPECT_TRUE(false);
  }
}

/**
 * This is a less "parallelized" version of RacerTest that tests the network layers functionality
 * on multiple synchronous clients in two batches
 */
// NOLINTNEXTLINE
TEST_F(NetworkTests, MultipleConnectionTest) {
  std::vector<std::unique_ptr<NetworkIoWrapper>> io_sockets;
  io_sockets.reserve(connection_thread_count_ * 2);

  for (size_t i = 0; i < 2; i++) {
    for (size_t i = 0; i < connection_thread_count_ * 2; i++) {
      auto io_socket_unique_ptr = network::ManualPacketUtil::StartConnection(port_);
      EXPECT_NE(io_socket_unique_ptr, nullptr);
      io_sockets.emplace_back(std::move(io_socket_unique_ptr));
    }

    for (auto &socket : io_sockets) {
      ManualPacketUtil::TerminateConnection(socket->GetSocketFd());
      socket->Close();
    }
    io_sockets.clear();
  }
}

/**
 * This is meant to overload the network layer with multiple concurrent client threads. It was made to uncover
 * a bug where ConnectionHandlerTask had a few race conditions amongst its fields. Two threads using the same
 * ConnectionHandlerTask instance could corrupt its fields.
 */
// NOLINTNEXTLINE
TEST_F(NetworkTests, DISABLED_RacerTest) {
  //  due to issue #766 PostgresProtocolHandler cannot consistently handle
  //  simultaneous client requests so for now we are allowing for some failure tolerance

  // The number of threads to use in each trial
  std::vector<size_t> thread_counts = {
      // clang-format off
      2,
      connection_thread_count_,
      connection_thread_count_ * 2ul,
      connection_thread_count_ * 3ul
      // clang-format on
  };

  for (auto num_threads : thread_counts) {
    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    NETWORK_LOG_INFO("Number of threads: {0}", num_threads);

    std::atomic_int successes = 0;
    for (size_t i = 0; i < num_threads; i++) {
      threads.emplace_back([this, &successes] {
        try {
          auto io_socket_unique_ptr = network::ManualPacketUtil::StartConnection(port_);
          if (io_socket_unique_ptr == nullptr) {
            return;
          }
          successes++;
          auto io_socket = common::ManagedPointer(io_socket_unique_ptr);
          ManualPacketUtil::TerminateConnection(io_socket->GetSocketFd());
          io_socket->Close();
        } catch (const std::exception &e) {
          EXPECT_TRUE(false);
        }
      });
    }
    for (auto &t : threads) {
      t.join();
    }
    threads.clear();
    EXPECT_EQ(successes, num_threads);
  }
}

/**
 * There was a bug in InputPacket::Clear. If the InputPacket had been extended for a big packet, then Clear was called,
 * the extended_ flag was not set back to false, so future reuse of that InputPacket was subject to copying garbage
 * data due to incorrect length and capacity.
 *
 * The test is so named because it fixed a bug that Gus was seeing in trying to replicate TPC-C across the network.
 */
// NOLINTNEXTLINE
TEST_F(NetworkTests, GusThesisSaver) {
  try {
    NETWORK_LOG_INFO("[GusThesisSaver] Starting, expect errors to be logged");
    auto io_socket_unique_ptr = network::ManualPacketUtil::StartConnection(port_);
    auto io_socket = common::ManagedPointer(io_socket_unique_ptr);
    PostgresPacketWriter writer(io_socket->GetWriteQueue());

    // Create a large packet that will require the InputPacket to be extended
    std::string big_query;
    for (uint32_t i = 0; i < 1000; i++) {
      big_query.append("SELECT A FROM B;");
    }
    writer.WriteSimpleQuery(big_query);
    io_socket->FlushAllWrites();
    bool is_ready = ManualPacketUtil::ReadUntilReadyOrClose(io_socket);
    EXPECT_TRUE(is_ready);  // should be okay

    // Create a small packet. Correctness is checked via added asserts in ProtocolInterpreter
    writer.WriteSimpleQuery("SELECT A FROM B;");
    io_socket->FlushAllWrites();

    is_ready = ManualPacketUtil::ReadUntilReadyOrClose(io_socket);
    EXPECT_TRUE(is_ready);  // should be okay
    io_socket->Close();
  } catch (const std::exception &e) {
    NETWORK_LOG_ERROR("[GusThesisSaver] Exception occurred: {0}", e.what());
    EXPECT_TRUE(false);
  }
  NETWORK_LOG_INFO("[GusThesisSaver] Completed");
}

}  // namespace terrier::network
