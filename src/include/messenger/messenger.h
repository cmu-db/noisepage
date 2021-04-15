#pragma once

#include <atomic>
#include <condition_variable>  // NOLINT
#include <functional>
#include <map>
#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <queue>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/dedicated_thread_owner.h"
#include "common/dedicated_thread_task.h"
#include "common/managed_pointer.h"
#include "messenger/connection_destination.h"
#include "messenger/messenger_defs.h"

// All zmq objects are forward-declared and allocated on the heap.
// This is to avoid leaking zmq headers into the rest of the system.
// The prediction is that doing this won't hurt performance too much.
namespace zmq {
class context_t;
class socket_t;
}  // namespace zmq

namespace noisepage::messenger {

class ConnectionDestination;
class Messenger;
class MessengerPolledSockets;
class ZmqUtil;

/** An abstraction around ZeroMQ messages which explicitly have the sender specified. */
class ZmqMessage {
 public:
  /** @return The ID of this message. */
  message_id_t GetMessageId() const { return message_id_; }

  /** @return The callback to invoke on the source. */
  callback_id_t GetSourceCallbackId() const { return source_cb_id_; }

  /** @return The callback to invoke on the destination. */
  callback_id_t GetDestinationCallbackId() const { return dest_cb_id_; }

  /** @return The routing ID of this message. */
  std::string_view GetRoutingId() const { return std::string_view(routing_id_); }

  /** @return The message itself. */
  std::string_view GetMessage() const { return message_; }

  /** @return The raw payload of the message. */
  std::string_view GetRawPayload() const { return std::string_view(payload_); }

 private:
  friend Messenger;
  friend ZmqUtil;

  /**
   * Build a new ZmqMessage from the supplied information.
   * @param message_id      The ID of this message.
   * @param source_cb_id    The callback ID of the message on the source.
   * @param dest_cb_id      The callback ID of the message on the destination.
   * @param routing_id      The routing ID of the message sender. Roughly speaking, "who sent this message".
   * @param message         The contents of the message.
   * @return A ZmqMessage encapsulating the given message.
   */
  static ZmqMessage Build(message_id_t message_id, callback_id_t source_cb_id, callback_id_t dest_cb_id,
                          const std::string &routing_id, std::string_view message);

  /**
   * Parse the given payload into a ZmqMessage.
   * @param routing_id      The message's routing ID.
   * @param message         The message for the destination.
   * @return A ZmqMessage encapsulating the given message.
   */
  static ZmqMessage Parse(const std::string &routing_id, const std::string &message);

  /** Construct a new ZmqMessage with the given routing ID and payload. Payload of form ID-MESSAGE. */
  ZmqMessage(std::string routing_id, std::string payload);

  /** The routing ID of the message. */
  std::string routing_id_;
  /** The payload in the message, of form ID-MESSAGE.  */
  std::string payload_;

  /** The cached id of the message. */
  message_id_t message_id_;
  /** The cached callback id of the message (source). */
  callback_id_t source_cb_id_;
  /** The cached callback id of the message (destination). */
  callback_id_t dest_cb_id_;
  /** The cached actual message. */
  std::string_view message_;
};

/** ConnectionId is an abstraction around establishing connections. */
class ConnectionId {
 public:
  /**
   * Create a new ConnectionId that is connected to the specified target.
   * @param messenger   The messenger that owns this connection ID.
   * @param target      The target to be connected to.
   * @param identity    The routing ID (name) that the connection should have.
   */
  explicit ConnectionId(common::ManagedPointer<Messenger> messenger, const ConnectionDestination &target,
                        const std::string &identity);

  /** An explicit destructor is necessary because of the unique_ptr around a forward-declared type. */
  ~ConnectionId();

 private:
  friend Messenger;
  friend ZmqUtil;

  /** The ZMQ socket. */
  std::unique_ptr<zmq::socket_t> socket_;
  /** The ZMQ socket routing ID. */
  std::string routing_id_;
  /** The target that was connected to. */
  std::string target_name_;
};

/** ConnectionRouter represents a new endpoint opened up on the Messenger to listen for connections. */
class ConnectionRouter {
 public:
  /**
   * Create a new ConnectionRouter that listens for incoming connections.
   * @param messenger   The messenger that owns this connection router.
   * @param target      The target to listen on.
   * @param identity    The routing ID (name) that the router should have.
   * @param callback    The server loop for this connection router.
   */
  explicit ConnectionRouter(common::ManagedPointer<Messenger> messenger, const ConnectionDestination &target,
                            const std::string &identity, CallbackFn callback);

  /** An explicit destructor is necessary because of the unique_ptr around a forward-declared type. */
  ~ConnectionRouter();

  /** @return The routing ID (name) of this connection router. */
  const std::string &GetIdentity() const { return identity_; }

 private:
  friend Messenger;

  /** The ZMQ socket. */
  std::unique_ptr<zmq::socket_t> socket_;
  /** The callback to be invoked on all messages received. */
  CallbackFn callback_;
  /** The identity of this router. */
  std::string identity_;
};

/**
 * Messenger handles all the network aspects of sending and receiving messages.
 * Logic based on the messages is is deferred to MessengerLogic.
 *
 * @see messenger.cpp for a crash course on ZeroMQ, the current backing implementation.
 */
class Messenger : public common::DedicatedThreadTask {
 private:
  /**
   * A ConnectionRouter that should be added. Unfortunately, ZeroMQ is not thread safe and this must be done from
   * the main serverloop thread for poll to function correctly.
   */
  struct RouterToBeAdded {
    router_id_t router_id_;         ///< The router ID to be assigned.
    ConnectionDestination target_;  ///< The destination to listen on.
    std::string identity_;          ///< The identity to listen as.
    CallbackFn callback_;           ///< The callback to invoke on all messages that are received on this destination.
  };

  /**
   * A ConnectionId that should be established. Unfortunately, ZeroMQ is not thread safe and this must be done from
   * the main serverloop thread for sending/receiving messages to function correctly.
   */
  struct ConnectionToBeAdded {
    connection_id_t connection_id_;  ///< The connection ID to be assigned.
    ConnectionDestination target_;   ///< The target to be connected to.
  };

 public:
  /** Builtin callbacks useful for testing. */
  enum class BuiltinCallback : uint8_t { NOOP = 0, ECHO, ACK, NUM_BUILTIN_CALLBACKS };
  /** @return The callback ID for the builtin callback. */
  static callback_id_t GetBuiltinCallback(BuiltinCallback cb) { return callback_id_t{static_cast<uint64_t>(cb)}; }

  /** @return The default TCP endpoint for a Messenger on the given port. */
  static ConnectionDestination GetEndpointTCP(std::string target_name, uint16_t port);
  /** @return The default IPC endpoint for a Messenger on the given port. */
  static ConnectionDestination GetEndpointIPC(std::string target_name, uint16_t port);
  /** @return The default INPROC endpoint for a Messenger on the given port. */
  static ConnectionDestination GetEndpointINPROC(std::string target_name, uint16_t port);

  /**
   * Create a new Messenger, listening to the default endpoints on the given port.
   * @param port        The port that determines the default endpoints.
   * @param identity    The identity that this Messenger instance is known by. See warning!
   *
   * @warning           Identity must be unique across all instances of Messengers.
   */
  explicit Messenger(uint16_t port, std::string identity);

  /** An explicit destructor is necessary because of the unique_ptr around a forward-declared type. */
  ~Messenger() override;

  /** Run the main server loop, which dispatches messages received to the MessengerLogic layer. */
  void RunTask() override;

  /** Terminate the Messenger. */
  void Terminate() override;

  /**
   * Listen for new connections on the specified target destination. Blocks until the listen is ready.
   *
   * @warning           TODO(WAN): figure out what bad things happen if you give it a ConnectionDestination that is
   *                     already in use. I don't think this is a problem that is likely to occur because all our
   *                     destinations are known at compile time and we don't have too many right now, but I should
   *                     fix this at some point. I am reluctant to add a set of destinations just for this though..
   *
   * @param target      The destination to listen on for new connections.
   * @param identity    The identity to listen as.
   * @param callback    The server loop for all messages received.
   * @return            The ID of the router that was created.
   */
  router_id_t ListenForConnection(const ConnectionDestination &target, const std::string &identity,
                                  CallbackFn callback);

  /**
   * Connect to the specified target destination.
   *
   * @param target      The destination to be connected to. Note that target_name is meaningless here.
   * @return            The ID of the connection that was created.
   *
   * @warning           The default behavior of ZMQ sockets is to allow connections to any target and to queue messages.
   *                    Obtaining a connection_id does NOT guarantee that a successful connection has been made.
   *                    If this is necessary, test the connection explicitly by sending a message through.
   *                    Currently, connection_id is setup so that messages are only queued for successful connections.
   *                    But note that it should not be necessary as your code should handle the case of the target
   *                    going away permanently anyway, in particular consider this ordering of events:
   *                      MakeConnection(target) success -> target dies -> SendMessage(target, ...)
   */
  connection_id_t MakeConnection(const ConnectionDestination &target);

  /**
   * Send a message through the specified connection id.
   *
   * @warning   Remember that ConnectionId can only be used from the same thread that created it!
   *
   * @param connection_id   The connection to send the message over.
   * @param message         The message to be sent.
   * @param callback        The callback function to be invoked locally on the response. Can be nullptr.
   * @param remote_cb_id    The callback function to be invoked remotely on the destination to handle this message.
   *                        For example, used for invoking preregistered functions or messages sent in response.
   *                        To invoke preregistered functions, use static_cast<uint8_t>(Messenger::BuiltinCallback).
   */
  void SendMessage(connection_id_t connection_id, const std::string &message, CallbackFn callback,
                   callback_id_t remote_cb_id);

  /**
   * Send a message through the specified connection router.
   *
   * @warning   A ConnectionRouter only knows about destinations that are directly connected to it.
   *
   * @param router_id       The connection router to send the message over.
   * @param recv_id         The routing ID of the destination.
   * @param message         The message to be sent.
   * @param callback        The callback function to be invoked on the response. Can be nullptr.
   * @param remote_cb_id    The callback function to be invoked remotely on the destination to handle this message.
   *                        For example, used for invoking preregistered functions or messages sent in response.
   *                        To invoke preregistered functions, use static_cast<uint8_t>(Messenger::BuiltinCallback).
   */
  void SendMessage(router_id_t router_id, const std::string &recv_id, const std::string &message, CallbackFn callback,
                   callback_id_t remote_cb_id);

 private:
  friend ConnectionId;
  friend ConnectionRouter;

  static constexpr const char *MESSENGER_DEFAULT_TCP = "*";
  static constexpr const char *MESSENGER_DEFAULT_IPC = "./noisepage-ipc-{}";
  static constexpr const char *MESSENGER_DEFAULT_INPROC = "noisepage-inproc-{}";
  static constexpr const std::chrono::milliseconds MESSENGER_RESEND_TIMER = std::chrono::milliseconds(6666);
  static constexpr const std::chrono::milliseconds MESSENGER_POLL_TIMER = std::chrono::milliseconds(250);
  /** The maximum timeout that a send or recv operation is allowed to block for. TODO(WAN): 30, really? */
  static constexpr const std::chrono::milliseconds MESSENGER_SNDRCV_TIMEOUT = std::chrono::seconds(30);

  /** @return The next callback ID to be used when sending messages. */
  callback_id_t GetNextSendCallbackId();

  /** The main server loop. */
  void ServerLoop();
  /** Add listening points. */
  void ServerLoopAddRouters();
  /** Make new connections to other listening points. */
  void ServerLoopMakeConnections();
  /** Send all queued messages. */
  void ServerLoopSendMessages();
  /** Receive and process any outstanding messages. */
  void ServerLoopRecvAndProcessMessages();

  struct PendingMessage {
    common::ManagedPointer<zmq::socket_t> zmq_socket_;
    std::string destination_id_;
    ZmqMessage msg_;
    bool is_router_socket_;
    std::time_t last_send_time_;
  };

  /**
   * Processes messages.
   * Responsible for special callback functions specified by message ID.
   * Also responsible for invoking the callbacks that were passed in with a SendMessage.
   * Note that these callbacks are distinct from the ServerLoop callbacks common to custom Messenger endpoints, such as
   * replication and the model server messenger.
   *
   * TODO(WAN): Unfortunately, this is also a limitation where the SendMessage() callback must always execute before
   *            the server loop gets a chance to handle the message.
   */
  void ProcessMessage(const ZmqMessage &msg);

  /**
   * Track the messages that have been seen so far by a given replica, additionally returning whether a message is
   * being seen for the first time. Note that this is necessary to simulate idempotence in the Messenger, which tries
   * to periodically resend messages that have not been acknowledged as part of bolting guaranteed delivery onto ZeroMQ.
   *
   * @param replica         The replica sending the message.
   * @param message_id      The ID of the message.
   * @return                True if the message is being seen for the first time. False if seen before.
   */
  bool UpdateMessagesSeen(const std::string &replica, message_id_t message_id);

  /** The port that is used for all default endpoints. */
  const uint16_t port_;
  /** The identity that this instance of the Messenger is known by. */
  const std::string identity_;

  std::unique_ptr<zmq::context_t> zmq_ctx_;
  std::unique_ptr<zmq::socket_t> zmq_default_socket_;
  std::unique_ptr<MessengerPolledSockets> polled_sockets_;
  std::unordered_map<callback_id_t, CallbackFn> callbacks_;

  std::vector<RouterToBeAdded> routers_to_be_added_;
  std::mutex routers_add_mutex_;
  std::condition_variable routers_add_cvar_;
  std::unordered_map<router_id_t, std::unique_ptr<ConnectionRouter>> routers_;

  std::vector<ConnectionToBeAdded> connections_to_be_added_;
  std::mutex connections_add_mutex_;
  std::condition_variable connections_add_cvar_;
  std::unordered_map<connection_id_t, std::unique_ptr<ConnectionId>> connections_;

  std::map<message_id_t, PendingMessage> pending_messages_;
  std::mutex pending_messages_mutex_;

  std::unordered_map<std::string, std::unordered_set<message_id_t>> seen_messages_complement_;
  std::unordered_map<std::string, message_id_t> seen_messages_max_;

  std::mutex callbacks_mutex_;
  bool is_messenger_running_ = false;
  /** The message ID that gets automatically prefixed to messages. */
  std::atomic<message_id_t> next_message_id_{0};
  /** The source callback ID that gets automatically prefixed to messages. */
  std::atomic<callback_id_t> next_callback_id_{static_cast<uint8_t>(BuiltinCallback::NUM_BUILTIN_CALLBACKS) + 1};
  /**
   * The ID of the next listening router to be made from ListenForConnection().
   * Note that this is always accessed while holding routers_add_mutex_.
   */
  router_id_t next_router_id_{0};
  /**
   * The ID of the next outgoing connection to be made from MakeConnection().
   * Note that this is always accessed while holding connections_add_mutex_.
   */
  connection_id_t next_connection_id_{0};
};

/**
 * MessengerManager is the entry point to the Messenger system.
 * MessengerManager is responsible for instantiating the Messenger and then registering the Messenger with the
 * DedicatedThreadRegistry.
 */
class MessengerManager : public common::DedicatedThreadOwner {
 public:
  /**
   * Create and run a new Messenger (which is a DedicatedThreadTask) on the specified thread registry.
   * @param thread_registry The registry in which the Messenger will be registered.
   * @param port            The port on which the Messenger will listen by default.
   * @param identity        The name that this Messenger will be known by. Must be unique across all instances!
   */
  explicit MessengerManager(common::ManagedPointer<common::DedicatedThreadRegistry> thread_registry, uint16_t port,
                            const std::string &identity);

  /** @return The Messenger being managed. */
  common::ManagedPointer<Messenger> GetMessenger() const { return messenger_; }

 private:
  common::ManagedPointer<Messenger> messenger_;
};

}  // namespace noisepage::messenger
