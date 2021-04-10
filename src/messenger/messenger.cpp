#include "messenger/messenger.h"

#include <cinttypes>
#include <mutex>  // NOLINT
#include <vector>
#include <zmq.hpp>

#include "common/dedicated_thread_registry.h"
#include "common/error/exception.h"
#include "loggers/messenger_logger.h"
#include "messenger/connection_destination.h"

/*
 * A crash course on ZeroMQ (ZMQ).
 * To find out more about ZeroMQ, the best resource I've found is the official book: http://zguide.zeromq.org/
 *
 * # What is ZeroMQ?
 *
 *    To quote the ZMQ documentation,
 *      In the ZeroMQ universe, sockets are doorways to fast little background communications engines
 *      that manage a whole set of connections automagically for you.
 *
 * # Why use ZeroMQ?
 *
 *    The Messenger is meant to solve the following problem:
 *    1.  I have a message.
 *    2.  I want to send this message to another target. I don't care if this target is on the same process, on the
 *        same machine but a different process, or on a different machine over the network. The Messenger should
 *        figure it out and pick the best way of sending the message there.
 *
 *    This is exactly ZeroMQ's use case!
 *
 * # What benefits does ZeroMQ guarantee?
 *
 *    1.  ZeroMQ handles switching between tcp, ipc, and in-process communications very easily.
 *    2.  ZeroMQ performs IO asynchronously in the background. Alongside automatic message buffering,
 *        i.e., you can send() from a connected client before the server starts up, and the server will still get it!,
 *        this keeps Messenger from being bottlenecked on sending data over the network.
 *    3.  ZeroMQ automatically reconnects, as long as the dropped node comes back up eventually.
 *    4.  ZeroMQ has support for multipart message delivery.
 *    5.  ZeroMQ has atomic message delivery. This includes for multipart messages.
 *    6.  ZeroMQ has a very light message format that looks like (size | data), where data's format is up to you.
 *
 * # What pitfalls does ZeroMQ have?
 *
 *    1.  ZeroMQ is not truly zero-copy, copies are performed between userspace and kernelspace.
 *
 * # How does NoisePage use ZeroMQ?
 *
 *    ZeroMQ has established communication patterns that cover common use cases.
 *    These communication patterns are better described in the ZeroMQ book, linked above.
 *
 *    The Messenger does NOT use the REQUEST-REPLY pattern, but discussing it will provide useful context.
 *    REQUEST-REPLY (also known as REQ-REP) is the simplest pattern available in ZeroMQ.
 *    There are two types of sockets:
 *      1.  The REQUEST (client) socket, and
 *      2.  The REPLY (server) socket.
 *    All communication must be initiated by the REQUEST socket. In particular, all communication must look like:
 *      REQUEST.send() REPLY.recv() REPLY.send() REQUEST.recv() REQUEST.send() REPLY.recv() ...
 *    Otherwise, if send (or receive) is called twice in a row, an exception is thrown.
 *    This is clearly limiting. However, the reason this restriction exists is because the "one at a time" nature
 *    of send-recv-send-recv simplifies identity management, hiding it completely from the users of ZeroMQ.
 *    To remove the restriction, it is necessary for each message to contain some identity information.
 *
 *    This motivates the ROUTER-DEALER pattern.
 *        ROUTER = async servers.
 *        DEALER = async clients.
 *
 *    The Messenger uses a ROUTER-DEALER pattern.
 *    This is ZeroMQ terminology for saying that:
 *    1.  The server process exposes one main ROUTER socket.
 *    2.  The ROUTER socket is a "server" socket that asynchronously sends and receives messages to "clients".
 *        To help the ROUTER route messages, every message is expected to be multipart and of the following form:
 *        ROUTING_IDENTIFIER DELIMITER PAYLOAD
 *        where:
 *          - ROUTING_IDENTIFIER is controlled by setsockopt or getsockopt on ZMQ_ROUTING_ID.
 *          - DELIMITER is an empty message with size 0.
 *          - PAYLOAD is the message itself.
 *    3.  The DEALER socket must send messages in the same format as well.
 *
 * # Interfacing with the Messenger from Python.
 *
 *    This is an example of how you connect to the Messenger from Python.
 *      import zmq                                    # Use the ZeroMQ library.
 *      ctx = zmq.Context()                           # Create a ZeroMQ context for our entire process.
 *      sock = ctx.socket(zmq.DEALER)                 # We want an async DEALER socket for reasons described above.
 *      sock.setsockopt(zmq.IDENTITY, b'snek')        # Set the name of our Python program,
 *      sock.connect('ipc:///tmp/noisepage-ipc0')     # Connect to NoisePage on the same machine over IPC.
 *      sock.send(b'', flags=zmq.SNDMORE)             # Start building a message. This is the empty delimiter packet.
 *      s.send(b'PThis is the message payload.')      # Finish the message and send it. P is a prefix for Print.
 *
 */

namespace noisepage::messenger {

ZmqMessage ZmqMessage::Build(message_id_t message_id, callback_id_t source_cb_id, callback_id_t dest_cb_id,
                             const std::string &routing_id, std::string_view message) {
  return ZmqMessage{routing_id, fmt::format("{}-{}-{}-{}", message_id.UnderlyingValue(), source_cb_id.UnderlyingValue(),
                                            dest_cb_id.UnderlyingValue(), message)};
}

ZmqMessage ZmqMessage::Parse(const std::string &routing_id, const std::string &message) {
  return ZmqMessage{routing_id, message};
}

ZmqMessage::ZmqMessage(std::string routing_id, std::string payload)
    : routing_id_(std::move(routing_id)), payload_(std::move(payload)), message_(payload_) {
  uint64_t message_id;
  uint64_t source_cb_id;
  uint64_t dest_cb_id;

  NOISEPAGE_ASSERT(!payload_.empty(), "Payload must be defined for valid messages.");
  // TODO(WAN): atoi, stoull, from_chars, etc? Error checking in general.
  UNUSED_ATTRIBUTE int check =
      std::sscanf(payload_.c_str(), "%" SCNu64 "-%" SCNu64 "-%" SCNu64 "-", &message_id, &source_cb_id, &dest_cb_id);
  NOISEPAGE_ASSERT(3 == check, "Couldn't parse the message header.");

  // Remove the prefix up to the third '-'
  message_.remove_prefix(message_.find_first_of('-') + 1);
  message_.remove_prefix(message_.find_first_of('-') + 1);
  message_.remove_prefix(message_.find_first_of('-') + 1);

  message_id_ = message_id_t{message_id};
  source_cb_id_ = callback_id_t{source_cb_id};
  dest_cb_id_ = callback_id_t{dest_cb_id};
}

/** An abstraction around all the ZeroMQ poll items that the Messenger holds. */
class MessengerPolledSockets {
 public:
  /** Wrapper around deficiency in C++ ZeroMQ poll API which doesn't have typed sockets. */
  class PollItems {
   public:
    /** The items to be polled. */
    std::vector<zmq::pollitem_t> items_;
    /** The server callback function to invoke on messages received. */
    std::vector<common::ManagedPointer<CallbackFn>> server_callbacks_;
  };

  /**
   * @return    The list of items to be polled on and their corresponding sockets.
   * @warning   Only one thread should be invoking GetPollItems().
   */
  PollItems &GetPollItems() {
    // If there are pending poll items, add all of them to the main list of poll items.
    if (!writer_.items_.empty()) {
      // Note that there is a TOCTOU here that does not matter. Stale reads are OK.
      // The user of GetPollItems() has to call GetPollItems() repeatedly by design.
      // If item I is added to the writer after the emptiness check,
      // item I will be picked up by the next call to GetPollItems().
      std::scoped_lock lock(mutex_);
      read_.items_.insert(read_.items_.cend(), writer_.items_.cbegin(), writer_.items_.cend());
      writer_.items_.clear();
      read_.server_callbacks_.insert(read_.server_callbacks_.cend(), writer_.server_callbacks_.cbegin(),
                                     writer_.server_callbacks_.cend());
      writer_.server_callbacks_.clear();
    }
    return read_;
  }

  /** Include the specified @p socket on all subsequent calls to pollitems. */

  /**
   * Add a new socket to all subsequent calls to GetPollItems().
   * @param socket      The socket to be added.
   * @param callback    The callback to be invoked on the socket when it receives a message.
   *                    If nullptr, the default messenger server loop will be used to process the message.
   */
  void AddPollItem(zmq::socket_t *socket, common::ManagedPointer<CallbackFn> callback) {
    zmq::pollitem_t pollitem;
    pollitem.socket = socket->operator void *();  // Poll on this raw socket.
    pollitem.events = ZMQ_POLLIN;                 // Event: at least one message can be received on the socket.
    {
      std::scoped_lock lock(mutex_);
      writer_.items_.emplace_back(pollitem);
      writer_.server_callbacks_.emplace_back(callback);
    }
  }

 private:
  /** The items to be polled (reader side). */
  PollItems read_;
  /** The items to be polled that can't be added yet (writer side). */
  PollItems writer_;
  /** Protecting the transfer of poll items from writer side to reader side. */
  std::mutex mutex_;
};

}  // namespace noisepage::messenger

namespace noisepage::messenger {

/**
 * Useful ZeroMQ utility functions implemented in a naive manner. Most functions have wasteful copies.
 * If perf indicates that these functions are a bottleneck, switch to the zero-copy messages of ZeroMQ.
 */
class ZmqUtil {
 private:
  /** @return True if there are more parts of the same multipart message to be received. */
  static bool HasMoreMessagePartsToReceive(const common::ManagedPointer<zmq::socket_t> socket) {
    return socket->get(zmq::sockopt::rcvmore) > 0;
  }

 public:
  /** ZmqUtil is a static utility class that should not be instantiated. */
  ZmqUtil() = delete;

  /** The maximum length of a routing ID. Specified by ZeroMQ. */
  static constexpr int MAX_ROUTING_ID_LEN = 255;

  /** @return The routing ID of the socket. */
  static std::string GetRoutingId(const common::ManagedPointer<zmq::socket_t> socket) {
    return socket->get(zmq::sockopt::routing_id);
  }

  /**
   * @return    The next string to be read off the socket.
   * @warning   Socket must be effectively latched!
   */
  static std::string Recv(const common::ManagedPointer<zmq::socket_t> socket, zmq::recv_flags flags) {
    zmq::message_t message;
    auto received = socket->recv(message, flags);
    if (!received.has_value()) {
      throw MESSENGER_EXCEPTION(fmt::format("Unable to receive on socket: {}", ZmqUtil::GetRoutingId(socket)));
    }
    return std::string(static_cast<char *>(message.data()), received.value());
  }

  /**
   * @return    The next ZmqMessage (identity and payload) read off the socket.
   * @warning   Socket must be effectively latched!
   */
  static ZmqMessage RecvMsg(const common::ManagedPointer<zmq::socket_t> socket) {
    std::string identity = Recv(socket, zmq::recv_flags::none);
    NOISEPAGE_ASSERT(HasMoreMessagePartsToReceive(socket), "Bad multipart message.");
    std::string delimiter = Recv(socket, zmq::recv_flags::none);
    NOISEPAGE_ASSERT(HasMoreMessagePartsToReceive(socket), "Bad multipart message.");
    std::string payload = Recv(socket, zmq::recv_flags::none);

    return ZmqMessage::Parse(identity, payload);
  }

  /**
   * @return    Send the specified identity over the socket. ROUTER sockets must send this before SendMsgPayload().
   * @warning   Socket must be effectively latched!
   */
  static void SendMsgIdentity(const common::ManagedPointer<zmq::socket_t> socket, const std::string &identity) {
    zmq::message_t identity_msg(identity.data(), identity.size());
    bool ok = socket->send(identity_msg, zmq::send_flags::sndmore).has_value();

    if (!ok) {
      throw MESSENGER_EXCEPTION(fmt::format("Unable to send on socket: {}", ZmqUtil::GetRoutingId(socket)));
    }
  }

  /**
   * @return    Send the specified ZmqMessage (delimiter and payload) over the socket.
   * @warning   Socket must be effectively latched!
   */
  static void SendMsgPayload(const common::ManagedPointer<zmq::socket_t> socket, const ZmqMessage &msg) {
    zmq::message_t delimiter_msg("", 0);
    zmq::message_t payload_msg(msg.GetRawPayload().data(), msg.GetRawPayload().size());
    bool ok = true;

    ok = ok && socket->send(delimiter_msg, zmq::send_flags::sndmore).has_value();
    ok = ok && socket->send(payload_msg, zmq::send_flags::none).has_value();

    if (!ok) {
      throw MESSENGER_EXCEPTION(fmt::format("Unable to send on socket: {}", ZmqUtil::GetRoutingId(socket)));
    }
  }
};

/** Utility functions for Messenger maintenance. */
class MessengerUtil {
 public:
  /** Delete the localhost IPC file. */
  static void DeleteLocalIPC(int16_t port) {
    ConnectionDestination dest_ipc = Messenger::GetEndpointIPC("localhost", port);
    const char *ipc_filename_hack = dest_ipc.GetDestination() + std::string("ipc://").length();
    if (0 == std::remove(ipc_filename_hack)) {
      MESSENGER_LOG_INFO(fmt::format("[PID={}] Messenger deleted IPC file: {}", ::getpid(), ipc_filename_hack));
    }
  }
};

}  // namespace noisepage::messenger

namespace noisepage::messenger {

ConnectionId::ConnectionId(common::ManagedPointer<Messenger> messenger, const ConnectionDestination &target,
                           const std::string &identity)
    : target_name_(target.GetTargetName()) {
  // Create a new DEALER socket and connect to the server.
  socket_ = std::make_unique<zmq::socket_t>(*messenger->zmq_ctx_, ZMQ_DEALER);
  socket_->set(zmq::sockopt::routing_id, identity);  // Socket identity.
  socket_->set(zmq::sockopt::immediate, true);       // Only queue messages to completed connections.
  socket_->set(zmq::sockopt::linger, 0);             // Discard all pending messages immediately on socket close.
  socket_->set(zmq::sockopt::sndtimeo, static_cast<int>(Messenger::MESSENGER_SNDRCV_TIMEOUT.count()));
  socket_->set(zmq::sockopt::rcvtimeo, static_cast<int>(Messenger::MESSENGER_SNDRCV_TIMEOUT.count()));
  socket_->connect(target.GetDestination());
  routing_id_ = ZmqUtil::GetRoutingId(common::ManagedPointer(socket_));
  MESSENGER_LOG_TRACE(fmt::format("[PID={}] Registered (but haven't opened!) a new connection to {} ({}) as {}.",
                                  ::getpid(), target_name_, target.GetDestination(), routing_id_.c_str()));
  // Add the new socket to the list of sockets that will be polled by the server loop.
  messenger->polled_sockets_->AddPollItem(socket_.get(), nullptr);
}

ConnectionId::~ConnectionId() = default;

ConnectionRouter::ConnectionRouter(common::ManagedPointer<Messenger> messenger, const ConnectionDestination &target,
                                   const std::string &identity, CallbackFn callback)
    : callback_(std::move(callback)), identity_(identity) {
  // See how zmq_default_socket_ is instantiated for an explanation of these options.
  socket_ = std::make_unique<zmq::socket_t>(*messenger->zmq_ctx_, ZMQ_ROUTER);
  socket_->set(zmq::sockopt::router_mandatory, true);
  socket_->set(zmq::sockopt::routing_id, identity.c_str());
  socket_->set(zmq::sockopt::sndtimeo, static_cast<int>(Messenger::MESSENGER_SNDRCV_TIMEOUT.count()));
  socket_->set(zmq::sockopt::rcvtimeo, static_cast<int>(Messenger::MESSENGER_SNDRCV_TIMEOUT.count()));
  socket_->bind(target.GetDestination());
  MESSENGER_LOG_INFO(
      fmt::format("[PID={}] Messenger ({}) listening: {}", ::getpid(), identity, target.GetDestination()));
  messenger->polled_sockets_->AddPollItem(socket_.get(), common::ManagedPointer(&callback_));
}

ConnectionRouter::~ConnectionRouter() = default;

ConnectionDestination Messenger::GetEndpointTCP(std::string target_name, const uint16_t port) {
  return ConnectionDestination::MakeTCP(std::move(target_name), MESSENGER_DEFAULT_TCP, port);
}

ConnectionDestination Messenger::GetEndpointIPC(std::string target_name, const uint16_t port) {
  return ConnectionDestination::MakeIPC(std::move(target_name), fmt::format(MESSENGER_DEFAULT_IPC, port));
}

ConnectionDestination Messenger::GetEndpointINPROC(std::string target_name, const uint16_t port) {
  return ConnectionDestination::MakeInProc(std::move(target_name), fmt::format(MESSENGER_DEFAULT_INPROC, port));
}

Messenger::Messenger(const uint16_t port, std::string identity) : port_(port), identity_(std::move(identity)) {
  // Create a ZMQ context. A ZMQ context abstracts away all of the in-process and networked sockets that ZMQ uses.
  // The ZMQ context is also the transport for in-process ("inproc") sockets.
  // Generally speaking, a single process should only have a single ZMQ context.
  // To have two ZMQ contexts is to have two separate ZMQ instances running, which is unlikely to be desired behavior.
  zmq_ctx_ = std::make_unique<zmq::context_t>();

  // Register a ROUTER socket on the default Messenger port.
  // A ROUTER socket is an async server process.
  zmq_default_socket_ = std::make_unique<zmq::socket_t>(*zmq_ctx_, ZMQ_ROUTER);
  // By default, the ROUTER socket silently discards messages that cannot be routed.
  // By setting ZMQ_ROUTER_MANDATORY, the ROUTER socket errors with EHOSTUNREACH instead.
  zmq_default_socket_->set(zmq::sockopt::router_mandatory, true);
  // Set the identity that this Messenger will be known by.
  zmq_default_socket_->set(zmq::sockopt::routing_id, identity_.c_str());

  // Bind the same ZeroMQ socket over the default TCP, IPC, and in-process channels.
  {
    ConnectionDestination dest_tcp = GetEndpointTCP("localhost", port_);
    zmq_default_socket_->bind(dest_tcp.GetDestination());
    MESSENGER_LOG_INFO(fmt::format("[PID={}] Messenger listening: {}", ::getpid(), dest_tcp.GetDestination()));

    ConnectionDestination dest_ipc = GetEndpointIPC("localhost", port_);
    MessengerUtil::DeleteLocalIPC(port_);
    zmq_default_socket_->bind(dest_ipc.GetDestination());
    MESSENGER_LOG_INFO(fmt::format("[PID={}] Messenger listening: {}", ::getpid(), dest_ipc.GetDestination()));

    ConnectionDestination dest_inproc = GetEndpointINPROC("localhost", port_);
    zmq_default_socket_->bind(dest_inproc.GetDestination());
    MESSENGER_LOG_INFO(fmt::format("[PID={}] Messenger listening: {}", ::getpid(), dest_inproc.GetDestination()));
  }

  // The following block of code is dead code that contains useful background information on ZMQ defaults.
  {
    // ZeroMQ does all I/O in background threads. By default, a ZMQ context has one I/O thread.
    // An old version of the ZMQ guide suggests one I/O thread per GBps of data.
    // If I/O bottlenecks due to huge message volume, adjust this.
    // zmq_ctx_set(zmq_ctx_, ZMQ_IO_THREADS, NUM_IO_THREADS);
  }

  polled_sockets_ = std::make_unique<MessengerPolledSockets>();
  polled_sockets_->AddPollItem(zmq_default_socket_.get(), nullptr);
  is_messenger_running_ = true;
}

Messenger::~Messenger() = default;

void Messenger::RunTask() {
  try {
    // Run the server loop.
    ServerLoop();
  } catch (zmq::error_t &err) {
    MESSENGER_LOG_ERROR(fmt::format("[PID={}] ServerLoop exited with {}", ::getpid(), err.what()));
    Terminate();
  }
}

void Messenger::Terminate() {
  if (is_messenger_running_) {
    // Shut down the ZeroMQ context. This causes all existing sockets to abort with ETERM.
    zmq_ctx_->shutdown();
    MessengerUtil::DeleteLocalIPC(port_);
    MESSENGER_LOG_INFO(fmt::format("[PID={}] Messenger terminated.", ::getpid()));
  }
  is_messenger_running_ = false;
}

router_id_t Messenger::ListenForConnection(const ConnectionDestination &target, const std::string &identity,
                                           CallbackFn callback) {
  // ZeroMQ is not thread-safe, and the actual binding of new connection endpoints must be done from the same
  // thread that is going to poll the endpoints. See RouterToBeAdded docstring.
  std::unique_lock lock(routers_add_mutex_);
  router_id_t router_id = next_router_id_++;
  routers_to_be_added_.emplace_back(RouterToBeAdded{router_id, target, identity, std::move(callback)});
  routers_add_cvar_.wait(lock);
  return router_id;
}

connection_id_t Messenger::MakeConnection(const ConnectionDestination &target) {
  // ZeroMQ is not thread-safe, and the actual creation of new sockets must be done from the same
  // thread that is going to send and receive using those sockets. See ConnectionToBeAdded docstring.
  std::unique_lock lock(connections_add_mutex_);
  connection_id_t connection_id = next_connection_id_++;
  connections_to_be_added_.emplace_back(ConnectionToBeAdded{connection_id, target});
  connections_add_cvar_.wait(lock);
  return connection_id;
}

void Messenger::SendMessage(const connection_id_t connection_id, const std::string &message, CallbackFn callback,
                            callback_id_t remote_cb_id) {
  common::ManagedPointer<ConnectionId> connection = common::ManagedPointer(connections_.at(connection_id));
  message_id_t msg_id = next_message_id_++;
  callback_id_t sender_cb_id = GetBuiltinCallback(BuiltinCallback::NOOP);
  if (callback != nullptr) {
    sender_cb_id = GetNextSendCallbackId();
    // Register the callback that will be invoked when a response to this message is received.
    callbacks_mutex_.lock();
    callbacks_[sender_cb_id] = std::move(callback);
    callbacks_mutex_.unlock();
  }

  // Build and queue the message to be sent.
  common::ManagedPointer<zmq::socket_t> socket = common::ManagedPointer(connection->socket_);
  {
    std::unique_lock lock(pending_messages_mutex_);
    pending_messages_.emplace(
        msg_id,
        PendingMessage{socket, connection->target_name_,
                       ZmqMessage::Build(msg_id, sender_cb_id, remote_cb_id, connection->routing_id_, message), false});
  }
}

void Messenger::SendMessage(const router_id_t router_id, const std::string &recv_id, const std::string &message,
                            CallbackFn callback, callback_id_t remote_cb_id) {
  common::ManagedPointer<ConnectionRouter> router = common::ManagedPointer(routers_.at(router_id));
  message_id_t msg_id = next_message_id_++;
  callback_id_t send_cb_id = GetBuiltinCallback(BuiltinCallback::NOOP);
  if (callback != nullptr) {
    // Register the callback that will be invoked when a response to this message is received.
    send_cb_id = GetNextSendCallbackId();
    callbacks_mutex_.lock();
    callbacks_[send_cb_id] = std::move(callback);
    callbacks_mutex_.unlock();
  }

  // Build and send the message. Note that ConnectionRouter is a ROUTER socket.
  common::ManagedPointer<zmq::socket_t> socket = common::ManagedPointer(router->socket_);
  {
    std::unique_lock lock(pending_messages_mutex_);
    pending_messages_.emplace(
        msg_id, PendingMessage{socket, recv_id,
                               ZmqMessage::Build(msg_id, send_cb_id, remote_cb_id, router->identity_, message), true});
  }
}

callback_id_t Messenger::GetNextSendCallbackId() {
  callback_id_t send_cb_id = next_callback_id_++;

  // Check for wraparound.
  {
    callback_id_t builtin{static_cast<uint8_t>(BuiltinCallback::NUM_BUILTIN_CALLBACKS)};
    bool wraparound = false;
    while (send_cb_id <= builtin) {
      send_cb_id = next_callback_id_++;
      wraparound = true;
    }
    if (wraparound) {
      MESSENGER_LOG_INFO("[PID={}] Messenger: Message ID wrapped around!");
    }
  }

  return send_cb_id;
}

void Messenger::ServerLoopAddRouters() {
  // Note that the stale read of empty() is probably undefined behavior. If wonky behavior is observed, watch out here.
  if (!routers_to_be_added_.empty()) {
    std::lock_guard lock(routers_add_mutex_);
    for (auto &item : routers_to_be_added_) {
      auto router = std::make_unique<ConnectionRouter>(common::ManagedPointer(this), item.target_, item.identity_,
                                                       item.callback_);
      routers_.try_emplace(item.router_id_, std::move(router));
    }
    routers_to_be_added_.clear();
    routers_add_cvar_.notify_all();
  }
}

void Messenger::ServerLoopMakeConnections() {
  // Note that the stale read of empty() is probably undefined behavior. If wonky behavior is observed, watch out here.
  if (!connections_to_be_added_.empty()) {
    std::lock_guard lock(connections_add_mutex_);
    for (auto &item : connections_to_be_added_) {
      auto connection = std::make_unique<ConnectionId>(common::ManagedPointer(this), item.target_, identity_);
      connections_.try_emplace(item.connection_id_, std::move(connection));
    }
    connections_to_be_added_.clear();
    connections_add_cvar_.notify_all();
  }
}

void Messenger::ServerLoopSendMessages() {
  // Note that the stale read of empty() is probably undefined behavior. If wonky behavior is observed, watch out here.
  if (!pending_messages_.empty()) {
    std::time_t now = std::time(nullptr);

    std::unique_lock lock(pending_messages_mutex_);
    for (auto &item : pending_messages_) {
      PendingMessage &msg = item.second;
      const common::ManagedPointer<zmq::socket_t> socket = msg.zmq_socket_;
      const std::string &destination = msg.destination_id_;

      if (now - msg.last_send_time_ <= MESSENGER_RESEND_TIMER.count()) {
        continue;
      }
      msg.last_send_time_ = now;

      if (msg.is_router_socket_) {
        zmq::message_t router_data(destination.data(), destination.size());
        if (!socket->send(router_data, zmq::send_flags::sndmore).has_value()) {
          throw MESSENGER_EXCEPTION("Could not send message!");
        }
        ZmqUtil::SendMsgIdentity(socket, msg.msg_.routing_id_);
      }
      ZmqUtil::SendMsgPayload(socket, msg.msg_);

      if (msg.is_router_socket_) {
        MESSENGER_LOG_TRACE(fmt::format("[PID={}] Messenger ({}) SENT-TO {}: {} ", ::getpid(), msg.msg_.routing_id_,
                                        destination, msg.msg_.GetRawPayload()));
      } else {
        MESSENGER_LOG_TRACE(
            fmt::format("[PID={}] Messenger SENT-TO {}: {} ", ::getpid(), destination, msg.msg_.GetRawPayload()));
      }
    }
  }
}

void Messenger::ServerLoopRecvAndProcessMessages() {
  // Get the latest set of poll items.
  auto poll_items = polled_sockets_->GetPollItems();
  // Poll on the current set of poll items.
  int num_sockets_with_data = zmq::poll(poll_items.items_, MESSENGER_POLL_TIMER);
  for (size_t i = 0; i < poll_items.items_.size(); ++i) {
    zmq::pollitem_t &item = poll_items.items_[i];
    // If no more sockets have data, then go back to polling.
    if (0 == num_sockets_with_data) {
      break;
    }
    // Otherwise, at least some socket has data. Is it the current socket?
    bool socket_has_data = (item.revents & ZMQ_POLLIN) != 0;
    if (socket_has_data) {
      common::ManagedPointer<zmq::socket_t> socket(reinterpret_cast<zmq::socket_t *>(&item.socket));
      ZmqMessage msg = ZmqUtil::RecvMsg(socket);

      if (msg.GetDestinationCallbackId().UnderlyingValue() != static_cast<uint8_t>(BuiltinCallback::ACK)) {
        zmq::message_t router_data(msg.GetRoutingId().data(), msg.GetRoutingId().size());
        if (!socket->send(router_data, zmq::send_flags::sndmore).has_value()) {
          throw MESSENGER_EXCEPTION("Failed to set router recipient.");
        }
        ZmqMessage ack = ZmqMessage::Build(msg.GetMessageId(), GetBuiltinCallback(BuiltinCallback::NOOP),
                                           GetBuiltinCallback(BuiltinCallback::ACK), identity_, "");
        ZmqUtil::SendMsgIdentity(socket, identity_);
        ZmqUtil::SendMsgPayload(socket, ack);
      }

      bool has_custom_serverloop = poll_items.server_callbacks_[i] != nullptr;
      MESSENGER_LOG_TRACE("[PID={}] Messenger RECV-FR {} (custom serverloop: {}): {}", ::getpid(), msg.GetRoutingId(),
                          has_custom_serverloop, msg.GetRawPayload());
      // See the ProcessMessage() docstring.
      // ProcessMessage() must always be invoked so that the callback that was passed in with SendMessage() is invoked.
      ProcessMessage(msg);
      if (msg.GetDestinationCallbackId().UnderlyingValue() != static_cast<uint8_t>(BuiltinCallback::ACK)) {
        std::string sender_id(msg.GetRoutingId());
        bool first_time = UpdateMessagesSeen(sender_id, msg.GetMessageId());
        if (!first_time) {
          continue;
        }
        if (has_custom_serverloop) {
          auto &server_callback = poll_items.server_callbacks_[i];
          (*server_callback)(common::ManagedPointer(this), msg);
        }
      }

      --num_sockets_with_data;
    }
  }
}

bool Messenger::UpdateMessagesSeen(const std::string &replica, const message_id_t message_id) {
  // See the algorithm description in docs/design_messenger.md.
  if (seen_messages_max_.find(replica) == seen_messages_max_.end()) {
    seen_messages_max_.emplace(replica, message_id);
    return true;
  }

  if (seen_messages_complement_.find(replica) == seen_messages_complement_.end()) {
    seen_messages_complement_.emplace(replica, std::unordered_set<message_id_t>());
  }

  std::unordered_set<message_id_t> &complement = seen_messages_complement_.at(replica);
  message_id_t &max_seen = seen_messages_max_.at(replica);

  bool first_time = false;
  if (message_id > max_seen) {
    for (message_id_t i = max_seen + 1; i < message_id; ++i) {
      complement.emplace(i);
    }
    max_seen = message_id;
    first_time = true;
  } else if (complement.find(message_id) != complement.end()) {
    complement.erase(message_id);
    first_time = true;
  }

  return first_time;
}

void Messenger::ProcessMessage(const ZmqMessage &msg) {
  auto recv_cb_id = msg.GetDestinationCallbackId();
  switch (recv_cb_id.UnderlyingValue()) {
    case static_cast<uint8_t>(BuiltinCallback::NOOP): {
      // Special function: NOOP.
      break;
    }
    case static_cast<uint8_t>(BuiltinCallback::ECHO): {
      // Special function: ECHO server.
      // ROUTER sockets must send their intended recipient as the first sndmore packet.
      // router_data must be the identity of a peer that connected directly to the router socket.
      MESSENGER_LOG_TRACE(
          fmt::format("[PID={}] Callback: echo {} {}", ::getpid(), msg.GetRoutingId(), msg.GetRawPayload()));
      zmq::message_t router_data(msg.GetRoutingId().data(), msg.GetRoutingId().size());
      if (zmq_default_socket_->send(router_data, zmq::send_flags::sndmore).has_value()) {
        ZmqMessage reply = ZmqMessage::Build(next_message_id_++, GetBuiltinCallback(BuiltinCallback::NOOP),
                                             msg.GetSourceCallbackId(), identity_, msg.GetMessage());
        ZmqUtil::SendMsgIdentity(common::ManagedPointer(zmq_default_socket_.get()), identity_);
        ZmqUtil::SendMsgPayload(common::ManagedPointer(zmq_default_socket_.get()), reply);
      } else {
        throw MESSENGER_EXCEPTION("Failed to set router recipient.");
      }
      break;
    }
    case static_cast<uint8_t>(BuiltinCallback::ACK): {
      std::unique_lock lock(pending_messages_mutex_);
      message_id_t msg_id = msg.GetMessageId();
      if (pending_messages_.find(msg_id) != pending_messages_.end()) {
        pending_messages_.erase(msg_id);
      }
      // Otherwise, assume this ACK is a retransmission and that it can be safely dropped.
      break;
    }
    default: {
      NOISEPAGE_ASSERT(recv_cb_id.UnderlyingValue() > static_cast<uint8_t>(BuiltinCallback::NUM_BUILTIN_CALLBACKS),
                       "Bad message ID.");
      // Default: there should be a stored callback.
      std::unique_lock lock(callbacks_mutex_);
      MESSENGER_LOG_TRACE(fmt::format("[PID={}] Callback: invoking stored callback {}", ::getpid(), recv_cb_id));
      if (callbacks_.find(recv_cb_id) == callbacks_.end()) {
        // Assume that this is a retransmission.
        break;
      }
      auto &callback = callbacks_.at(recv_cb_id);
      callback(common::ManagedPointer(this), msg);
      callbacks_.erase(recv_cb_id);
    }
  }
}

void Messenger::ServerLoop() {
  while (is_messenger_running_) {
    // Note that listening routers and outgoing connections _have_ to be established on the Messenger thread, and
    // similarly all messages must be sent to and from the Messenger thread, as part of a ZeroMQ limitation where
    // a ZeroMQ socket MUST be used from the thread that created it.

    ServerLoopAddRouters();
    ServerLoopMakeConnections();
    // TODO(WAN): For higher performance in the future, Send and Recv should be fused into the same zmq socket poll.
    //            Right now, messages are only sent every MESSENGER_POLL_TIMER instead of being sent whenever available.
    ServerLoopSendMessages();
    ServerLoopRecvAndProcessMessages();
  }
}

MessengerManager::MessengerManager(const common::ManagedPointer<common::DedicatedThreadRegistry> thread_registry,
                                   const uint16_t port, const std::string &identity)
    : DedicatedThreadOwner(thread_registry),
      messenger_(thread_registry_->RegisterDedicatedThread<Messenger>(this, port, identity)) {}

}  // namespace noisepage::messenger
