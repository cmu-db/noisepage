#include "messenger/messenger.h"

#include <zmq.hpp>

#include "common/dedicated_thread_registry.h"
#include "common/error/exception.h"
#include "loggers/messenger_logger.h"
#include "messenger/messenger_logic.h"
#include "spdlog/fmt/fmt.h"

/*
 * A crash course on ZeroMQ (ZMQ).
 *
 * # What is ZeroMQ?
 *
 *   To quote the ZMQ documentation,
 *     In the ZeroMQ universe, sockets are doorways to fast little background communications engines
 *     that manage a whole set of connections automagically for you.
 *
 * # Why use ZeroMQ?
 *
 *
 * # What benefits does ZeroMQ guarantee?
 *
 *   - talk about async io
 *   - talk about automatic reconnect
 *   - talk about easy tcp/ipc/inproc
 *   - talk about multipart messages
 *   - talk about atomic message delivery, including for multipart messages
 *   - talk about automatic message buffering (up to a configurable limit)
 *   - talk about message format being more or less low overhead
 *
 * # What pitfalls does ZeroMQ have?
 *
 *
 *
 * # How does NoisePage use ZeroMQ?
 *
 * - talk about communication patterns
 *
 *   There are established ZMQ communication patterns.
 *   The Messenger uses a ROUTER-DEALER pattern.
 *   This is ZeroMQ terminology for saying that:
 *   1. The server process exposes one main ROUTER socket.
 *   2. The ROUTER socket is a "server" socket that asynchronously sends and receives messages to "clients".
 *      To help the ROUTER route messages, every message is expected to be multipart and of the following form:
 *        ROUTING_IDENTIFIER DELIMITER PAYLOAD
 *        where:
 *          - ROUTING_IDENTIFIER is controlled by setsockopt or getsockopt on ZMQ_ROUTING_ID.
 *          - DELIMITER is an empty message with size 0.
 *          - PAYLOAD is the message itself.
 *   3. ...
 *
 * To find out more about ZeroMQ, the best resource I've found is the official book: http://zguide.zeromq.org/
 */

namespace terrier::messenger {

/** An abstraction around ZeroMQ messages which explicitly have the sender specified. */
class ZmqMessage {
 public:
  /** The routing ID of the message sender. */
  std::string_view identity_;
  /** The payload in the message. */
  std::string_view payload_;
};

}  // namespace terrier::messenger

namespace terrier {

/**
 * Useful ZeroMQ utility functions implemented in a naive manner. Most functions have wasteful copies.
 * If performance indicates that these functions are a bottleneck, switch to the zero-copy messages of ZeroMQ.
 */
class ZmqUtil {
 public:
  /** Static utility class. */
  ZmqUtil() = delete;

  /** The maximum length of a routing ID. Specified by ZeroMQ. */
  static constexpr int MAX_ROUTING_ID_LEN = 255;

  /** @return The routing ID of the socket. */
  static std::string GetRoutingId(zmq::socket_t *socket) {
    char buf[MAX_ROUTING_ID_LEN];
    size_t routing_id_len;
    socket->getsockopt(ZMQ_ROUTING_ID, &buf, &routing_id_len);
    return std::string(buf, routing_id_len);
  }

  /** @return The next string read off the socket. */
  static std::string Recv(zmq::socket_t *socket, zmq::recv_flags flags) {
    zmq::message_t message;
    auto received = socket->recv(message, flags);
    if (!received.has_value()) {
      throw MESSENGER_EXCEPTION(fmt::format("Unable to receive on socket: {}", ZmqUtil::GetRoutingId(socket)));
    }
    return std::string(static_cast<char *>(message.data()), received.value());
  }

  /** @return The next ZmqMessage (identity and payload) read off the socket. */
  static messenger::ZmqMessage RecvMsg(zmq::socket_t *socket) {
    std::string identity = Recv(socket, zmq::recv_flags::none);
    std::string delimiter = Recv(socket, zmq::recv_flags::none);
    std::string payload = Recv(socket, zmq::recv_flags::none);

    return messenger::ZmqMessage{identity, payload};
  }

  /** @return Send the specified ZmqMessage (identity and payload) over the socket. */
  static void SendMsg(zmq::socket_t *socket, const messenger::ZmqMessage &msg) {
    zmq::message_t identity_msg(msg.identity_.data(), msg.identity_.size());
    zmq::message_t delimiter_msg("", 0);
    zmq::message_t payload_msg(msg.payload_.data(), msg.payload_.size());
    bool ok = true;

    ok = ok && socket->send(identity_msg, zmq::send_flags::sndmore);
    ok = ok && socket->send(delimiter_msg, zmq::send_flags::sndmore);
    ok = ok && socket->send(payload_msg, zmq::send_flags::none);

    if (!ok) {
      throw MESSENGER_EXCEPTION(fmt::format("Unable to send on socket: {}", ZmqUtil::GetRoutingId(socket)));
    }
  }
};

}  // namespace terrier

namespace terrier::messenger {

/** An abstraction around successful connections made through ZeroMQ. */
class ConnectionId {
 public:
  /** Create a new ConnectionId that wraps the specified ZMQ socket. */
  explicit ConnectionId(zmq::context_t *zmq_ctx, ConnectionDestination target,
                        std::optional<std::string_view> identity) {
    // Create a new DEALER socket and connect to the server.
    // TODO(WAN): justify DEALER socket.
    socket_ = zmq::socket_t(*zmq_ctx, ZMQ_DEALER);

    if (identity.has_value()) {
      socket_.setsockopt(ZMQ_ROUTING_ID, identity.value().data(), identity.value().size());
    }
    routing_id_ = ZmqUtil::GetRoutingId(&socket_);

    socket_.connect(target.GetDestination());
  }

 private:
  friend Messenger;

  /** The ZMQ socket. */
  zmq::socket_t socket_;
  /** The ZMQ socket routing ID. */
  std::string routing_id_;
};

ConnectionDestination ConnectionDestination::MakeTCP(std::string_view hostname, int port) {
  return ConnectionDestination(fmt::format("tcp://{}:{}", hostname, port).c_str());
}

ConnectionDestination ConnectionDestination::MakeIPC(std::string_view pathname) {
  return ConnectionDestination(fmt::format("ipc://{}", pathname).c_str());
}

ConnectionDestination ConnectionDestination::MakeInProc(std::string_view endpoint) {
  return ConnectionDestination(fmt::format("inproc://{}", endpoint).c_str());
}

Messenger::Messenger(common::ManagedPointer<MessengerLogic> messenger_logic) : messenger_logic_(messenger_logic) {
  // Create a ZMQ context. A ZMQ context abstracts away all of the in-process and networked sockets that ZMQ uses.
  // The ZMQ context is also the transport for in-process ("inproc") sockets.
  // Generally speaking, a single process should only have a single ZMQ context.
  // To have two ZMQ contexts is to have two separate ZMQ instances running, which is unlikely to be desired behavior.

  // Register a ROUTER socket on the default Messenger port.
  // A ROUTER socket is an async server process.
  // TODO(WAN): elaborate yadadada
  zmq_default_socket_ = zmq::socket_t(zmq_ctx_, ZMQ_ROUTER);
  // By default, the ROUTER socket silently discards messages that cannot be routed.
  // By setting ZMQ_ROUTER_MANDATORY, the ROUTER socket errors with EHOSTUNREACH instead.
  zmq_default_socket_.setsockopt(ZMQ_ROUTER_MANDATORY, 1);

  // Bind the same ZeroMQ socket over the default TCP, IPC, and in-process channels.
  {
    zmq_default_socket_.bind(MESSENGER_DEFAULT_TCP);
    zmq_default_socket_.bind(MESSENGER_DEFAULT_IPC);
    zmq_default_socket_.bind(MESSENGER_DEFAULT_INPROC);
  }

  // The following block of code is dead code that contains useful background information on ZMQ defaults.
  {
    // ZeroMQ does all I/O in background threads. By default, a ZMQ context has one I/O thread.
    // An old version of the ZMQ guide suggests one I/O thread per GBps of data.
    // If I/O bottlenecks due to huge message volume, adjust this.
    // zmq_ctx_set(zmq_ctx_, ZMQ_IO_THREADS, NUM_IO_THREADS);
  }

  messenger_running_ = true;
}

void Messenger::RunTask() {
  // Run the server loop.
  ServerLoop();
}

void Messenger::Terminate() {
  messenger_running_ = false;
  // TODO(WAN): zmq cleanup? or is that all handled with RAII?
}

ConnectionId Messenger::MakeConnection(const ConnectionDestination &target, std::optional<std::string> identity) {
  return ConnectionId(&zmq_ctx_, target, identity);
}

void Messenger::SendMessage(ConnectionId *connection_id, std::string_view message) {
  ZmqMessage msg{connection_id->routing_id_, message};
  ZmqUtil::SendMsg(&connection_id->socket_, msg);
}

void Messenger::ServerLoop() {
  zmq::socket_t *socket = &zmq_default_socket_;

  while (messenger_running_) {
    ZmqMessage msg = ZmqUtil::RecvMsg(socket);

    messenger_logic_->ProcessMessage(msg.identity_, msg.payload_);
    messenger::ZmqMessage reply;
    reply.identity_ = msg.identity_;
    reply.payload_ = "pong";
    ZmqUtil::SendMsg(socket, reply);
    MESSENGER_LOG_INFO("SEND \"{}\": \"{}\"", reply.identity_, reply.payload_);
  }
}

MessengerOwner::MessengerOwner(const common::ManagedPointer<common::DedicatedThreadRegistry> thread_registry)
    : DedicatedThreadOwner(thread_registry),
      logic_(),
      messenger_(thread_registry_->RegisterDedicatedThread<Messenger>(this, common::ManagedPointer(&logic_))) {}

}  // namespace terrier::messenger
