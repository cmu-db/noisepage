#include "messenger/messenger.h"

#include <zmq.hpp>

#include "common/dedicated_thread_registry.h"
#include "common/error/exception.h"
#include "loggers/messenger_logger.h"
#include "messenger/connection_destination.h"
#include "messenger/messenger_logic.h"

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

namespace terrier::messenger {

/** An abstraction around ZeroMQ messages which explicitly have the sender specified. */
class ZmqMessage {
 public:
  /** The routing ID of the message sender. */
  std::string identity_;
  /** The payload in the message. */
  std::string payload_;
};

}  // namespace terrier::messenger

namespace terrier {

/**
 * Useful ZeroMQ utility functions implemented in a naive manner. Most functions have wasteful copies.
 * If perf indicates that these functions are a bottleneck, switch to the zero-copy messages of ZeroMQ.
 */
class ZmqUtil {
 public:
  /** ZmqUtil is a static utility class that should not be instantiated. */
  ZmqUtil() = delete;

  /** The maximum length of a routing ID. Specified by ZeroMQ. */
  static constexpr int MAX_ROUTING_ID_LEN = 255;

  /** @return The routing ID of the socket. */
  static std::string GetRoutingId(common::ManagedPointer<zmq::socket_t> socket) {
    char buf[MAX_ROUTING_ID_LEN];
    size_t routing_id_len = MAX_ROUTING_ID_LEN;
    socket->getsockopt(ZMQ_ROUTING_ID, &buf, &routing_id_len);
    return std::string(buf, routing_id_len);
  }

  /** @return The next string to be read off the socket. */
  static std::string Recv(common::ManagedPointer<zmq::socket_t> socket, zmq::recv_flags flags) {
    zmq::message_t message;
    auto received = socket->recv(message, flags);
    if (!received.has_value()) {
      throw MESSENGER_EXCEPTION(fmt::format("Unable to receive on socket: {}", ZmqUtil::GetRoutingId(socket)));
    }
    return std::string(static_cast<char *>(message.data()), received.value());
  }

  /** @return The next ZmqMessage (identity and payload) read off the socket. */
  static messenger::ZmqMessage RecvMsg(common::ManagedPointer<zmq::socket_t> socket) {
    std::string identity = Recv(socket, zmq::recv_flags::none);
    std::string delimiter = Recv(socket, zmq::recv_flags::none);
    std::string payload = Recv(socket, zmq::recv_flags::none);

    return messenger::ZmqMessage{identity, payload};
  }

  /** @return Send the specified ZmqMessage (identity and payload) over the socket. */
  static void SendMsg(common::ManagedPointer<zmq::socket_t> socket, const messenger::ZmqMessage &msg) {
    zmq::message_t identity_msg(msg.identity_.data(), msg.identity_.size());
    zmq::message_t delimiter_msg("", 0);
    zmq::message_t payload_msg(msg.payload_.data(), msg.payload_.size());
    bool ok = true;

    ok = ok && socket->send(delimiter_msg, zmq::send_flags::sndmore);
    ok = ok && socket->send(payload_msg, zmq::send_flags::none);

    if (!ok) {
      throw MESSENGER_EXCEPTION(fmt::format("Unable to send on socket: {}", ZmqUtil::GetRoutingId(socket)));
    }
  }
};

}  // namespace terrier

namespace terrier::messenger {

ConnectionId::ConnectionId(common::ManagedPointer<zmq::context_t> zmq_ctx, const ConnectionDestination &target,
                           std::string_view identity) {
  // Create a new DEALER socket and connect to the server.
  socket_ = std::make_unique<zmq::socket_t>(*zmq_ctx, ZMQ_DEALER);
  socket_->setsockopt(ZMQ_ROUTING_ID, identity.data(), identity.size());
  routing_id_ = ZmqUtil::GetRoutingId(common::ManagedPointer(socket_));

  socket_->connect(target.GetDestination());
}

ConnectionId::~ConnectionId() = default;

Messenger::Messenger(common::ManagedPointer<MessengerLogic> messenger_logic) : messenger_logic_(messenger_logic) {
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
  zmq_default_socket_->setsockopt(ZMQ_ROUTER_MANDATORY, 1);

  // Bind the same ZeroMQ socket over the default TCP, IPC, and in-process channels.
  {
    zmq_default_socket_->bind(MESSENGER_DEFAULT_TCP);
    zmq_default_socket_->bind(MESSENGER_DEFAULT_IPC);
    zmq_default_socket_->bind(MESSENGER_DEFAULT_INPROC);
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

Messenger::~Messenger() = default;

void Messenger::RunTask() {
  // Run the server loop.
  ServerLoop();
}

void Messenger::Terminate() {
  messenger_running_ = false;
  // TODO(WAN): zmq cleanup? or is that all handled with RAII?
}

void Messenger::ListenForConnection(const ConnectionDestination &target) {
  zmq_default_socket_->bind(target.GetDestination());
}

ConnectionId Messenger::MakeConnection(const ConnectionDestination &target, std::optional<std::string> identity) {
  const auto &identifier = identity.has_value() ? identity.value() : fmt::format("conn{}", connection_id_count_++);
  return ConnectionId(common::ManagedPointer(zmq_ctx_), target, identifier);
}

void Messenger::SendMessage(common::ManagedPointer<ConnectionId> connection_id, std::string message) {
  ZmqMessage msg{connection_id->routing_id_, message};
  ZmqUtil::SendMsg(common::ManagedPointer(connection_id->socket_), msg);
}

void Messenger::ServerLoop() {
  common::ManagedPointer<zmq::socket_t> socket{zmq_default_socket_};

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
