#pragma once

#include <string>
#include <utility>

namespace noisepage::messenger {

/**
 * ConnectionDestination abstracts over the different types of connections that the messenger system can make.
 * Note that no connection is actually made. This class only exists to unify the different types of targets
 * that the messenger system can connect to.
 */
class ConnectionDestination {
 public:
  /**
   * Specify that the connection's destination is over the network, specifically TCP.
   *
   * @note      Since the connection is over TCP, this should always work.
   * @note      Implemented with standard IP sockets.
   *
   * @param target_name The name of the target that is being connected to.
   * @param hostname    The name of the host on which the Messenger is listening for TCP.
   * @param port        The port on which the Messenger is listening for TCP.
   * @return A destination that will go over TCP. Note that no connection is actually made.
   */
  static ConnectionDestination MakeTCP(std::string target_name, std::string_view hostname, int port);

  /**
   * Specify that the connection's destination is another process on the same machine, using IPC.
   *
   * @warning   Because this is Inter-Process Communication (IPC), the destination is only valid if the specified
   *            pathname is really on the same machine!
   *
   * @note      Implemented with Unix domain sockets. Faster than TCP.
   *
   * @param target_name The name of the target that is being connected to.
   * @param pathname    A valid filesystem path on which the Messenger is listening for IPC, e.g., "/tmp/foo-ipc".
   * @return A destination that will go over IPC. Note that no connection is actually made. See warning!
   */
  static ConnectionDestination MakeIPC(std::string target_name, std::string_view pathname);

  /**
   * Specify that the connection's destination is the same process as the current process.
   *
   * @warning   Because this is in-process communication (inproc), the destination is only valid if the specified
   *            endpoint is on the same machine AND the user of this destination is in the same Messenger process!
   *
   * @note      Implemented with a custom ZeroMQ protocol. Faster than TCP and IPC.
   *
   * @param target_name The name of the target that is being connected to.
   * @param endpoint    A valid destination on which the Messenger is listening for inproc, e.g., "foo".
   * @return A destination that will go over a custom protocol. Note that no connection is actually made. See warning!
   */
  static ConnectionDestination MakeInProc(std::string target_name, std::string_view endpoint);

  /** @return The name of the target. */
  const std::string &GetTargetName() const { return target_name_; }

  /** @return The destination in Messenger format. */
  const char *GetDestination() const { return zmq_address_.c_str(); }

 private:
  /** Construct a new ConnectionDestination with the specified address. */
  explicit ConnectionDestination(std::string target_name, std::string zmq_address)
      : target_name_(std::move(target_name)), zmq_address_(std::move(zmq_address)) {}
  const std::string target_name_;
  const std::string zmq_address_;
};

}  // namespace noisepage::messenger
