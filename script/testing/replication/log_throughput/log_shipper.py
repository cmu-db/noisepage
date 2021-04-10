import atexit

import zmq


class LogShipper:
    def __init__(self, log_file: str, primary_identity: str, primary_messenger_port: int, primary_replication_port: int,
                 replica_identity: str, replica_replication_port: int):
        with open(log_file, 'r') as f:
            self.messages = f.readlines()
            self.messages = [message.strip() for message in self.messages]

        self.context = zmq.Context()

        # Default socket bound to the messenger port
        # Probably not needed
        self.default_socket = self.context.socket(zmq.ROUTER)
        self.default_socket.set_string(zmq.IDENTITY, primary_identity)
        self.default_socket.bind(f"tcp://*:{primary_messenger_port}")
        self.default_socket.bind(f"ipc://./noisepage-ipc-{primary_messenger_port}")
        self.default_socket.bind(f"inproc://noisepage-inproc-{primary_messenger_port}")

        # Primary replication socket that listens for messages from the replica
        # Not used right now, but may be needed to send ACKs and things like that
        self.primary_router_socket = self.context.socket(zmq.ROUTER)
        self.primary_router_socket.set_string(zmq.IDENTITY, primary_identity)
        self.primary_router_socket.bind(f"tcp://127.0.0.1:{primary_replication_port}")

        # Replica connection used to send messages to replica
        self.replica_dealer_socket = self.context.socket(zmq.DEALER)
        self.replica_dealer_socket.set_string(zmq.IDENTITY, replica_identity)
        self.replica_dealer_socket.connect(f"tcp://127.0.0.1:{replica_replication_port}")

        atexit.register(self.cleanup_zmq)

    def ship(self):
        for message in self.messages:
            self.send_msg(message)

    def send_msg(self, message: str):
        self.replica_dealer_socket.send_multipart([''.encode('utf-8'), message.encode('utf-8')])

    def cleanup_zmq(self):
        """
        Close the socket when the script exits
        :return:
        """
        self.replica_dealer_socket.close()
        self.primary_router_socket.close()
        self.default_socket.close()
        self.context.destroy()


if __name__ == '__main__':
    shipper = LogShipper("resources/log-messages.txt", "primary", 9022, 15445, "replica1", 15446)
    shipper.ship()
