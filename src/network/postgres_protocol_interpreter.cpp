 #include "network/network_defs.h"
 #include "network/postgres_protocol_interpreter.h"
 #include "network/terrier_server.h"

 #define MAKE_COMMAND(type) \
   std::static_pointer_cast<PostgresNetworkCommand, type>( \
       std::make_shared<type>(curr_input_packet_))
 #define SSL_MESSAGE_VERNO 80877103
 #define PROTO_MAJOR_VERSION(x) ((x) >> 16)

  namespace terrier::network {
  Transition PostgresProtocolInterpreter::Process(std::shared_ptr<ReadBuffer> in,
                                                 std::shared_ptr<WriteQueue> out,
                                                 CallbackFunc callback) {
   if (!TryBuildPacket(in)) return Transition::NEED_READ;
   if (startup_) {
     // Always flush startup packet response
     out->ForceFlush();
     curr_input_packet_.Clear();
     return ProcessStartup(in, out);
   }
   std::shared_ptr<PostgresNetworkCommand> command = PacketToCommand();
   curr_input_packet_.Clear();
   PostgresPacketWriter writer(*out);
   if (command->FlushOnComplete()) out->ForceFlush();
   return command->Exec(*this, writer, callback);
 }

  Transition PostgresProtocolInterpreter::ProcessStartup(std::shared_ptr<ReadBuffer> in,
                                                        std::shared_ptr<WriteQueue> out) {
   PostgresPacketWriter writer(*out);
   auto proto_version = in->ReadValue<uint32_t>();
   LOG_INFO("protocol version: %d", proto_version);

   if (proto_version == SSL_MESSAGE_VERNO) {
     // TODO(Tianyu): Should this be moved from PelotonServer into settings?
     writer.WriteSingleTypePacket(NetworkMessageType::SSL_NO);
     return Transition::PROCEED;
   }

   // Process startup packet
   if (PROTO_MAJOR_VERSION(proto_version) != 3) {
     LOG_ERROR("Protocol error: only protocol version 3 is supported");
     writer.WriteErrorResponse({{NetworkMessageType::HUMAN_READABLE_ERROR,
                              "Protocol Version Not Supported"}});
     return Transition::TERMINATE;
   }

   // The last bit of the packet will be nul. This is not a valid field. When there
   // is less than 2 bytes of data remaining we can already exit early.
   while (in->HasMore(2)) {
     // TODO(Tianyu): We don't seem to really handle the other flags?
     std::string key = in->ReadString(), value = in->ReadString();
     LOG_TRACE("Option key %s, value %s", key.c_str(), value.c_str());
     if (key == std::string("database"))
       //state_.db_name_ = value;
     cmdline_options_[key] = std::move(value);
   }
   // skip the last nul byte
   in->Skip(1);
   // TODO(Tianyu): Implement authentication. For now we always send AuthOK
   writer.WriteStartupResponse();
   startup_ = false;
   return Transition::PROCEED;
 }

  bool PostgresProtocolInterpreter::TryBuildPacket(std::shared_ptr<ReadBuffer> &in) {
   if (!TryReadPacketHeader(in)) return false;

   size_t size_needed = curr_input_packet_.extended_
                        ? curr_input_packet_.len_
                            - curr_input_packet_.buf_->BytesAvailable()
                        : curr_input_packet_.len_;
   if (!in->HasMore(size_needed)) return false;

   // copy bytes only if the packet is longer than the read buffer,
   // otherwise we can use the read buffer to save space
   if (curr_input_packet_.extended_)
     curr_input_packet_.buf_->FillBufferFrom(*in, size_needed);
   return true;
 }

  bool PostgresProtocolInterpreter::TryReadPacketHeader(std::shared_ptr<ReadBuffer> &in) {
   if (curr_input_packet_.header_parsed_) return true;

   // Header format: 1 byte message type (only if non-startup)
   //              + 4 byte message size (inclusive of these 4 bytes)
   size_t header_size = startup_ ? sizeof(int32_t) : 1 + sizeof(int32_t);
   // Make sure the entire header is readable
   if (!in->HasMore(header_size)) return false;

   // The header is ready to be read, fill in fields accordingly
   if (!startup_)
     curr_input_packet_.msg_type_ = in->ReadValue<NetworkMessageType>();
   curr_input_packet_.len_ = in->ReadValue<uint32_t>() - sizeof(uint32_t);

   // Extend the buffer as needed
   if (curr_input_packet_.len_ > in->Capacity()) {
     LOG_INFO("Extended Buffer size required for packet of size %ld",
              curr_input_packet_.len_);
     // Allocate a larger buffer and copy bytes off from the I/O layer's buffer
     curr_input_packet_.buf_ =
         std::make_shared<ReadBuffer>(curr_input_packet_.len_);
     curr_input_packet_.extended_ = true;
   } else {
     curr_input_packet_.buf_ = in;
   }

   curr_input_packet_.header_parsed_ = true;
   return true;
 }

  std::shared_ptr<PostgresNetworkCommand> PostgresProtocolInterpreter::PacketToCommand() {
   switch (curr_input_packet_.msg_type_) {
     case NetworkMessageType::SIMPLE_QUERY_COMMAND:
       return MAKE_COMMAND(SimpleQueryCommand);
     case NetworkMessageType::PARSE_COMMAND:
       return MAKE_COMMAND(ParseCommand);
     case NetworkMessageType::BIND_COMMAND
       :return MAKE_COMMAND(BindCommand);
     case NetworkMessageType::DESCRIBE_COMMAND:
       return MAKE_COMMAND(DescribeCommand);
     case NetworkMessageType::EXECUTE_COMMAND:
       return MAKE_COMMAND(ExecuteCommand);
     case NetworkMessageType::SYNC_COMMAND
       :return MAKE_COMMAND(SyncCommand);
     case NetworkMessageType::CLOSE_COMMAND:
       return MAKE_COMMAND(CloseCommand);
     case NetworkMessageType::TERMINATE_COMMAND:
       return MAKE_COMMAND(TerminateCommand);
     default:
       throw NETWORK_PROCESS_EXCEPTION("Unexpected Packet Type: ");
   }
 }

  void PostgresProtocolInterpreter::CompleteCommand(PostgresPacketWriter &out,
                                                   const QueryType &query_type,
                                                   int rows) {

   std::string tag = QueryTypeToString(query_type);
   out.BeginPacket(NetworkMessageType::COMMAND_COMPLETE)
       .AppendString(tag)
       .EndPacket();
 }

  void PostgresProtocolInterpreter::ExecQueryMessageGetResult(PostgresPacketWriter &out,
                                                             ResultType status) {

   CompleteCommand(out,
                   QueryType::QUERY_INVALID,
                   0);

   out.WriteReadyForQuery(NetworkTransactionStateType::IDLE);
 }

  void PostgresProtocolInterpreter::ExecExecuteMessageGetResult(PostgresPacketWriter &out, terrier::ResultType status)
  {
    CompleteCommand(out, QueryType::QUERY_INVALID, 0);
    return;
  }

 } // namespace terrier
