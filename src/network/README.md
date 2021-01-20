# Networking

## Overview
A prototype implementation of the Postgres network protocol in C++ in order to support communication with Postgres shell clients (psql) with NoisePage.

## Description

- Expected callee: `DBMain`
- Entry point of the network layer: `NoisePageServer::RunServer()` in `noisepage_server.h`
- `NoisePageServer::RunServer()` will:
    1. Create a `ConnectionDispatcherTask` (CDT) with a specified `ProtocolInterpreter` and a list of file descriptors.  
       Each file descriptor is registered with `libevent` to invoke a callback `connection_dispatcher_fn` whenever  
       the respective file descriptor becomes readable. The `ProtocolInterpreter` is saved for later use.  
       When the CDT is run, a pool of `ConnectionHandlerTask` (CHT) threads are created.
    2. When a file descriptor `fd` becomes readable, the descriptor is dispatched from the CDT to an idle CHT with the `ProtocolInterpreter` from above.
    3. The CHT creates (or reuses) a new `ConnectionHandle` (CH) to handle `fd` and invokes `ConnectionHandle::RegisterToReceiveEvents()`.
    4. The CH makes a `NetworkIOWrapper` around `fd` and registers two events:
       - `workpool_event_`: DOESN'T DO ANYTHING RIGHT NOW. Originally envisioned for future callback support.
       - `network_event_`: Handle transitions through the state machine of the `ProtocolInterpreter`, which is currently always `PostgresProtocolInterpreter`. See footnote A1.
    5. It is through `ProtocolInterpreter::Process()` that control flow proceeds to the next layer of the system.  
       An example is `PostgresProtocolInterpreter::Process() -> SimpleQueryCommand::Exec()`, which goes through the
       `TrafficCop` before returning control flow to the `PostgresProtocolInterpreter`. 
    
**Footnote A1.**
It was envisioned that the internal Terrier protocol (ITP) would use the same network state machine as Postgres does.
However, the `Messenger` system serves this purpose instead. This is because it does not necessarily make sense for
an internal communication protocol to have the same `TryRead`, `TryWrite`, etc., that the Postgres connection handler does.


## Glossary

### Packet types ([PostgreSQL Definitions](https://www.postgresql.org/docs/9.6/protocol-message-formats.html))
#### Client to Server
	* StartupMessage
	* Query (Q)
	* Sync (S)
	* Parse (P)
	* Bind (B)
	* Execute (E)
	* Describe (D)

#### Server to Client
	* AuthenticationOk (R)
	* ErrorResponse (E)
	* ReadyForQuery (Z)
	* EmptyQueryResponse (I)
 	* RowDescription (T)
	* DataRow (D)
	* CommandComplete (C)
