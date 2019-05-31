# Networking

## Overview
A prototype implementation of the Postgres network protocol in C++ in order to support communication with Postgres shell clients with Terrier.

## Scope
>Which parts of the system will this feature rely on or modify? Write down specifics so people involved can review the design doc

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

## Architectural Design
>Explain the input and output of the component, describe interactions and breakdown the smaller components if any. Include diagrams if appropriate.

## Design Rationale
>Explain the goals of this design and how the design achieves these goals. Present alternatives considered and document why they are not chosen.

## Testing Plan
Unit testing of basic command handling is provided by `test/network/network_test.cpp`.  More complete test coverage will be handled by the Junit integration tests.

## Trade-offs and Potential Problems
>Write down any conscious trade-off you made that can be problematic in the future, or any problems discovered during the design process that remain unaddressed (technical debts).

## Future Work
- Support internal protocol for server-to-server communications
