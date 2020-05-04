# Add/Drop Index

## Overview

>*What motivates this to be implemented? What will this component achieve?*

Currently, the system just creates the index without populating it. This component will support actually adding the index (including populating the index) and/or dropping in a transactionally consistent manner. This will be achieved by halting any ongoing transactions and maintaining a delta storage while the index is being built, applying the delta changes after the index is done being built. The bonus task would include index building in a multi-threaded fashion.

## Scope
>*Which parts of the system will this feature rely on or modify? Write down specifics so people involved can review the design doc*

The parts of the system this feature will modify include the following files:
 - index_builder.\*
 - ddl_executors.\*
 - catalog_accessor.\*
 - index_schema.\*
 - codegen.\*


## Architectural Design
>*Explain the input and output of the component, describe interactions and breakdown the smaller components if any. Include diagrams if appropriate.*

There are two phases to index creation. In the first phase, the index is created and added to the catalog (with a flag indicating it is not yet ready for use). Then, that transaction will populate the index as if it were a snapshot of the table at the timestamp of the transaction. _Are we using the standard delta store here or are we maintaining a separate delta store_?

At the start of the next phase (which starts at t2), a separate flag is set in the catalog - indicating that any transactions with a timestamp after t2 that wish to modify the column the index is being built on must block. The transaction that is building the index then blocks - waiting for all transactions with timestamp less than t1 to commit. Once that is done, the index building transaction scans through the delta store and updates the index with any changes.

The output of this component is the index created

## Design Rationale
>*Explain the goals of this design and how the design achieves these goals. Present alternatives considered and document why they are not chosen.*

The goals of the above design are to ensure that the system knows when to block and when to not, through the utilization of transaction timestamps.

## Testing Plan
>*How should the component be tested?*

The component will be tested with unit tests to make sure the create index successfully adds existing entries. Multi-threaded tests will be included to make sure there are no bugs with reads/writes happening at the same time as the index is being built. Additionally, the drop functionality will need to be tested.


## Trade-offs and Potential Problems
>*Write down any conscious trade-off you made that can be problematic in the future, or any problems discovered during the design process that remain unaddressed (technical debts).*

## Future Work
>*Write down future work to fix known problems or otherwise improve the component.*

If the bonus task is not accomplished, this can be put as a future work.


