
# Add/Drop Index

## Overview

>*What motivates this to be implemented? What will this component achieve?* 

Currently, the system just creates the index without populating it. This component supports actually adding the index (including populating the index) and/or dropping in a transactionally consistent manner. This is accomplished by blocking any transaction wanting to write to the table as the component builds the index through a sequential scan. Future work includes supporting building with concurrent modifications.

## Scope
>*Which parts of the system will this feature rely on or modify? Write down specifics so people involved can review the design doc*

This mainly touches the traffic cop (to control blocking), index_builder (to build the index), the database catalog (to add the new flags to the indexes, such as live), and transaction stuff (to manage held table locks by a transaction)

The parts of the system this feature will modify include the following files:
 - index_builder.\*
 - ddl_executors.\*
 - traffic_cop.\*
 - transaction_context.\*
 - transaction_manager.\*
 - catalog_accessor.\*
 - database_catalog.\*


## Architectural Design
>*Explain the input and output of the component, describe interactions and breakdown the smaller components if any. Include diagrams if appropriate.*

In broad strokes, this component works by first waiting until all uncommitted transactions that have already modified the table either commit or abort. During this time and until index creation is completed, any transactions that have not already modified the table and wish to do so are blocked and must wait until index creation is complete. After the transactions all complete, index creation proceeds by scanning through the table and inserting all visible versions of each tuple into the index. Once index creation is complete, waiting transactions are allowed to proceed.

While scanning through the table, we can be assured that there are no uncommitted delta records in the version chain, because we know that all transactions that have edited the table have already committed. As such we do not have to worry about reverts occurring that would require us to revert versions in the index.

In practice, this is implemented by having a read/write lock for each table. Any transactions that wishes to modify the table entries acquire a read lock on the table, and the CREATE INDEX transaction acquires a write lock on the table. Transactions that modify the table entries release their lock on the table as soon as the transaction either commits or aborts, and CREATE INDEX transactions release their lock on the table as soon as the index becomes live. This keeps the high level semantics of the waiting.

## Design Rationale
>*Explain the goals of this design and how the design achieves these goals. Present alternatives considered and document why they are not chosen.* 

The goals of the above design are to construct an index correctly, so that once the index is constructed, selects continue to be correct, albeit much faster. We do not put high priority on fast index construction with respect to concurrent table modification, as that is the use case of CREATE INDEX CONCURRENTLY.


We do this using the shared lock to ensure that while we are building the index, we don't have any writes. We originally had a single condition variable lock on the individual index; however, we had to move it to the table class because transactions which don't see the index may still have to see the table needs to be locked. This also makes it easier to have our index construction wait for modifications to complete before starting. 

Additionally, we wait for the writing transactions to fully commit/abort before we start building the index. Originally, we just waited for that individual insert to complete, but we ran into problems when handling aborted transactions, and needed to do messy stuff like add abort callbacks to the uncommitted transactions. This is much cleaner (even if it's a little bit less convenient). This did lead to one small issue with a CREATE INDEX after additions to the table in the same transactions, as that would cause deadlock potentially. We decided to disallow this for now, as it is not a common use case at all (you can just build the index first before additions).

Once we are building the index, we have to scan the version chain such that transactions which are in progress can still see old versions in the index, but these versions get correctly deleted later. We do this using a scan function which takes in a lambda, as that provides clean code that could potentially be used in other systems. We originally were going to write a low-level specialized function inside data_table.cpp, but felt it was too specialized and instead went for a higher-order function.

## Testing Plan
>*How should the component be tested?*

The component is tested using some simple low level tests, as well as a multitude of multithreaded high-level JDBC tests. Because there is no way to completely ensure that certain race conditions ensue (such as having transactions in flight when the CREATE INDEX starts), we provide large tests to maximize the chance that these race conditions occur.

## Trade-offs and Potential Problems
>*Write down any conscious trade-off you made that can be problematic in the future, or any problems discovered during the design process that remain unaddressed (technical debts).*

One trade-off we made was forcing all in-flight transactions with existing modifications to the table to end before starting index creation, rather than allowing index creation to proceed. The reason we made the tradeoff is that not doing so would require a way to go from UndoRecord to the transaction that created the undo record (so that you can register abort and commit actions). The only ways we could see to do this would either require each transaction to register itself in a global map (which would be very inefficient) or add a pointer to the transaction to each UndoRecord (which would significantly increase the memory footprint of the database). Neither of these choices seemed preferrable to slowing down CREATE INDEX - especially since (as mentioned in the design rationale) we do not have a strong incentive to speed up CREATE INDEX in this situation.

Another tradeoff we made was disallowing CREATE INDEX in a transaction with uncommitted changes to the table we are creating the index on. This is because our design forces us to keep a read-lock for the duration of the transaction. The issue with this is that getting a write-lock while holding a read-lock will lead to deadlock on most implementations of read/write lock. We did not consider this workload important or common enough to justify the large design and coding effort required in creating a correct read/write lock that allows upgrading locks. 

## Future Work
>*Write down future work to fix known problems or otherwise improve the component.* 

CREATE INDEX CONCURRENTLY currently does not work at all. This would be the main future work, as that would allow for index construction on a large table without grinding the table insertions to a halt. The way this would work takes a note out of postgres' book, and do two separate scans of the table. The first would take the latest committed version, timestamped at the first transaction (so, just add everything into the index), and put that into the index. Then, we would update the catalog with a separate flag on the index, so writes know to update the index. Finally, the second transaction would then wait for all transactions to end, then scan the table's delta storage for things which were not added to the index, and add those. Finally, it sets the index to live, meaning it can be used for reading as well.


Note that we disallow CREATE INDEX CONCURRENTLY on a multi-command transaction (this is what postgres does), which allows us to end the transaction and start multiple ones without running into issues like locks on the catalog.

Another improvement is things like constraints and foreign keys, which we do not support.

Finally, we currently have the table locks on SqlTable. This could be moved to a separate place later, if needs be.
