# Checkpoint Recovery

## Overview
Terrier uses a log-based recovery mechanism that rebuilds the entire system through log records only. We propose a checkpoint recovery mechanism that saves the replaying process. The checkpoint contains information of all the tables, which can be used to directly recover the database to its previous state without looking through logs one by one. Ideally, we will recover the system using both checkpoint and log records; the system should find the most recent checkpoint and apply log records to reach the full state.

## Scope
Files/folders that will be modified:
   - storage/recovery/recovery_manager
   - storage/catalog/*
   - storage/recovery_test

New files/folder:
   - storage/checkpoint/*

## Architectural Design
Our implementation is divided into three sections: checkpoint formatting, creating checkpoints and recovery.
The overall pipeline is as follows:
```
   * Storing Checkpoint:
   * Logs -> Filtered Catalog Logs -> Save to Disk
   * Data Tables -> Compressed Cold Data -> Serialized Arrow Format -> Save to Disk
   * -----------------------------------------------------------------------------------------------------------------
   * Recovering Checkpoint:
   * Catalog Logs -> Log Recovery -> Catalog
   * Arrow Format -> Raw Blocks -> Data Tables
```

### Checkpoint Formatting
We are still debating on whether to use the internal Arrow format or our own recovery format. The Arrow format would be defined in arrow_serializer, which is being used to dump tables for data analysis.
Currently, our checkpoint has the following header format, followed by data table content:
```
   * -----------------------------------------------------------------------------------------------------------------
   * | data_table *(64) | padding (16) | layout_version (16) | insert_head (32) |        control_block (64)          |
   * -----------------------------------------------------------------------------------------------------------------
   * | ArrowBlockMetadata | attr_offsets[num_col] (32) | bitmap for slots (64-bit aligned) | data (64-bit aligned)   |
   * -----------------------------------------------------------------------------------------------------------------
   *
```

### Creating Checkpoints
We plan to take checkpoints on an epoch-based rule. Checkpoints will be taken in intervals by scanning through every table. For each data table, we write its content specified by the format mentioned above into a single file. Therefore, a single checkpoint would contain a series of data table files. Eventually, we will write all these data table files into a single file to avoid exploding the filesystem.

### Recovery
Recovery consists of three steps:
The first step is to recover the catalog. This is a tricky step as the catalog information is also contained in the data table files. We are still developing ideas for recovering the catalog. Currently, we use the existing log recovery for restoring the catalog.
The second step is to recover all the data tables. The files are interpreted and dumped directly into the DataTables. For var-length datas, we reconstruct new pointers to the var-length content and replace the deprecated pointers.
The third step is to recover the index. One solution would be to re-insert all the tuples into the index one by one and rebuild the index from the start. The other solution would be to store the primary index as well, so we can load the index directly. This approach, however, requires careful handling with pointers.


## Design Rationale
On a high level, we seperate the recovery of catalog tables from recovery from data tables. For catalog information and catalog table data and indices, we make use of the existing write-ahead logs to recover those portions. For all other user created data tables, we recover the content of the data tables with our checkpoint mechanism. This approach simplifies the implementation of checkpointing to stay in the scope of user-defined data-table. It also provides us with the assumption that catalog tables are fully recovered before we recover from the checkpoint. We believe this approach will still take the advantage of using checkpoint, since in a larger database the majority of data that requires recovery will lie within user defined data tables.

Another major design decision we made is to deploy our own file format instead of using the existing arrow serializer. The reasoning is that we believe the current arrow format includes a lot of unnecessary information not needed for recovery. As we already performed all catalog recovery through the existing recovery from logging, recovery of the data tables’ content will require very little metadata about table columns. Therefore, we have created our own file format for this part.

Finally, we design the recovery of table index to be performed in a tuple by tuple fashion. After we recover the table content, we iterate over each tuple available in the data table, and update the corresponding table index as an insert operation. We believe this approach makes full use of the efficiency of our checkpoints by eliminating the cost of performing deletes on indexes, and at the same time minimizes the design cost of hard coding updates into table indexes.

## Testing Plan
Currently, we only provide a single test case for our checkpointing approach. It involves creating a data table, performing some basic transactions, taking checkpoints and recovering from the checkpoint. We eventually compare the recovered table with the original table to examine the correctness of our implementation.

In the final stage, the recovery framework should work for all existing test cases in recovery_test.cpp, with a checkpoint taken before shutting down the system and previous log deleted.

We will eventually rely on recovery_benchmark.cpp to compare the performance of our approach to the existing one.


## Trade-offs and Potential Problems
For the checkpoint part, the current design creates several threads and lets one thread write the data for exactly one table to disk. To inform the recovery stage the corresponding table for each file, the filename format db_oid-table_oid is used. Considering catalog table recovery is a separated process from current user table recovery, the order of the tables is not recorded. This design simplifies the implementation of multithreading in this phase, but may introduce two potential problems. First, the catalog table may need to be recovered from the checkpoint in some situations. The database has to recover the catalog table before recovering any other tables. This requires the checkpoints to record the order of table data files. Also, if the order of files is recorded in the filename or in any other data structures, traversing all the files to find the one for the catalog table increases the total recovery time when doing the recovery. 

Current recovery mechanism recovers data tables directly from the checkpoints without touching the sql tables related to them. Also, the checkpoint class accesses the data table directly from a given sql table. It is possible that later, one sql table can have multiple data tables related to it. Thus, the checkpoint and recovery mechanism may need to be modified to handle the recovery of sql tables as well in the future. 

Finally, current checkpoints record the content field of each block in a data table for the convenience of software engineering, which includes repeated block headers. If given many giant tables, the redundant headers might consume relatively large space on the disk. 


## Future Work
As mentioned in the previous section, the current design of checkpoints taking creates one file for each table with the filename db_oid-table_oid. Future work can modify the write_to_disk function in current checkpoint class to record all the tables in one large file, which ensures the order correctness of the tables to be recovered. Also, in the current design of checkpoints taking, each table is processed by one thread. People can consider a better multithreading plan such as allowing threads which finish early to process a part of work from those process large tables to reduce the total time for taking checkpoints. 

