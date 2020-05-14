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
We split the recovery process into two parts: catalog recovery and user table recovery. Since catalog table recovery requires specific order on execution of transactions, we considered it might be simpler to simply redo the logs on catalog tables. Therefore, our overall design remains the log based recovery mechanism for catalog tables, and only takes checkpoints of user defined tables. This design makes use of the high performance of checkpoint as user defined tables grow larger, and at the same time reduces the underlying design difficulty of checkpointing catalog tables.

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

### Checkpoint
We decided to make use of the current ArrowSerializer to serialize data tables into arrow format files on disk. Specific file format is clarified in the ArrowSerializer class. Arrow serializer requires each block to be compacted and contain updated arrow metadata. Therefore, we pass each block through BlockCompactor before creating a checkpoint.

```
   * -----------------------------------------------------------------------------------------------------------------
   * | data_table *(64) | padding (16) | layout_version (16) | insert_head (32) |        control_block (64)          |
   * -----------------------------------------------------------------------------------------------------------------
   * | ArrowBlockMetadata | attr_offsets[num_col] (32) | bitmap for slots (64-bit aligned) | data (64-bit aligned)   |
   * -----------------------------------------------------------------------------------------------------------------
   *
```

A major design challenge is to maintain the consistency between checkpoint and the remaining log. To solve this problem, we grab a lock on LogSerializerTask before taking a checkpoint, so no further log can be written during the time a checkpoint is being taken. We then compact the blocks in each table, release the lock, and then take the checkpoint based on our copies of the data tables.

Before we release the lock on LogManager, we delete all previous logs on user-defined data tables. We currently perform the pruning by reading all logs, and delete the portion on user-defined tables. 

We take checkpoints on an epoch-based rule. Checkpoints will be taken in intervals by scanning through every table. For each data table, we write its content specified by the format mentioned above into a single file. Therefore, a single checkpoint would contain a series of data table files. Eventually, we will write all these data table files into a single file to avoid exploding the filesystem.

### Recovery
The first step is to recover the catalog using the existing log utility. Since all remaining log records in the log file are on catalog tables, we will directly execute the log with the existing recovery utility. The second step is to recover user-defined data tables. We implemented an arrow file deserializer to deserialize checkpoint files taken using ArrowSerializer, and add the blocks into existing data tables created in our first step. The third step is to recover the indexes for each table. We perform re-insert all the tuples into the index one by one and rebuild the index from the start, as is done in recovery manager. 

## Testing Plan
Currently, we provide a test case for our checkpointing approach. It involves creating a data table, performing some basic transactions, taking checkpoints and recovering from the checkpoint. We eventually compare the recovered table with the original table to examine the correctness of our implementation.

We have another test case on the epoch-based checkpointing mechanism, where we force the test to sleep for a fixed period of time, and check for the existence and correctness of a checkpoint.

## Trade-offs and Potential Problems
Firstly, as mentioned previously, we perform checkpoints with no copy of any table, and convert all blocks back to hot blocks after checkpoints. Another approach is to make a deep copy of all data tables, during the time we hold the LogManager’s lock, so we can perform checkpointing based on the deep copies after the lock is released. This alternative approach will result in higher memory consumption, but reduce the time of locking.

Another tradeoff is that we currently prune the log files by reading the original log files, and filtering out all records on catalog tables. This approach is inefficient in that it requires massive reads of logs from the memory. An alternative approach could be creating two separate log streams, one for catalog tables and one for user tables. This will be a lot more efficient since we can directly delete the existing user log record at the time a checkpoint is taken. However, this approach requires massive changes into the current infrastructure (involving 500-800 lines of changes into >10 different files). 

## Future Work
As is mentioned above, `ArrowSerializer` assumes blocks to be fully compacted so it can be converted into arrow format, meaning it requires the Block Compactor to move all blocks to the front with no empty slots. The block compactor now requires the block to be full to be compacted. This requirement may not be met when taking checkpoints because blocks in user tables are very likely to be not full at the time of checkpoint taking. Also, the index handling in the current block compactor is not implemented yet, which may cause errors in ArrowSeiralizer with delete transaction.Thus, in our current test case, we only include transactions with no deletes to ensure the blocks are compact by themselves. Future collaboration with people modifying the block compactor is needed to resolve these issues. 

Another potential improvement is on the parallelization schema when taking a checkpoint. Currently, we use a naive multithreading where a specific number of threads pull tables from a shared queue. Threads continue to process the next table until no tables remain on the queue. Later work could use a more efficient way for threads to collaboratively process one table if any large table is discovered.  

