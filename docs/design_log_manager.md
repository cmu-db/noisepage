# Design Doc: Log Manager

# Overview

The job of the [`LogManager`](https://github.com/cmu-db/terrier/blob/master/src/include/storage/write_ahead_log/log_manager.h) in the system is to receive buffers of [`LogRecord`](https://github.com/cmu-db/terrier/blob/3a5852820cc5f5a4ba1c19f466919e0ec95de064/src/include/storage/write_ahead_log/log_record.h) from transactions and persist them in memory so that the changes can be recovered in the event of a crash. It does this by first serializing. The `LogManager` does this by serializing the `LogRecord` into a recoverable format (described below) into 4K byte buffers. These buffers are then written into the log file, and the log file is periodically persisted. 

In the future, the `LogManager` will also be responsible for shipping logs over the network to replicas.

# LogManager

The `LogManager` has `Start()` and `StopAndPersist()` to bring it up and shut it down. Decoupling this logic from the constructor and destructor (respectively) allows one to start and stop a single log manager repeatedly without having to initialize a new `LogManager` object. As the name suggests, `StopAndPersist()` forces a processing of all unserialized logs, as well as persisting the log file on disk. 

The main entry point for the `LogManager` is through `AddBufferToFlushQueue()`. Transactions will pass their (potentially partially) full buffers of `LogRecord` through this method. After it is called, the `LogManager` owns the buffers, and transactions should no longer reference these buffers.

The `LogManager` also exposes a `ForceFlush()` method for testing. This forces the disk writer task to persist the log file. 

Finally, the bulk of the work of the LogManager is handled by two tasks:
1. [`LogSerializerTask`](https://github.com/cmu-db/terrier/blob/master/src/include/storage/write_ahead_log/log_serializer_task.h)
2. [`DiskLogConsumerTask`](https://github.com/cmu-db/terrier/blob/master/src/include/storage/write_ahead_log/disk_log_consumer_task.h)

These tasks are managed through the [`DedicatedThreadRegistry`](https://github.com/cmu-db/terrier/blob/master/src/include/common/dedicated_thread_registry.h). 

# LogSerializerTask

## Overview

The `LogSerializerTask` is responsible for serializing the `LogRecord` received from transactions into a format that can be processed by log consumers. These serialized records are written into [buffers](https://github.com/cmu-db/terrier/blob/218d072a85f2f87453013cec3c743a6ead9a7aa9/src/include/storage/write_ahead_log/log_io.h#L78), which are handed over to consumers when full. We also extract the commit callbacks from commit records so that they can be called by the `DiskLogConsumerTask` when it guarantees the commit log is persisted in memory. In the case of the `DiskLogConsumerTask`, this means a format that can be written to disk, and then can be used to reconstruct the entire `LogRecord` during recovery.  

## Periodical Processing

The `LogSerializerTask` periodically serializes the logs by sleeping in a loop. The amount of time we sleep is a [settings defined interval](https://github.com/cmu-db/terrier/blob/218d072a85f2f87453013cec3c743a6ead9a7aa9/src/include/settings/settings_defs.h#L30). The reason serialization is done in the background and not as soon as logs are received is to reduce the amount of time a transaction spends interacting with the log manager

## Serialization Format

A `LogRecord` is serialized into the following **recoverable** format depending on the record type:

### RedoRecord
```
----------------------------------------------------------------------------------
| redo record size (32-bit) | record type (8-bit) | txn_begin_timestamp (64-bit) |
----------------------------------------------------------------------------------
|   database_oid (32-bit)   |   table_oid (32-bit)   |    TupleSlot (64-bit)     |
----------------------------------------------------------------------------------
| num_cols (16-bit) | col_id1 (32-bit) | col_id2 (32-bit) |          ...         | 
----------------------------------------------------------------------------------
| col_attr_size1 (8-bit) | col_attr_size2 (8-bit) | ... | null_bitmap (variable) | 
----------------------------------------------------------------------------------
| val1 | val2 | val3_varlen_size (32-bit) | val3_varlen_content | val4 |   ...   |
----------------------------------------------------------------------------------
```
**Notes**:
* The size of the serialization of `null_bitmap` is the "byte-alligned ceiling" of `num_cols`. If `num_cols = 5`, then `null_bitmap` is 8-bit. If `num_cols = 32`, then `null_bitmap` is 32-bit, etc. 
* As can be seen in the last row, var-len's sizes and content are interleaved  with non-varlen values. The size of non-varlen attributes is determined using the `storage::BlockLayout`.
* NULL values are determined using the `null_bitmap` and are **not** serialized out.

### DeleteRecord
```
----------------------------------------------------------------------------------
| redo record size (32-bit) | record type (8-bit) | txn_begin_timestamp (64-bit) |
----------------------------------------------------------------------------------
|   database_oid (32-bit)   |   table_oid (32-bit)   |    TupleSlot (64-bit)     |
----------------------------------------------------------------------------------
```

### CommitRecord
```
-----------------------------------------------------------------------------------------------------------------------------------------------------
| redo record size (32-bit) | record type (8-bit) | txn_begin_timestamp (64-bit) | commit_timestamp (64-bit) | oldest_active_txn_timestamp (64-bit) |
-----------------------------------------------------------------------------------------------------------------------------------------------------
```

### AbortRecord
```
----------------------------------------------------------------------------------
| redo record size (32-bit) | record type (8-bit) | txn_begin_timestamp (64-bit) |
----------------------------------------------------------------------------------
```

# DiskLogConsumerTask

The job of the `DiskLogConsumerTask` is to take logs serialized by the `LogSerializerTask` and persist them on disk. It is important to differentiate between writing to the log file and persisting to disk. Whenever the task thread's wait is interrupted, it will always write the buffers to the log file, but it will only persist the log file if some condition is met. The task will interrupt its wait and write the buffers to the log file if any of the following conditions are met:
1. Someone calls `ForceFlush()` on the log manager
2. We have received a new buffer through the consumer queue
3. The task is being shut down
4. The wait times out (see #1 below)

After writing the log buffers to the file, the task will then persist the file if any of the following conditions are met: 
1. A settings defined amount of time has passed (default: 20ms)
2. A settings defined threshold of memory written has been written since the last persist (default: 1MB)
3. Someone calls `ForceFlush()` on the log manager
4. The task is being shut down

# Typical LogRecord flow

To understand the flow of the log manager, below is an example of how a `LogRecord` goes from a transaction to persisted on disk:
1. The `LogManager` receives buffers containing records from transactions via the `AddBufferToFlushQueue` method, and adds them to the serializer task's flush queue 
2. The `LogSerializerTask` will periodically process and serialize buffers in its flush queue and hand them over to the consumer queue.
3. When a buffer of logs is handed over to a consumer, the consumer will wake up and process the logs. In the case of the `DiskLogConsumerTask`, this means writing it to the log file.
4. The `DiskLogConsumerTask` will persist the log file when the conditions mentioned in the `DiskLogConsumerTask` section are met.
5. When the persist is done, the `DiskLogConsumerTask` will call the commit callbacks for any CommitRecords that were just persisted.

# Future Work

In the future, the `LogManager` will also be in-charge of sending logs over the network to replicas. This will most likely entail having a new `ReplicaLogConsumerTask` that mirrors the `DiskLogConsumerTask`, except instead of writing to disk, it will send the logs over network.