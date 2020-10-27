# Design Doc: Storage

## Basics
The storage engine is designed to be an in-memory column store, organized in 1 MB blocks.
It is fully integrated with a concurrency control system and a garbage collection mechanism. We use HyPer-style MV-OCC,
with delta-storage, newest-to-oldest and in-place updates. Currently only snapshot isolation is implemented, and thus there is no need for a validation phase yet. Read
[this paper](https://15721.courses.cs.cmu.edu/spring2018/papers/06-mvcc2/p677-neumann.pdf) for more detail.

## Block Organization
As mentioned before, we arrange our data in 1 MB blocks. The blocks themselves are internally organized similar to
[PAX](http://www.pdl.cmu.edu/PDL-FTP/Database/pax.pdf), but changes are necessary to adapt the storage model to work
in a main-memory setting. Notable changes include:
- Lack of the concept of an "id" for blocks. Since they are in memory a physical pointer suffices to identify them.
- Lack of the concept of varlen fields. We store instead physical pointers to entries in dedicated varlen pools.
- Addition of the "layout version" field, which we will use to support concurrent schemas.

The actual layout is reproduced below:
```
-----------------------------------------------------------------------
| layout_version (32-bit) | num_records (32-bit) | num_slots (32-bit) |
-----------------------------------------------------------------------
| attr_offsets[num_attrs] (32-bit) |       num_attrs (16-bit)         |
-----------------------------------------------------------------------
| attr_sizes[num_attrs] (8-bit) |  Miniblock 1 | Miniblock 2 |   ...  |
-----------------------------------------------------------------------
```

Here a `Miniblock` is how we store an entire column. The internal layout is as follows:
```
-------------------------------------------------------------------------------
| null_bitmap[num_slots] (1-bit) | attr_values[num_slots] (size of attribute) |
-------------------------------------------------------------------------------
```

Note that everything is padded to align to its own size, and every `Miniblock` is aligned to 8 bytes. Be aware that we use least-significant byte order (LSB) within each byte if the bitmap. 

## Storage Layer Overview
The storage engine is designed to have physical-level storage concepts only (bytes, pointers, etc.), and generally has no
notion of SQL-level constructs such as types, constraints, varlen entries, etc., and can be considered a key-value store that only stores
fixed-length objects. This is to say that the storage engine will rely on external indexes and execution engine checks to
enforce SQL level rules, and store physical pointers to objects for varlen entries.

## A Standardized Storage Layer Vocabulary

Here is how we use some ambiguous terms in the system:

- offset: number of bytes to jump from a known location to get to something. (`attr_offsets`)
- index: usually index of a list, for example indexes within the projection list in a `ProjectedRow`.
- column id: within a block, the index assigned to the column in the array fields (`attr_offsets`, `attr_sizes`, etc.) This is
             given special meaning in the system as it is the canonical representation of a column in a table.
- attribute: A single value within a tuple

## Storage API
Here are high level descriptions of the various classes and concepts in the storage layer. Code-level APIs are documented
via doxygen.

### TupleSlot
------
We use a `TupleSlot` as a key for stored objects, which always corresponds directly with a physical memory location.
More specifically, a `TupleSlot` is similar to a Postgres `ctid`, in that it is a pair of block id and block offset.
Since we are in memory, we simply use the raw pointer to the start of the block instead of the block id, and use some
bit-packing tricks to put both values into a single 64-bit value.

Since all blocks are 1 MB in size, it is guaranteed that the head of two blocks are always 1MB apart at least, and we can
tell which block any given physical pointer might be in, provided we know the possible starts of blocks. We take advantage
of this fact by always aligning the head of a block to 1 MB. Then, we only need the higher 64 - 20 = 44 bits inside a
64-bit pointer to locate the head of the block. The remaining 20 bits are guaranteed to be large enough to hold the
possible offset values within a block, because any tuple has to be at least 1 byte in size, so there can at most be 2 ^ 20
tuples and unique offsets within a block.

In reality, with headers and other metadata, we will even have a couple bits extra to spare. This can be useful in the future
if we need to pack more states into a `TupleSlot`.

### BlockLayout and TupleAccessStrategy
------
Since some computation is required to layout a block properly with paddings, and to calculate the number of slots a block would
have with those paddings, most of the work is done up-front and stored within a `TupleAccessStrategy` object. We originally intended
for the object to be a stand-in for compiled code, but it turns out just by doing these computations up-front we are already very
fast. You need a `TupleAccessStrategy` to be able to initialize and interpret blocks correctly.

The `BlockLayout` is a read-only piece of meta data that describes the columns stored in the blocks and their sizes. It is essentially
the value object we use to pass around information about a block.

### VarlenEntry
------
A variable length value is stored in our system as a 16-byte column, which may or may include a pointer to a chunk of memory on the heap where the actual varlen value lives. This is 16 bytes because we potentially need to store an 8-byte pointer plus a 4-byte size field, and have to make sure all values are aligned when stored in columns. This is wasteful for smaller varlens, so we can choose to inline values instead of storing pointers. A
diagram of how members are laid out in a `VarlenEntry` object:

```
---------------------------------------------------------------
| size (4-byte) | prefix (4-byte) | pointer / suffix (8-byte) |
---------------------------------------------------------------
```

The padding between the 4-byte size and 8 byte pointer is used to store the first 4 bytes of a varlen value, to facilitate potential fast filtering. Notice that the prefix is a copy, the varlen entry pointed to still has these bytes. In the case where size is less than or equal to 12, we are able to store the entire varlen value within this object and omit the pointer. The value is stored contiguously from the start of the prefix member, and the pointer field is used as a suffix. Note here that accessing prefix on an inlined value still returns the correct prefix in this setup, and a call to `Content()` preserves the semantics of the non-inlined version (it returns `Prefix()` if inlined).  Always access prefix and inline threshold using the provided function `InlineThreshold()` and `PrefixSize()` to avoid magic numbers.

To create a `VarlenEntry`, use the static factory methods `VarlenEntry::Create` and `VarlenEntry::CreateInline`. The caller is responsible for checking which one should be used by comparing the size of the varlen with `InlineThreshold()`. The reason why we don't incorporate this checkin within the constructor is because, depending on whether the entry is inlined, the object either takes over ownership (if not inlined) or does not
(if inlined), and having a split constructor forces programmers to think about this.

A sign bit from the size field is used to keep track of whether the GC is allowed to deallocate the varlen entry's pointer value. A varlen entry could point to Arrow varlen buffer or a compressed block's dictionary, which cannot be freed by themselves. Simply call `NeedReclaim` to see if it needs to be reclaimed by the GC as a stand-alone entry. 

### DataTable
------
In its current form, the `DataTable` is a simple "key"-value store that is also transactional and supports multi-versioning. The key is
in quotes because we use a `TupleSlot` as a key to lookup a tuple, which is not supplied by the client and actually represents a physical
location in memory.

### ProjectedRow and ProjectedColumns
------
As a delta-storage system, the concept of a partial image of a tuple is central to the storage engine. Normally, we use
`ProjectedRow` to represent a partial tuple. A `ProjectedRow` is not really a full object, but a reinterpereted piece of
memory assumed to have the following layout:

```
 -------------------------------------------------------------------------------
 | size | num_cols | col_id1 | col_id2 | ... | val1_offset | val2_offset | ... |
 -------------------------------------------------------------------------------
 | null-bitmap (pad up to byte) | val1 | val2 | ...                            |
 -------------------------------------------------------------------------------
```

We use this structure both as input and output for the storage engine, and embedded into redo and undo records for use
in the concurrency control system.

Analogous to `ProjectedRow` is the concept of `ProjectedColumns`, except that these objects hold multiple tuples at the
same time, and is laid out contiguously on each column. To make this more concrete, the memory layout of `ProjectedColumns`:

```
 -----------------------------------------------------------------------
 | size | max_tuples | num_tuples | num_cols | col_id1 | col_id2 | ... |
 -----------------------------------------------------------------------
 | val1_offset | val2_offset | ... | TupleSlot_1 | TupleSlot_2 |  ...  |
 -----------------------------------------------------------------------
 | null-bitmap, col_id1 | val1, col_id1 | val2, col_id1 |      ...     |
 -----------------------------------------------------------------------
 | null-bitmap, col_id1 | val1, col_id2 | val2, col_id2 |      ...     |
 -----------------------------------------------------------------------
 |                                ...                                  |
 -----------------------------------------------------------------------
```
We use this structure when providing vectorized access to stored data, such as on a sequential scan. The reason we need
to do this is because as a delta-store, we will always need to materialize a tuple before processing. This also means
that a filled `ProjectedColumns` object is guaranteed to not have any holes within it, i.e. the tuples are always
filled out from position 0 to `num_tuples`, with their source `TupleSlot` in the corresponding position on the list
of `TupleSlot`s


## Concurrency Control
We use HyPer-style multi-version concurrency control for our system. The concurrency control system is delta based
and updates records in-place. We highly recommend that you read the original paper in full for context, but we will
sum up the important bits here. We will then describe our own implementation.

### Isolation Level
------
The current storage engine only supports snapshot isolation, which does not protect against write skews or
phantoms. The HyPer paper details how a variation of MV-OCC can be implemented on top of the current system
to achieve full serializability. Several details of the HyPer model is changed or unimplemented because of this.

### Timestamps
------
We use a simple logical timestamp to order transactions. The HyPer paper states that a transaction will get three
timestamps during its life time: a begin timestamp, a commit timestamp if the transaction commits, and a uniquely-
identifying transaction id that is logically larger than any possible begin or commit timestamps. (negative values
in a 64-bit integer, but treated as unsigned).

In our implementation, a transaction receives only 2, and we derive the "transaction id" from a transaction's begin
timestamps, as they are already uniquely-identifying. We simply flip the sign bit of the begin timestamp to obtain
a HyPer style transaction id. Aborting transactions don't receive a commit timestamp.

### Lock-free Version Chains
------
Our concurrency control system is generally lock-free, with the exception of transaction begins and ends, where the
transaction manager has to update global data structures atomically. For every tuple, we keep a special column
called the "version vector" that stores a physical pointer, which would form a singly linked list of undo records.
It is easy to maintain the said linked list, known as the version chain, in a lock-free manner. The column is
simply null if no older version is visible. Every version chain node, or an undo record, stores a physical
"before image" of the tuple modified, as well as the timestamp of the transaction that modified it. The timestamp
is either the transaction's id or the commit timestamp for committed transactions.

On a tuple read, the reading thread first materializes the latest version into the output buffer, and then follows
the version chain, copying relevant before-images into the buffer, until a version that is visible has been
constructed from the version chain, and the buffer is returned. Uncommited versions will not be read because the
timestamp on the version will be larger than any running transaction's begin timestamp. There is special case logic so that an uncommitted transaction can read its own changes.

On a tuple update, the updating thread first copies the current version of the tuple into an undo record, and then
attempts to atomically prepend the record to the version chain. Note that HyPer, and by extension, us, does not
allow for write-write conflict, so the version chain prepend serves as an implicit "write lock", and transactions
will abort if the head of the version chain is not visible to it. At this point, the transaction is free to make
in-place changes as reading transactions can reconstruct the version from the version chain anyway, and no other
transaction can be updating the tuple.

### Undo Records
------
One innovative approach HyPer took was to store the undo records inside the transaction's context. They are still
accessible as linked list nodes, but a transaction can easily iterate through all of them locally on commit or
abort. This replaces the write set found in a traditional OCC system. (For symmetry, the read set is replaced
with a predicate that is evaluated against the deltas of all recently committed transactions, but that is relevant
for serializability only)

Our system implements this by embedding a vector of pointers to what we call "BufferSegments", a fixed-size chunk
of memory we obtain from object pools. Firstly, a naive implementation using a resizable buffer does not work
since resizing usually changes the memory location of the elements, which would break the version chain since the
original location is pointed to. We provide the illusion of a continuous, unbounded buffer for undo records
by taking advantage of the fact that the system never removes a record until the transaction is done, and the
transaction only traverses them sequentially. We then fill each segment sequentially and request a new one to
logically append to the end of the last one if there is no space left for the next entry.

### Committing and Aborting
------
The transaction manager manages lifecycles of transactions, and must maintain several global data structures
for the rest of the system to work correctly:
- A global running transactions table. We need this information for the garbage collector to come and deallocate
  transactions that are no longer visible anymore
- Potentially a recently committed transaction table, so the transaction manager can check a committing transaction
  for conflicts, on serializable isolation level

On commit, a transaction obtains a commit timestamp from the transaction manager and is removed from the table
of running transactions. The transaction then iterates through its local undo buffer and changes the timestamp
in the undo records, effectively make its changes visible to the rest of the system.

We do not implement the second data structure for the time being, and this results in rare race conditions.
Observe first that the commit process we have described is not atomic, and cannot be since a transaction can
have arbitrarily large write sets. If a transaction begins while another transaction is in the process of
changing the timestamps on its undo record, it could skip the committing version in an earlier read and see the
committing version in a later read from the committing transaction, because the flipping hasn't taken place on the
first one but is completed on the second one. Alternatively, a transaction can read a previous version initially
before the committing transaction flipped the and come back to the same tuple after the commit is complete,
resulting in a unrepeatable read. This anomaly would have been discovered in the serializability check, but since
we did not implement it, we rely on a global shared lock to prevent this from happening. Specifically, no
transaction is allowed to start while another transaction is committing. (the HyPer implementation also has
a global latch, just that they do not include flipping timestamps into the critical section.)

On an abort, similarly the transaction manager removes the transaction from the table of running transactions. In
addition, however, all the changes from the transaction needs to be reverted. This can be done easily by iterating
through the undo buffer of the transaction, copying the before-image into the latest version (undoing the change),
and unlinking the records atomically from the version chain.

## Garbage Collection

One important observation with the new system, is that all of the data to clean up resides within the transaction
context themselves. This elegantly eliminates the need for full table vacuums or collaborative garbage collection,
as information needed by GC is no longer associated with the tables themselves.

The transaction manager essentially keeps a back queue of all the transactions that are finished. The garbage
collector is then a simple background thread that periodically polls from the transaction manager's queue and
processes the finished transactions.

Two passes are required to fully deallocate a transaction context. On the first pass, the garbage collector will
look at the oldest transaction in the system that is still running, identify committed transactions whose undo
records are no longer visible to the running transactions (that is, no transaction in the system started before
the transaction committed), and unlink them from the version chain. However, running transactions can still be
traversing the version chain and landing on the node, so we cannot safely deallocate or reuse this piece of memory.
We refer to this as the unlink phase.

To deal with this, we note down the time we unlink the records, and leave the records be for now. When the GC
thread wakes up again, it first checks the current running transactions and deallocates these transactions if
the oldest running transactions are more recent than the unlink time (so they cannot be working on any of the
unlinked versions). We refer to this as the deallocation phase.

Two caveats of note:
- An aborted transaction essentially goes through the unlink phase as soon as the abort takes place, and goes
  straight to the deallocation phase in the garbage collector.
- The deallocation is not very aggressive. Throughout the code we have made decisions to back off and come back later
  when deallocating a record may result in races that needs complex resolution, to ensure correctness. This keeps up well
  enough in the absence of long running transactions; however, in an HTAP environment, long running transactions can be
  detrimental to our garbage collector. Luckily there are known optimization for this problem.
  Read [this paper](https://dl.acm.org/purchase.cfm?id=2903734) for more detail.

## Write-Ahead Logging and Recovery

Write-ahead logging in the new system operates very similarly to the garbage collection process. (In fact, there is a clear
symmetry between the two) When an update is performed, the delta record is really separated into two images: the before-image
and the after-image. Similar to how we store the undo records (before-images) in a transaction context, we can do the same to
redo records (after-images).

Because we intend for our database to be entirely in memory, there is no need to keep track of "dirty pages", like on a disk-based
system. All of the changes persisted would be through checkpoints, and it is not very hard to ensure the checkpoint is consistent
in a multi-version system. (It is essentially a transactional full-table scan.) This means that there never is anything to undo
when we recover from crash, and that it suffices to simply load the checkpoint and replay all the committed operations in the log
after the checkpoint.

Thus we only write out redo records, or after images, in the write ahead log. When updating, the transaction context uses the same
local mechanism for undo records to store a redo record. The difference is that transactions do not need to hold on to all its redo
records because it essentially never reads it after the changes have been applied. Thus, instead of maintaining a queue of transactions,
like the garbage collector, the log manager maintains a queue of redo record buffers to flush. A transaction would fill its local
buffer, and add it to the queue before requesting a fresh one to use for later updates. On commit, the transaction manager would insert
one last record, the commit record, into the buffer and manually add the buffer to the log manager's queue.

The log manager is then free to process the queue at its own pace. The only catch is that because the database is not allowed to report
that a transaction committed (report as in write a response back to the client. We are free to assume that the transaction committed in the
system itself), the log manager will take in a callback function with every committing transaction, which it promises to invoke when the
commit record is persistent on disk. Records from a single transaction and dependent commit records from different transactions are guaranteed
to appear in the record buffer in order, in this setup, so we don't bother with numbering the records. The log manager essentially wakes up
periodically to go through the queue and serialize the records from their in-memory format to their on-disk format (currently the same, but will
most likely change in the future), calling the registered callback whenever a commit record is being flushed. The callbacks are embedded C-function
pointers in the commit record for performance reasons.