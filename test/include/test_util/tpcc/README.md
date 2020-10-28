# TPC-C microbenchmark

This folder contains the implementation for running a TPC-C workload on the backend of the DBMS. This exercises the
logging, garbage collection, concurrency control, and storage layers of the system.

There were several goals for this microbenchmark:
* Evaluate the correctness of the back-end of the system in a Google Test. There are `NOISEPAGE_ASSERT` expressions
throughout all stages of the TPC-C code to evaluate both the internal validity of the system and that the values
going in and out of the system match expected behavior according to the TPC-C spec.
* Evaluate the performance of the back-end of the system in a Google Benchmark (`NOISEPAGE_ASSERT` is compiled out in
Release mode as opposed to relying on Google's `EXPECT` or `ASSERT` macros, which would still be present in Release
mode.)
* Evaluate the APIs of the back-end of the system and address any shortcomings along the way. We found and addressed
issues in the indexes and SQLTable layers.

## Architecture

The benchmark is broken into several files that represent different aspects of the benchmark:

* **databaseh.h**: A `Database` owns the pointers to all of the tables, indexes, and their schemas. Think of this as a
replacement for a catalog.
* **schemas.h**: Defines the DDL for tables and indexes per the TPC-C spec. When correct and within spec, SQL types of
attributes have been minimized in size as much as possible (TINYINT instead of INTEGER, etc.).
* **builder.h.cpp**: Given a `BlockStore`, the `Builder` constructs a `Database` from all of the schemas defined in
schemas.h. It is responsible for translating the DDL of the tables and indexes into physical objects, and assigning
correct oids along the way.
* **worker.h**: Each thread needs buffers for executing its queries. A `Worker` object is assigned to each execution
thread, and contains reusable buffers it would need for these operations in order to avoid frequent allocations and
deletions.
* **loader.h**: Given a `Database`, `TransactionManager`, and `Worker` the static `Loader` class populates all of the
tables and indexes according to the TPC-C spec.
* **workload.h.cpp**: Defines the `TransactionArgs` struct that contains all of the input needed for TPC-C queries. The
`PrecomputeArgs()` function will return all of the inputs needed to run a TPC-C workload in order to avoid the runtime
overhead of generating these on the fly during benchmarking.
* **util.h**: Miscellaneous helper functions used for populating tables, generating the workload, and executing queries.
* **delivery.h/.cpp**, **new_order.h.cpp**, **order_status.h.cpp**, **payment.h.cpp**, **stock_level.h.cpp**: Each TPC-C
query has its own class and `Execute()` function.
This is done to mimic the behavior of code compilation and query caching. Each class has
fields for all of the oids and offsets it will need to touch, and the constructor is responsible for looking at all of
the maps once to populate these values. This is done to avoid performing lookups into the maps in every query execution.
Each query should be instantiated once for a given benchmark, and then `Execute()` should be called whenever the query
should be run. Re-instantiating these classes for each query would eliminate the benefit of caching all of the map
lookups. They are otherwise stateless and carry no information from one execution to the next, hence `Execute()` being
defined as `const`.

## Execution

Refer to **tpcc_test.cpp** and **tpcc_benchmark.cpp** for detailed instructions, and the dependencies should be clear
from function arguments and the descriptions above, but the the general way to run TPC-C is as follows:
1. Instantiate `BufferSegmentPool`, `BlockStore`, `LogManager`,`GarbageCollector`, and `TransactionManager`.
2. Instantiate `Builder`.
3. Construct TPC-C `Database` with `Builder::Build()`
4. Allocate a collection of `Worker` objects with one for each warehouse. This assumes 1:1 mapping between warehouse and
terminals.
5. Populate TPC-C `Database` with `Loader::PopulateDatabase()`.
6. Precompute in the TPC-C input arguments for however many execution threads and transactions you want to run.
7. Run the workload, most likely with a worker pool and timing the execution.
8. Deallocate any `VarlenEntry` attributes that are not inlined from the precomputed arguments.