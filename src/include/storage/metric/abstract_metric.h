#pragma once

#include <emmintrin.h>
#include <atomic>
#include <memory>
#include <string>
#include <utility>

#include "catalog/catalog_defs.h"
#include "storage/metric/abstract_raw_data.h"
#include "storage/metric/metric_defs.h"

namespace terrier {

namespace transaction {
class TransactionContext;
}  // namespace transaction

namespace storage::metric {
/**
 * @brief Interface representing a metric.
 * A metric is defined to be some piece of logic that processes events generated
 * by the database. @see StatsEventType for a list of available events.
 * It is guaranteed that the appropriate callback method (identified by the
 * naming pattern On[event name]) is invoked and the args filled out with
 * relevant information. To enable safe and efficient collection of data,
 * it is required that all data be collected be written to a RawData
 * object, @see AbstractRawData.
 *
 * While you could write your own metric directly extending from this class,
 * it is recommended that you use @see AbstractMetric class, which takes in
 * an AbstractRawData class as a template argument and implements the tricky
 * concurrent code for you.
 *
 * To write a new Metric, first write your own RawData class, extending from
 * AbstractRawData, and extend from AbstractMetric with your RawData class as
 * template argument. Then, override the event callbacks that you wish to know
 * about. @see AbstractMetric on how to deal with concurrency.
 */
class Metric {
 public:
  virtual ~Metric() = default;

  /**
   * @param txn context of the transaction beginning
   */
  virtual void OnTransactionBegin(const transaction::TransactionContext *txn) {}

  /**
   * @param txn context of the transaction committing
   * @param database_oid OID of the database where the txn happens.
   */
  virtual void OnTransactionCommit(const transaction::TransactionContext *txn, catalog::db_oid_t database_oid) {}

  /**
   * @param txn context of the transaction aborting
   * @param database_oid OID of the database where the txn happens.
   */
  virtual void OnTransactionAbort(const transaction::TransactionContext *txn, catalog::db_oid_t database_oid) {}

  /**
   * @param txn context of the transaction performing read
   * @param database_oid OID of the database that the tuple read happens
   * @param namespace_oid OID of the namespace that the tuple read happens
   * @param table_oid OID of the table that the tuple read happens
   */
  virtual void OnTupleRead(const transaction::TransactionContext *txn, catalog::db_oid_t database_oid,
                           catalog::namespace_oid_t namespace_oid, catalog::table_oid_t table_oid) {}

  /**
   * @param txn context of the transaction performing update
   * @param database_oid OID of the database that the tuple update happens
   * @param namespace_oid OID of the namespace that the tuple update happens
   * @param table_oid OID of the table that the tuple update happens
   */
  virtual void OnTupleUpdate(const transaction::TransactionContext *txn, catalog::db_oid_t database_oid,
                             catalog::namespace_oid_t namespace_oid, catalog::table_oid_t table_oid) {}

  /**
   * @param txn context of the transaction performing insert
   * @param database_oid OID of the database that the tuple insert happens
   * @param namespace_oid OID of the namespace that the tuple insert happens
   * @param table_oid OID of the table that the tuple insert happens
   */
  virtual void OnTupleInsert(const transaction::TransactionContext *txn, catalog::db_oid_t database_oid,
                             catalog::namespace_oid_t namespace_oid, catalog::table_oid_t table_oid) {}

  /**
   * @param txn Context of the transaction performing delete
   * @param database_oid OID of the database that the tuple delete happens
   * @param namespace_oid OID of the namespace that the tuple delete happens
   * @param table_oid OID of the table that the tuple delete happens
   */
  virtual void OnTupleDelete(const transaction::TransactionContext *txn, catalog::db_oid_t database_oid,
                             catalog::namespace_oid_t namespace_oid, catalog::table_oid_t table_oid) {}

  /**
   * @param database_oid OID of the database that the index read happens
   * @param namespace_oid OID of the namespace that the index read happens
   * @param index_oid OID of the index that the index read happens
   * @param freq number of read happening
   */
  virtual void OnIndexRead(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                           catalog::index_oid_t index_oid, size_t freq) {}

  /**
   * @param database_oid OID of the database that the index update happens
   * @param namespace_oid OID of the namespace that the index update happens
   * @param index_oid OID of the index that the index update happens
   */
  virtual void OnIndexUpdate(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                             catalog::index_oid_t index_oid) {}

  /**
   * @param database_oid OID of the database that the index insert happens
   * @param namespace_oid OID of the namespace that the index insert happens
   * @param index_oid OID of the index that the index insert happens
   */
  virtual void OnIndexInsert(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                             catalog::index_oid_t index_oid) {}

  /**
   * @param database_oid OID of the database that the index delete happens
   * @param namespace_oid OID of the namespace that the index delete happens
   * @param index_oid OID of the index that the index delete happens
   */
  virtual void OnIndexDelete(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                             catalog::index_oid_t index_oid) {}

  /**
   * @param database_oid OID of the database that the table memory allocation happens
   * @param namespace_oid OID of the namespace that the table memory allocation happens
   * @param table_oid OID of the table that the table memory allocation happens
   * @param size number of bytes being allocated
   */
  virtual void OnTableMemoryAlloc(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                                  catalog::table_oid_t table_oid, size_t size) {}

  /**
   * @param database_oid OID of the database that the index memory allocation happens
   * @param namespace_oid OID of the namespace that the index memory allocation happens
   * @param index_oid OID of the index that the index memory allocation happens
   * @param size number of bytes being allocated
   */
  virtual void OnIndexMemoryAlloc(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                                  catalog::index_oid_t index_oid, size_t size) {}

  /**
   * @param database_oid OID of the database that the table memory free happens
   * @param namespace_oid OID of the namespace that the table memory free happens
   * @param table_oid OID of the table that the table memory free happens
   * @param size number of bytes being freed
   */
  virtual void OnTableMemoryFree(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                                 catalog::table_oid_t table_oid, size_t size) {}

  /**
   * @param database_oid OID of the database that the index memory free happens
   * @param namespace_oid OID of the namespace that the index memory free happens
   * @param index_oid OID of the index that the index memory free happens
   * @param size number of bytes being freed
   */
  virtual void OnIndexMemoryFree(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                                 catalog::index_oid_t index_oid, size_t size) {}

  /**
   * @param database_oid OID of the database that the table memory usage happens
   * @param namespace_oid OID of the namespace that the table memory usage happens
   * @param table_oid OID of the table that the table memory usage happens
   * @param size number of bytes being used
   */
  virtual void OnTableMemoryUsage(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                                  catalog::table_oid_t table_oid, size_t size) {}

  /**
   * @param database_oid OID of the database that the index memory usage happens
   * @param namespace_oid OID of the namespace that the index memory usage happens
   * @param index_oid OID of the index that the index memory usage happens
   * @param size number of bytes being used
   */
  virtual void OnIndexMemoryUsage(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                                  catalog::index_oid_t index_oid, size_t size) {}

  /**
   * @param database_oid OID of the database that the table memory reclaim happens
   * @param namespace_oid OID of the namespace that the table memory reclaim happens
   * @param table_oid OID of the table that the table memory reclaim happens
   * @param size number of bytes being reclaim
   */
  virtual void OnTableMemoryReclaim(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                                    catalog::table_oid_t table_oid, size_t size) {}

  /**
   * @param database_oid OID of the database that the index memory reclaim happens
   * @param namespace_oid OID of the namespace that the index memory reclaim happens
   * @param index_oid OID of the index that the index memory reclaim happens
   * @param size number of bytes being reclaim
   */
  virtual void OnIndexMemoryReclaim(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                                    catalog::index_oid_t index_oid, size_t size) {}

  /**
   * @brief collect the signal of query begin
   */
  virtual void OnQueryBegin() {}

  /**
   * @brief collect the signal of query end
   */
  virtual void OnQueryEnd() {}

  /**
   * @brief event used to test the framework
   */
  virtual void OnTest(int increment) {}

  /**
   * @brief Replace RawData with an empty one and return the old one.
   *
   * Data from a metric is collected first into a thread-local storage to
   * ensure efficiency and safety, and periodically aggregated by an aggregator
   * thread into meaningful statistics. However, new statistics can still come
   * in when we aggregate, resulting in race conditions. To avoid this, every
   * time the aggregator wishes to aggregate data, the RawData object is
   * extracted and a fresh one swapped in, so collection continues seamlessly
   * while the aggregator is working.
   *
   * Unless you know what you are doing, you should probably just use the one
   * implemented for you(@see AbstractMetric). Otherwise, it is guaranteed that
   * this method is only called from the aggregator thread, so it is okay to
   * block in this method. As soon as this method returns, the aggregator
   * assumes that it is safe to start reading from the data and discards the
   * data after it's done. Therefore, it is essential that any implementation
   * ensures this method does not return if the collecting thread still can
   * write to the old raw data.
   *
   * @return a shared pointer to the old AbstractRawData
   */
  virtual std::shared_ptr<AbstractRawData> Swap() = 0;
};

/**
 * Forward Declaration
 */
template <typename DataType>
class AbstractMetric;

/**
 * @brief Wraps around a pointer to an AbstractRawData to allow safe access.
 *
 * This class is always handed out by an AbstractMetric and would prevent an
 * Aggregator from reading or deleting the AbstractRawData it holds. When the
 * object goes out of scope, its destructor will unblock the aggregator. Access
 * to its underlying pointer is always non-blocking.
 *
 * @tparam DataType the type of AbstractRawData this Wrapper holds
 */
template <typename DataType>
class RawDataWrapper {
  friend class AbstractMetric<DataType>;

 public:
  /**
   * Unblock aggregator
   */
  ~RawDataWrapper() { *safe_ = true; }

  /**
   * Don't allow RawDataWrapper to be copied, only allow change of ownership (move)
   */
  DISALLOW_COPY(RawDataWrapper);

  /**
   * @return the underlying pointer
   */
  DataType *operator->() const { return ptr_; }

 private:
  /**
   * Constructs a new Wrapper instance
   * @param ptr the pointer it wraps around
   * @param safe the boolean variable it uses to signal its lifetime
   */
  RawDataWrapper(DataType *ptr, std::atomic<bool> *safe) : ptr_(ptr), safe_(safe) {}
  DataType *ptr_;
  std::atomic<bool> *safe_;
};

/**
 * @brief General purpose implementation to Metric that you should inherit from.
 *
 * This class implements the tricky Swap method and exposes an interface for
 * children class. @see Metric for detail
 *
 * @tparam DataType the type of AbstractRawData this Metric holds
 */
template <typename DataType>
class AbstractMetric : public Metric {
 public:
  /**
   * Instantiate an abstract metric object with the templated data type
   */
  AbstractMetric() : raw_data_(new DataType()), safe_(std::atomic<bool>(true)) {}
  /**
   * De-allocate pointer to raw data
   */
  ~AbstractMetric() override { delete raw_data_.load(); }
  /**
   * @see Metric
   *
   * To ensure this method works as intended, be sure to use GetRawData() to
   * access the underlying raw data
   * @return a shared pointer to the old AbstractRawData
   */
  std::shared_ptr<AbstractRawData> Swap() override {
    // After this point, the collector thread can not see old data on new
    // events, but will still be able to write to it, if they loaded the
    // pointer before this operation but haven't written to it yet.
    DataType *old_data = raw_data_.exchange(new DataType());
    // We will need to wait for last writer to finish before it's safe
    // to start reading the content. It is okay to block since this
    // method should only be called from the aggregator thread.
    while (!safe_) _mm_pause();
    return std::shared_ptr<AbstractRawData>(old_data);
  }

 protected:
  /**
   * @see RawDataWrapper
   *
   * Always use this method to access the raw data within an AbstractMetric.
   * @return a RawDataWrapper object to access raw_data_
   */
  RawDataWrapper<DataType> GetRawData() {
    // safe_ should first be flipped to false before loading the raw_data_ so
    // that the aggregator would always be blocked when it tries to swap out if
    // there is a reader. At most one instance of this should be live at any
    // given time.
    safe_ = false;
    return {raw_data_.load(), &safe_};
  }

 private:
  /**
   * Pointer to raw data
   */
  std::atomic<DataType *> raw_data_;
  /**
   * Indicate whether it is safe to read the raw data, similar to a latch
   */
  std::atomic<bool> safe_;
};
}  // namespace storage::metric
}  // namespace terrier
