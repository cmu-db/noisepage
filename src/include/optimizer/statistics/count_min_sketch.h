#pragma once

#include <algorithm>
#include <cerrno>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "common/error/exception.h"
#include "common/json.h"
#include "common/macros.h"
#include "common/posix_io_wrappers.h"
#include "execution/sql/value.h"
#include "loggers/optimizer_logger.h"
#include "madoka/madoka.h"

namespace noisepage::optimizer {

/**
 * An approximate counting data structure.
 * Think of this like a Bloom filter but instead of determining whether
 * a key exists in a set or not, the CountMinSketch estimates
 * the count for the given key.
 * @tparam KeyType the data type of the entries we will store
 */
template <typename KeyType>
class CountMinSketch {
  using NormalizedKeyType = double;

 public:
  /**
   * Constructor with specific sketch size.
   * The larger the width then the more accurate the count estimates are,
   * but this will make the sketch's memory size larger.
   * @param width the number of 'slots' in each bucket level in this sketch.
   */
  explicit CountMinSketch(uint64_t width) : total_count_{0} {
    NOISEPAGE_ASSERT(width > 0, "Invalid width");

    // The only thing that we need to set in madoka when we initialize the
    // sketch is its the width. You can set the seed value but the documentation
    // says that you don't really need to do that.
    // https://www.s-yata.jp/madoka/doc/cpp-api.html
    sketch_.create(width);
  }

  /**
   * Move constructor
   * @param other sketch to move into this
   */
  CountMinSketch(CountMinSketch &&other) noexcept : total_count_(std::move(other.total_count_)) {
    sketch_.copy(other.sketch_);
  }

  /**
   * Move assignment operator
   * @param other sketch to move into this
   * @return this after moving
   */
  CountMinSketch &operator=(CountMinSketch &&other) noexcept {
    total_count_ = std::move(other.total_count_);
    sketch_.copy(other.sketch_);
    return *this;
  }

  /**
   * Increase the count for a key by a given amount.
   * The key does not need to exist in the sketch first.
   * @param key the key to increment the count for.
   * @param delta how much to increment the key's count.
   */
  void Increment(const KeyType &key, const uint32_t delta) {
    auto normalized_key = NormalizeKey(key);
    Increment(normalized_key, sizeof(normalized_key), delta);
  }

  /**
   * Decrease the count for a key by a given amount.
   * @param key the key to decrement the count for
   * @param delta how much to decrement the key's count.
   */
  void Decrement(const KeyType &key, const uint32_t delta) {
    auto normalized_key = NormalizeKey(key);
    Decrement(normalized_key, sizeof(normalized_key), delta);
  }

  /**
   * Remove the given key from the sketch. This attempts to set
   * the value of the key in the sketch to zero.
   * @param key
   */
  void Remove(const KeyType &key) {
    auto normalized_key = NormalizeKey(key);
    Remove(normalized_key, sizeof(normalized_key));
  }

  /**
   * Compute the approximate count for the given key.
   * @param key the key to get the count for.
   * @return the approximate count number for the key.
   */
  uint64_t EstimateItemCount(const KeyType &key) {
    auto normalized_key = NormalizeKey(key);
    return EstimateItemCount(normalized_key, sizeof(normalized_key));
  }

  /**
   * Merge Count Min Sketch with another Count Min Sketch
   * @param sketch sketch to merge with this
   */
  void Merge(const CountMinSketch &sketch) {
    total_count_ += sketch.GetTotalCount();
    sketch_.merge(sketch.sketch_);
  }

  /**
   * Clear the sketch object
   */
  void Clear() {
    total_count_ = 0;
    sketch_.clear();
  }

  /**
   * @return the number of 'slots' in each bucket level in this sketch.
   */
  uint64_t GetWidth() const { return sketch_.width(); }

  /**
   * @return the size of the sketch in bytes.
   */
  size_t GetSize() const { return sketch_.table_size(); }

  /**
   * @return the approximate total count of all keys in this sketch.
   */
  size_t GetTotalCount() const { return total_count_; }

  /**
   * Convert CountMinSketch to json
   * @return json representation of a CountMinSketch
   */
  nlohmann::json ToJson() const {
    nlohmann::json j;
    j["total_count"] = total_count_;
    size_t sketch_size;
    auto sketch_bin = SerializeMadokaSketch(&sketch_size);
    std::vector<byte> sketch_bin_vec(sketch_bin.get(), (sketch_bin.get() + sketch_size));
    j["sketch"] = sketch_bin_vec;
    j["width"] = sketch_.width();
    return j;
  }

  /**
   * Convert json to CountMinSketch
   * @param j json representation of a CountMinSketch
   * @return CountMinSketch object parsed from json
   */
  static CountMinSketch FromJson(const nlohmann::json &j) {
    auto width = j.at("width").get<uint64_t>();
    CountMinSketch sketch(width);
    sketch.total_count_ = j.at("total_count").get<size_t>();
    auto sketch_bin_vec = j.at("sketch").get<std::vector<byte>>();
    sketch.DeserializeMadokaSketch(sketch_bin_vec.data(), sketch_bin_vec.size());
    return sketch;
  }

  /**
   * Serialize CountMinSketch object into byte array
   * @param[out] size length of byte array
   * @return byte array representation of CountMinSketch
   */
  std::unique_ptr<byte[]> Serialize(size_t *size) const {
    const std::string json_str = ToJson().dump();
    *size = json_str.size();
    auto buffer = std::make_unique<byte[]>(*size);
    std::memcpy(buffer.get(), json_str.c_str(), *size);
    return buffer;
  }

  /**
   * Deserialize CountMinSketch object from byte array
   * @param buffer byte array representation of CountMinSketch
   * @param size length of byte array
   * @return Deserialized CountMinSketch object
   */
  static CountMinSketch Deserialize(const byte *buffer, size_t size) {
    std::string json_str(reinterpret_cast<const char *>(buffer), size);
    auto json = nlohmann::json::parse(json_str);
    return CountMinSketch::FromJson(json);
  }

 private:
  /**
   * Simple counter of the approximate number of entries we have stored.
   */
  size_t total_count_;

  /**
   * The underlying sketch implementation
   */
  madoka::Sketch sketch_;

  /**
   * Increase the count for a key by a given amount.
   * The key does not need to exist in the sketch first.
   * @param key the key to increment the count for.
   * @param key_size the length of the key's data.
   * @param delta how much to increment the key's count.
   */
  void Increment(const NormalizedKeyType &key, const size_t key_size, const uint32_t delta) {
    sketch_.add(reinterpret_cast<const void *>(&key), key_size, delta);
    total_count_ += delta;
  }

  /**
   * Decrease the count for a key by a given amount.
   * @param key the key to decrement the count for
   * @param key_size the length of the key's data.
   * @param delta how much to decrement the key's count.
   */
  void Decrement(const NormalizedKeyType &key, const size_t key_size, const uint32_t delta) {
    sketch_.add(reinterpret_cast<const void *>(&key), key_size, -1 * delta);

    // We have to check whether the delta is greater than the total count
    // to avoid wrap around.
    total_count_ = (delta <= total_count_ ? total_count_ - delta : 0UL);
  }

  /**
   * Remove the given key from the sketch. This attempts to set
   * the value of the key in the sketch to zero.
   * @param key
   * @param key_size
   */
  void Remove(const NormalizedKeyType &key, const size_t key_size) {
    auto delta = EstimateItemCount(key);
    sketch_.set(reinterpret_cast<const void *>(&key), key_size, 0);

    // The total count is going to be incorrect now because we don't
    // know whether the the original delta is accurate or not.
    // We have to check whether the delta is greater than the total count
    // to avoid wrap around.
    total_count_ = (delta <= total_count_ ? total_count_ - delta : 0UL);
  }

  /**
   * Compute the approximate count for the given key.
   * @param key the key to get the count for.
   * @param key_size the length of the key's data.
   * @return the approximate count number for the key.
   */
  uint64_t EstimateItemCount(const NormalizedKeyType &key, const size_t key_size) {
    return sketch_.get(reinterpret_cast<const void *>(&key), key_size);
  }

  /*
   * We need these methods to properly handle StringVals. madoka::Sketch casts the key to a byte array. The problem is
   * that decltype(execution::sql::StringVal::val_) is storage::VarlenEntry and storage::VarlenEntry has a pointer to a
   * byte array to store the string contents. So madoka::Sketch would end up using the address of this byte array as
   * part of the key instead of the actual contents. So two storage::VarlenEntry with the same underlying strings would
   * be treated as different because the addresses for their string contents are different. Converting all keys to
   * doubles gets around this issue.
   */
  static NormalizedKeyType NormalizeKey(const decltype(execution::sql::BoolVal::val_) &key) {
    return static_cast<NormalizedKeyType>(key);
  }

  static NormalizedKeyType NormalizeKey(const decltype(execution::sql::Integer::val_) &key) {
    return static_cast<NormalizedKeyType>(key);
  }

  static NormalizedKeyType NormalizeKey(const decltype(execution::sql::Real::val_) &key) {
    return static_cast<NormalizedKeyType>(key);
  }

  static NormalizedKeyType NormalizeKey(const decltype(execution::sql::DecimalVal::val_) &key) {
    return static_cast<NormalizedKeyType>(key);
  }

  static NormalizedKeyType NormalizeKey(const decltype(execution::sql::StringVal::val_) &key) { return key.Hash(); }

  static NormalizedKeyType NormalizeKey(const decltype(execution::sql::DateVal::val_) &key) {
    return static_cast<NormalizedKeyType>(key.ToNative());
  }

  static NormalizedKeyType NormalizeKey(const decltype(execution::sql::TimestampVal::val_) &key) {
    return static_cast<NormalizedKeyType>(key.ToNative());
  }

  static NormalizedKeyType NormalizeKey(const int &key) { return static_cast<NormalizedKeyType>(key); }

  static NormalizedKeyType NormalizeKey(const std::string &key) {
    return static_cast<NormalizedKeyType>(std::hash<std::string>()(key));
  }

  static NormalizedKeyType NormalizeKey(const char *key) { return NormalizeKey(std::string(key)); }

  /**
   * Serialize the madoka::Sketch into a byte array.
   *
   * This method is not thread safe for a single instance. You can call this method from multiple threads as long as
   * each call is on a separate instance. This is because we use the instance's address in memory to create a file name.
   * Currently there is no need to make this thread safe, but if you need to make it thread safe in the future here are
   * 2 possible implementations:
   *    1. Surround all the file logic with a mutex
   *    2. Create some atomic counter. Every call to this method will get and increment this counter. Then append the
   *    counter to the end of the file name.
   *
   * It's unfortunate that in order to serialize the madoka::Sketch object we must write it to a file and then read
   * in the file. madoka::Sketch provides no other way to directly serialize the object except through a file. I've
   * opened an issue on the library to see if they will add this feature. If they do then we should switch to using the
   * new feature. https://github.com/s-yata/madoka/issues/2
   *
   * @param[out] size length of byte array
   * @return byte array representation of madoka::Sketch
   */
  std::unique_ptr<byte[]> SerializeMadokaSketch(size_t *size) const {
    int fd;
    auto file_name = CreateAndOpenTempFile(&fd);
    sketch_.save(file_name.c_str(), madoka::FILE_TRUNCATE);
    *size = sketch_.file_size();
    auto buffer = std::make_unique<byte[]>(*size);
    storage::PosixIoWrappers::ReadFully(fd, buffer.get(), *size);
    CloseAndDeleteTempFile(fd, file_name);
    return buffer;
  }

  /**
   * Deserialize madoka::Sketch object from byte array.
   *
   * This method is not thread safe for a single instance. You can call this method from multiple threads as long as
   * each call is on a separate instance. This is because we use the instance's address in memory to create a file name.
   * Currently there is no need to make this thread safe, but if you need to make it thread safe in the future here are
   * 2 possible implementations:
   *    1. Surround all the file logic with a mutex
   *    2. Create some atomic counter. Every call to this method will get and increment this counter. Then append the
   *    counter to the end of the file name.
   *
   * It's unfortunate that in order to deserialize the madoka::Sketch object we must write it to a file and then read
   * in the file. madoka::Sketch provides no other way to directly deserialize the object except through a file. I've
   * opened an issue on the library to see if they will add this feature. If they do then we should switch to using the
   * new feature. https://github.com/s-yata/madoka/issues/2
   * @param buffer byte array representation of madoka::Sketch
   * @param size length of byte array
   */
  void DeserializeMadokaSketch(const byte *buffer, size_t size) {
    int fd;
    auto file_name = CreateAndOpenTempFile(&fd);
    storage::PosixIoWrappers::WriteFully(fd, buffer, size);
    sketch_.load(file_name.c_str());
    CloseAndDeleteTempFile(fd, file_name);
  }

  std::string CreateAndOpenTempFile(int *fd) const {
    try {
      auto temp_dir = std::filesystem::temp_directory_path();
      // We use this CountMinSketch's address in memory in the file name to garauntee that the file name is unique in
      // case multiple instances of CountMinSketch are being serialized at the same time
      std::string file_name =
          "noisepage_" + std::to_string(reinterpret_cast<const size_t>(reinterpret_cast<const void *>(this)));
      temp_dir.append(file_name);
      *fd = storage::PosixIoWrappers::Open(temp_dir.c_str(), O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
      return temp_dir.string();
    } catch (std::filesystem::filesystem_error &e) {
      throw OPTIMIZER_EXCEPTION(fmt::format("Failed to get temporary directory path: {}", e.what()));
    } catch (std::runtime_error &e) {
      throw OPTIMIZER_EXCEPTION(e.what());
    }
  }

  void CloseAndDeleteTempFile(int fd, const std::string &file_name) const {
    try {
      storage::PosixIoWrappers::Close(fd);
    } catch (std::runtime_error &e) {
      throw OPTIMIZER_EXCEPTION(e.what());
    }
    if (std::remove(file_name.c_str()) == -1) {
      throw OPTIMIZER_EXCEPTION(fmt::format("Failed to delete temporary file {} with errno {}", file_name, errno));
    }
  }
};

}  // namespace noisepage::optimizer
