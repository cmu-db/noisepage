#pragma once

#if 0
#include <charconv>  // TODO(WAN): charconv missing on Ubuntu 18.04
#include <memory>
#include <string>
#include <vector>

#include "common/constants.h"
#include "execution/util/file.h"

namespace noisepage::execution::util {

//===----------------------------------------------------------------------===//
//
// CSV Source
//
//===----------------------------------------------------------------------===//

/**
 * Base class interface for anything producing CSV data.
 *
 * IMPORTANT: All sources must provide CSVSource::NUM_EXTRA_PADDING_CHARS characters of tail
 *            padding to ensure fast processing!
 */
class CSVSource {
 public:
  /** The number of extra padding characters. */
  static constexpr uint32_t NUM_EXTRA_PADDING_CHARS = 16;

  /**
   * Destructor.
   */
  virtual ~CSVSource() = default;

  /**
   * @return True if this source was correctly initialized.
   */
  virtual bool Initialize() = 0;

  /**
   * @return The current buffer of input.
   */
  virtual const char *GetBuffer() = 0;

  /**
   * @return The size of the current buffer of input.
   */
  virtual std::size_t GetSize() = 0;

  /**
   * Called by the consumer to indicate that @em n bytes of the buffer have been consumer.
   * @param n The number of bytes of the buffer that've been consumed.
   */
  virtual void Consume(std::size_t n) = 0;

  /**
   * A request by the consumer to fill the buffer with new data.
   * @return True if there is more data; false otherwise.
   */
  virtual bool Fill() = 0;
};

//===----------------------------------------------------------------------===//
//
// CSV File
//
//===----------------------------------------------------------------------===//

/**
 * CSV source implementation for files.
 */
class CSVFile : public CSVSource {
  constexpr static std::size_t DEFAULT_BUFFER_SIZE = 64 * common::Constants::KB;
  constexpr static std::size_t MAX_ALLOC_SIZE = 1 * common::Constants::GB;

 public:
  /**
   * Create an instance using a file at the given path.
   * @param path Accessible path to the CSV file.
   */
  explicit CSVFile(std::string_view path);

  /**
   * Prepare the file for reading.
   * @return True if the file is opened and ready to be read; false otherwise.
   */
  bool Initialize() override;

  /**
   * @return Access a buffer of input.
   */
  const char *GetBuffer() override { return &buffer_[read_pos_]; }

  /**
   * @return The number of readable bytes in the buffer.
   */
  std::size_t GetSize() override { return end_pos_ - read_pos_; }

  /**
   * Called by the consumer to indicate consumption of some buffer bytes.
   * @param n The number of bytes that've been consumed.
   */
  void Consume(std::size_t n) override;

  /**
   * A request by the consumer to read more data from the file. The data will be visible in the
   * file's buffer through CSVFile::GetBuffer() and the size is reflected in CSVFile::GetSize().
   * @return True if there is more data; false otherwise.
   */
  bool Fill() override;

 protected:
  /** The file to read. */
  util::File file_;
  /** The buffer for the raeder. */
  std::unique_ptr<char[]> buffer_;
  /** The current read position in the buffer. */
  std::size_t read_pos_;
  /** The current end position in the buffer. */
  std::size_t end_pos_;
  /** The size of the allocated buffer.*/
  std::size_t buffer_alloc_size_;
};

//===----------------------------------------------------------------------===//
//
// CSV String
//
//===----------------------------------------------------------------------===//

/**
 * Helper class that makes a std::string look like a CSV source for reaching.
 */
class CSVString : public CSVSource {
 public:
  //
  /**
   * Construct from a string. Note: we're making a copy! So, don't use anything large.
   * @param data The CSV data.
   */
  explicit CSVString(std::string data)
      : data_(data.append(NUM_EXTRA_PADDING_CHARS, static_cast<char>(0))),
        p_(data_.data()),
        pend_(data_.data() + data_.length() - NUM_EXTRA_PADDING_CHARS) {}

  /**
   * Construct from a C-string.
   * @param data The CSV data.
   */
  explicit CSVString(const char *data) : CSVString(std::string(data)) {}

  /**
   * @return True always.
   */
  bool Initialize() override { return true; }

  /**
   * @return The start of the current buffer.
   */
  const char *GetBuffer() override { return p_; }

  /**
   * @return The size of the buffer is the number of unread characters.
   */
  std::size_t GetSize() override { return pend_ - p_; }

  /**
   * Bump the pointer into the string by the size of the consumption.
   * @param n The number of bytes to bump.
   */
  void Consume(const std::size_t n) override { p_ += n; }

  /**
   * @return True as long as there is remaining data in the buffer.
   */
  bool Fill() override { return p_ < pend_; }

 private:
  const std::string data_;
  const char *p_;
  const char *pend_;
};

//===----------------------------------------------------------------------===//
//
// CSV Reader
//
//===----------------------------------------------------------------------===//

/**
 * A fast CSV file reader. After instantiation, callers must invoke CSVReader::Initialize() before
 * the reader can be safely used. General usage pattern:
 *
 * @code
 * auto reader = CSVReader(source, delim, quote, escape);
 * if (!reader.Initialize()) { ... ERROR ... }
 * while (reader.Advance()) {
 *   auto row = reader.GetRow();
 *   auto cell_val = row->cells[0].AsString();
 *   ...
 * }
 * @endcode
 *
 * In general, this parser is efficient and fast - it can parse CSV files at roughly ~2 GB/s. It
 * will not be your bottleneck.
 */
class CSVReader {
 public:
  /**
   * A cell in a row in the CSV.
   */
  struct CSVCell {
    /** Pointer to the cell's data. */
    const char *ptr_;
    /** Length of the data in bytes. */
    std::size_t len_;
    /** True if this cell contains escaped data. */
    bool escaped_;
    /** The escaping character. */
    char escape_char_;

    /**
     * @return True if the cell is empty; false otherwise.
     */
    bool IsEmpty() const noexcept { return len_ == 0; }

    /**
     * @return This cell's value converted into a 64-bit signed integer.
     */
    int64_t AsInteger() const {
      NOISEPAGE_ASSERT(!escaped_, "Integer data cannot contain be escaped");
      int64_t n = 0;
      std::from_chars(ptr_, ptr_ + len_, n);
      return n;
    }

    /**
     * @return This cell's value convert into a 64-bit floating point value.
     */
    double AsDouble() const;

    /**
     * @return This cell's value as a string.
     */
    std::string AsString() const {
      std::string result(ptr_, len_);
      if (escaped_) {
        std::size_t new_len = 0;
        for (std::size_t i = 0; i < len_; i++) {
          if (result[i] == escape_char_) {
            i++;
          }
          result[new_len++] = result[i];
        }
        result.resize(new_len);
      }
      return result;
    }
  };

  /**
   * A row in the CSV.
   */
  struct CSVRow {
    /** The cells/attributes in this row. */
    std::vector<CSVCell> cells_;
    /** The number of active cells. */
    uint32_t count_;
  };

  /**
   * This structure tracks various statistics while we scan the CSV
   */
  struct Stats {
    /** The number of times we requested the source to refill. */
    uint32_t num_fills_ = 0;
    /** The total bytes read. */
    uint32_t bytes_read_ = 0;
    /** The number of lines in the CSV. */
    uint32_t num_lines_ = 0;
  };

  /**
   * Constructor.
   * @param source The source providing CSV data.
   * @param delimiter The character that separates columns within a row.
   * @param quote The quoting character used to quote data (i.e., strings).
   * @param escape The character appearing before the quote character.
   */
  explicit CSVReader(std::unique_ptr<CSVSource> source, char delimiter = ',', char quote = '"', char escape = '"');

  /**
   * Initialize the scan.
   * @return True if success; false otherwise.
   */
  bool Initialize();

  /**
   * Advance to the next row.
   * @return True if there is another row; false otherwise.
   */
  bool Advance();

  /**
   * @return The current parsed row.
   */
  const CSVRow *GetRow() const { return &row_; }

  /**
   * @warning No bounds-checking is done on the provided index.
   * @return The parsed cell at the given index in the current row.
   */
  const CSVCell *GetRowCell(uint32_t idx) const {
    NOISEPAGE_ASSERT(idx < row_.count_, "Out-of-bounds access");
    return &row_.cells_[idx];
  }

  /**
   * @return Return statistics collected during parsing.
   */
  const Stats *GetStatistics() const { return &stats_; }

  /**
   * @return The current record number. This also represents the number of records that have been
   *         parsed and processed thus far.
   */
  uint64_t GetRecordNumber() const { return stats_.num_lines_; }

 private:
  // The result of an attempted parse
  enum class ParseResult { Ok, NeedMoreData, NeedMoreCells };

  // Read the next line from the CSV file
  ParseResult TryParse();

 private:
  // The source of CSV data.
  std::unique_ptr<CSVSource> source_;

  // [buf,buf_end] represents a valid contiguous range of bytes provided by the
  // CSV source. At any point in time, buf points to the start of a row. When
  // the pointers are equal, the source is requested to refill its buffer, after
  // which these pointers are reset.
  const char *buf_;
  const char *buf_end_;

  // The active current row.
  CSVRow row_;

  // The column delimiter, quote, and escape characters configured for this CSV.
  char delimiter_;
  char quote_char_;
  char escape_char_;

  // Structure capturing various statistics.
  Stats stats_;
};

}  // namespace noisepage::execution::util
#endif
