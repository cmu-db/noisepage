// #include "execution/util/csv_reader.h" Fix later.
#if 0
#include <immintrin.h>

#include <cstring>

#include "fast_float/fast_float.h"
#include "loggers/execution_logger.h"

namespace noisepage::execution::util {

//===----------------------------------------------------------------------===//
//
// CSV File Source
//
//===----------------------------------------------------------------------===//

CSVFile::CSVFile(std::string_view path)
    : file_(path, util::File::FLAG_OPEN | util::File::FLAG_READ),
      buffer_(std::unique_ptr<char[]>(new char[DEFAULT_BUFFER_SIZE + NUM_EXTRA_PADDING_CHARS])),
      read_pos_(0),
      end_pos_(0),
      buffer_alloc_size_(DEFAULT_BUFFER_SIZE) {}

bool CSVFile::Initialize() { return file_.IsOpen(); }

void CSVFile::Consume(const std::size_t n) {
  NOISEPAGE_ASSERT(read_pos_ + n <= end_pos_, "Buffer overflow!");
  read_pos_ += n;
}

bool CSVFile::Fill() {
  // If there are any left over bytes in the current buffer, copy it to the
  // front of the buffer before reading more data.

  if (const auto left_over_bytes = end_pos_ - read_pos_; left_over_bytes > 0) {
    std::memcpy(&buffer_[0], &buffer_[read_pos_], left_over_bytes);
    end_pos_ -= read_pos_;
    read_pos_ = 0;
  }

  // If there isn't room in the current buffer to read new data, it's a signal
  // that the consumer's working set is larger than what we're currently
  // delivering. We'll double the buffer size to accommodate, but up to a
  // maximum determined by kMaxAllocSize, usually 1GB.

  if (end_pos_ == buffer_alloc_size_) {
    buffer_alloc_size_ = std::min(buffer_alloc_size_ * 2, MAX_ALLOC_SIZE);
    auto new_buffer = std::unique_ptr<char[]>(new char[buffer_alloc_size_ + NUM_EXTRA_PADDING_CHARS]);
    std::memcpy(&new_buffer[0], &buffer_[0], end_pos_ - read_pos_);
    buffer_ = std::move(new_buffer);
  }

  // We have some room to read new data from the file. Do so now. If there's an
  // error, log it and terminate.

  const auto available = buffer_alloc_size_ - end_pos_;
  const auto bytes_read = file_.ReadFull(reinterpret_cast<std::byte *>(&buffer_[end_pos_]), available);

  if (bytes_read < 0) {
    EXECUTION_LOG_ERROR("Error reading from CSV: {}", util::File::ErrorToString(file_.GetErrorIndicator()));
    return false;
  }

  end_pos_ += bytes_read;

  return read_pos_ < end_pos_;
}

//===----------------------------------------------------------------------===//
//
// CSV Reader
//
//===----------------------------------------------------------------------===//

double CSVReader::CSVCell::AsDouble() const {
  double output = 0;
  fast_float::from_chars(this->ptr_, this->ptr_ + this->len_, output);
  return output;
}

CSVReader::CSVReader(std::unique_ptr<CSVSource> source, char delimiter, char quote, char escape)
    : source_(std::move(source)),
      buf_(nullptr),
      buf_end_(nullptr),
      delimiter_(delimiter),
      quote_char_(quote),
      escape_char_(escape) {
  // Assume 8 columns for now. We'll discover as we go along.
  row_.cells_.resize(8);
  for (CSVCell &cell : row_.cells_) {
    cell.ptr_ = nullptr;
    cell.len_ = 0;
    cell.escape_char_ = escape_char_;
  }
}

bool CSVReader::Initialize() { return source_->Initialize(); }

CSVReader::ParseResult CSVReader::TryParse() {
  const auto check_quoted = [&](const char *buf) noexcept {
    const auto special = _mm_setr_epi8(quote_char_, escape_char_, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    const auto data = _mm_loadu_si128(reinterpret_cast<const __m128i *>(buf));
    return _mm_cmpistri(special, data, _SIDD_CMP_EQUAL_ANY);
  };

  const auto check_unquoted = [&](const char *buf) noexcept {
    const auto special = _mm_setr_epi8(delimiter_, escape_char_, '\r', '\n', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    const auto data = _mm_loadu_si128(reinterpret_cast<const __m128i *>(buf));
    return _mm_cmpistri(special, data, _SIDD_CMP_EQUAL_ANY);
  };

  const auto is_new_line = [](const char c) noexcept { return c == '\r' || c == '\n'; };

  // The current cell
  CSVCell *cell = &row_.cells_[0];

  // The running pointer into the buffer
  const char *ptr = buf_;

#define RETURN_IF_AT_END()            \
  if (ptr >= buf_end_) {              \
    return ParseResult::NeedMoreData; \
  }

#define FINISH_CELL()      \
  cell->ptr_ = cell_start; \
  cell->len_ = ptr - cell_start;

#define FINISH_QUOTED_CELL() \
  cell->ptr_ = cell_start;   \
  cell->len_ = ptr - cell_start - 1;

#define NEXT_CELL()                          \
  cell++;                                    \
  if (++row_.count_ == row_.cells_.size()) { \
    return ParseResult::NeedMoreCells;       \
  }

cell_start:
  const char *cell_start = ptr;
  cell->escaped_ = false;

  // The first check we do is if we've reached the end of a line. This can be
  // caused by an empty last cell.

  RETURN_IF_AT_END();
  if (is_new_line(*ptr)) {
    FINISH_CELL();
    row_.count_++;
    buf_ = ptr + 1;
    return ParseResult::Ok;
  }

  // If the first character in a cell is the quote character, it's deemed a
  // "quoted cell". Quoted cells can contain more quote characters, escape
  // characters, new lines, or carriage returns - all of which have to be
  // escaped, of course. However, quoted fields must always end in a quoting
  // character followed by a delimiter character.

  if (*ptr == quote_char_) {
    cell_start = ++ptr;
  quoted_cell:
    while (true) {
      RETURN_IF_AT_END();
      int32_t ret = check_quoted(ptr);
      if (ret != 16) {
        ptr += ret + 1;
        break;
      }
      ptr += 16;
    }

    RETURN_IF_AT_END();
    if (*ptr == delimiter_) {
      FINISH_QUOTED_CELL();
      NEXT_CELL();
      ptr++;
      goto cell_start;
    }
    if (is_new_line(*ptr)) {
      FINISH_QUOTED_CELL();
      row_.count_++;
      buf_ = ptr + 1;
      return ParseResult::Ok;
    }
    cell->escaped_ = true;
    ptr++;
    goto quoted_cell;
  }

  // The first character isn't a quote, so this is a vanilla unquoted field. We
  // just need to find the next delimiter. However, these fields can also have
  // escape characters which we need to handle.

unquoted_cell:
  while (true) {
    RETURN_IF_AT_END();
    int32_t ret = check_unquoted(ptr);
    if (ret != 16) {
      ptr += ret;
      break;
    }
    ptr += 16;
  }

  RETURN_IF_AT_END();
  if (*ptr == delimiter_) {
    FINISH_CELL();
    NEXT_CELL();
    ptr++;
    goto cell_start;
  }
  if (is_new_line(*ptr)) {
    FINISH_CELL();
    row_.count_++;
    buf_ = ptr + 1;
    return ParseResult::Ok;
  }
  cell->escaped_ = true;
  ptr++;
  goto unquoted_cell;
}

bool CSVReader::Advance() {
  do {
    row_.count_ = 0;
    buf_ = source_->GetBuffer();
    buf_end_ = buf_ + source_->GetSize();
    const char *start_pos = buf_;
    switch (TryParse()) {
      case ParseResult::Ok:
        stats_.num_lines_++;
        stats_.bytes_read_ += buf_ - start_pos;
        source_->Consume(buf_ - start_pos);
        return true;
      case ParseResult::NeedMoreCells:
        row_.cells_.resize(row_.cells_.size() * 2);
        for (auto &cell : row_.cells_) cell.escape_char_ = escape_char_;
        return Advance();
      case ParseResult::NeedMoreData:
        break;
    }
    stats_.num_fills_++;
  } while (source_->Fill());

  // There's no more data in the buffer.
  return false;
}

}  // namespace noisepage::execution::util
#endif
