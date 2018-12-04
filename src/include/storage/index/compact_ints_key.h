#include <boost/functional/hash.hpp>
#include <cstring>

#include "common/portable_endian.h"
#include "storage/storage_defs.h"
#include "type/type_id.h"

namespace terrier::storage::index {

// This is the maximum number of 8-byte slots that we will pack into a single
// CompactIntsKey template. You should not instantiate anything with more than this
#define INTSKEY_MAX_SLOTS 4

/*
 * CompactIntsKey - Compact representation of multifield integers
 *
 * This class is used for storing multiple integral fields into a compact
 * array representation. This class is largely used as a static object,
 * because special storage format is used to ensure a fast comparison
 * implementation.
 *
 * Integers are stored in a big-endian and sign-magnitude format. Big-endian
 * favors comparison since we could always start comparison using the first few
 * bytes. This gives the compiler opportunities to optimize comparison
 * using advanced techniques such as SIMD or loop unrolling.
 *
 * For details of how and why integers must be stored in a big-endian and
 * sign-magnitude format, please refer to adaptive radix tree's key format
 *
 * Note: CompactIntsKey should always be aligned to 64 bit boundaries; There
 * are static assertion to enforce this rule
 */
template <uint8_t KeySize>
class CompactIntsKey {
 public:
  // This is the actual byte size of the key
  static constexpr size_t key_size_byte = KeySize * 8UL;

 private:
  // This is the array we use for storing integers
  byte key_data[key_size_byte];

 private:
  /*
   * TwoBytesToBigEndian() - Change 2 bytes to big endian
   *
   * This function could be achieved using XCHG instruction; so do not write
   * your own
   *
   * i.e. MOV AX, WORD PTR [data]
   *      XCHG AH, AL
   */
  static uint16_t TwoBytesToBigEndian(const uint16_t data) { return htobe16(data); }

  /*
   * FourBytesToBigEndian() - Change 4 bytes to big endian format
   *
   * This function uses BSWAP instruction in one atomic step; do not write
   * your own
   *
   * i.e. MOV EAX, WORD PTR [data]
   *      BSWAP EAX
   */
  static uint32_t FourBytesToBigEndian(const uint32_t data) { return htobe32(data); }

  /*
   * EightBytesToBigEndian() - Change 8 bytes to big endian format
   *
   * This function uses BSWAP instruction
   */
  static uint64_t EightBytesToBigEndian(const uint64_t data) { return htobe64(data); }

  /*
   * TwoBytesToHostEndian() - Converts back two byte integer to host byte order
   */
  static uint16_t TwoBytesToHostEndian(const uint16_t data) { return be16toh(data); }

  /*
   * FourBytesToHostEndian() - Converts back four byte integer to host byte
   * order
   */
  static uint32_t FourBytesToHostEndian(const uint32_t data) { return be32toh(data); }

  /*
   * EightBytesToHostEndian() - Converts back eight byte integer to host byte
   * order
   */
  static uint64_t EightBytesToHostEndian(const uint64_t data) { return be64toh(data); }

  /*
   * ToBigEndian() - Overloaded version for all kinds of integral data types
   */

  static uint8_t ToBigEndian(const uint8_t data) { return data; }

  static uint8_t ToBigEndian(const int8_t data) { return static_cast<uint8_t>(data); }

  static uint16_t ToBigEndian(const uint16_t data) { return TwoBytesToBigEndian(data); }

  static uint16_t ToBigEndian(const int16_t data) { return TwoBytesToBigEndian(static_cast<uint16_t>(data)); }

  static uint32_t ToBigEndian(const uint32_t data) { return FourBytesToBigEndian(data); }

  static uint32_t ToBigEndian(const int32_t data) { return FourBytesToBigEndian(static_cast<uint32_t>(data)); }

  static uint64_t ToBigEndian(const uint64_t data) { return EightBytesToBigEndian(data); }

  static uint64_t ToBigEndian(const int64_t data) { return EightBytesToBigEndian(static_cast<uint64_t>(data)); }

  /*
   * ToHostEndian() - Converts big endian data to host format
   */

  static uint8_t ToHostEndian(const uint8_t data) { return data; }

  static uint8_t ToHostEndian(const int8_t data) { return static_cast<uint8_t>(data); }

  static uint16_t ToHostEndian(const uint16_t data) { return TwoBytesToHostEndian(data); }

  static uint16_t ToHostEndian(const int16_t data) { return TwoBytesToHostEndian(static_cast<uint16_t>(data)); }

  static uint32_t ToHostEndian(const uint32_t data) { return FourBytesToHostEndian(data); }

  static uint32_t ToHostEndian(const int32_t data) { return FourBytesToHostEndian(static_cast<uint32_t>(data)); }

  static uint64_t ToHostEndian(const uint64_t data) { return EightBytesToHostEndian(data); }

  static uint64_t ToHostEndian(const int64_t data) { return EightBytesToHostEndian(static_cast<uint64_t>(data)); }

  /*
   * SignFlip() - Flips the highest bit of a given integral type
   *
   * This flip is logical, i.e. it happens on the logical highest bit of an
   * integer. The actual position on the address space is related to endianess
   * Therefore this should happen first.
   *
   * It does not matter whether IntType is signed or unsigned because we do
   * not use the sign bit
   */
  template <typename IntType>
  static IntType SignFlip(IntType data) {
    // This sets 1 on the MSB of the corresponding type
    // NOTE: Must cast 0x1 to the correct type first
    // otherwise, 0x1 is treated as the signed int type, and after leftshifting
    // if it is extended to larger type then sign extension will be used
    IntType mask = static_cast<IntType>(0x1) << (sizeof(IntType) * 8UL - 1);

    return data ^ mask;
  }

 public:
  /*
   * Constructor
   */
  CompactIntsKey() {
    TERRIER_ASSERT(KeySize > 0 && KeySize <= INTSKEY_MAX_SLOTS, "Invalid key size.");
    ZeroOut();
  }

  /*
   * ZeroOut() - Sets all bits to zero
   */
  void ZeroOut() { TERRIER_MEMSET(key_data, 0x00, key_size_byte); }

  /*
   * GetRawData() - Returns the raw data array
   */
  const byte *GetRawData() const { return key_data; }

  /*
   * AddInteger() - Adds a new integer into the compact form
   *
   * Note that IntType must be of the following 8 types:
   *   int8_t; int16_t; int32_t; int64_t
   * Otherwise the result is undefined
   */
  template <typename IntType>
  void AddInteger(IntType data, size_t offset) {
    auto sign_flipped = SignFlip<IntType>(data);

    // This function always returns the unsigned type
    // so we must use automatic type inference
    auto big_endian = ToBigEndian(sign_flipped);

    // This will almost always be optimized into single move
    TERRIER_MEMCPY(key_data + offset, &big_endian, sizeof(IntType));
  }

  /*
   * AddUnsignedInteger() - Adds an unsigned integer of a certain type
   *
   * Only the following unsigned type should be used:
   *   uint8_t; uint16_t; uint32_t; uint64_t
   */
  template <typename IntType>
  void AddUnsignedInteger(IntType data, size_t offset) {
    // This function always returns the unsigned type
    // so we must use automatic type inference
    auto big_endian = ToBigEndian(data);

    // This will almost always be optimized into single move
    TERRIER_MEMCPY(key_data + offset, &big_endian, sizeof(IntType));
  }

  /*
   * GetInteger() - Extracts an integer from the given offset
   *
   * This function has the same limitation as stated for AddInteger()
   */
  template <typename IntType>
  IntType GetInteger(size_t offset) const {
    const auto *ptr = reinterpret_cast<const IntType *>(key_data + offset);

    // This always returns an unsigned number
    auto host_endian = ToHostEndian(*ptr);

    return SignFlip<IntType>(static_cast<IntType>(host_endian));
  }

  /*
   * GetUnsignedInteger() - Extracts an unsigned integer from the given offset
   *
   * The same constraint about IntType applies
   */
  template <typename IntType>
  IntType GetUnsignedInteger(size_t offset) {
    const IntType *ptr = reinterpret_cast<IntType *>(key_data + offset);
    auto host_endian = ToHostEndian(*ptr);
    return static_cast<IntType>(host_endian);
  }

  /*
   * Compare() - Compares two IntsType object of the same length
   *
   * This function has the same semantics as memcmp(). Negative result means
   * less than, positive result means greater than, and 0 means equal
   */
  static int Compare(const CompactIntsKey<KeySize> &a, const CompactIntsKey<KeySize> &b) {
    return std::memcmp(a.key_data, b.key_data, CompactIntsKey<KeySize>::key_size_byte);
  }

  /*
   * LessThan() - Returns true if first is less than the second
   */
  static bool LessThan(const CompactIntsKey<KeySize> &a, const CompactIntsKey<KeySize> &b) { return Compare(a, b) < 0; }

  /*
   * Equals() - Returns true if first is equivalent to the second
   */
  static bool Equals(const CompactIntsKey<KeySize> &a, const CompactIntsKey<KeySize> &b) { return Compare(a, b) == 0; }

 public:
  /**
   * Prints the content of this key.
   *
   * IMPORTANT: This class should <b>not</b> override Printable
   * because that adds an extra 8 bytes and it makes all the math
   * above for doing fast comparisons fail.
   *
   * @return
   */
  //  const std::string GetInfo() const {
  //    std::ostringstream os;
  //    os << "CompactIntsKey<" << KeySize << "> - " << key_size_byte << " bytes" << std::endl;
  //
  //    // This is the current offset we are on printing the key
  //    size_t offset = 0;
  //    while (offset < key_size_byte) {
  //      constexpr int byte_per_line = 16;
  //      os << StringUtil::Format("0x%.8X    ", offset);
  //
  //      for (int i = 0; i < byte_per_line; i++) {
  //        if (offset >= key_size_byte) {
  //          break;
  //        }
  //        os << StringUtil::Format("%.2X ", key_data[offset]);
  //        // Add a delimiter on the 8th byte
  //        if (i == 7) {
  //          os << "   ";
  //        }
  //        offset++;
  //      }  // FOR
  //      os << std::endl;
  //    }  // WHILE
  //
  //    return (os.str());
  //  }

 private:
  /*
   * SetFromColumn() - Sets the value of a column into a given offset of
   *                   this ints key
   *
   * This function returns a size_t which is the next starting offset.
   *
   * Note: Two column IDs are needed - one into the key schema which is used
   * to determine the type of the column; another into the tuple to
   * get data
   */
  //  size_t SetFromColumn(oid_t key_column_id, oid_t tuple_column_id, const catalog::Schema *key_schema,
  //                              const storage::Tuple *tuple, size_t offset) {
  //    // We act depending on the length of integer types
  //    type::TypeId column_type = key_schema->GetColumn(key_column_id).GetType();
  //
  //    switch (column_type) {
  //      case type::TypeId::BIGINT: {
  //        int64_t data = tuple->GetInlinedDataOfType<int64_t>(tuple_column_id);
  //
  //        AddInteger<int64_t>(data, offset);
  //        offset += sizeof(data);
  //
  //        break;
  //      }
  //      case type::TypeId::INTEGER: {
  //        int32_t data = tuple->GetInlinedDataOfType<int32_t>(tuple_column_id);
  //
  //        AddInteger<int32_t>(data, offset);
  //        offset += sizeof(data);
  //
  //        break;
  //      }
  //      case type::TypeId::SMALLINT: {
  //        int16_t data = tuple->GetInlinedDataOfType<int16_t>(tuple_column_id);
  //
  //        AddInteger<int16_t>(data, offset);
  //        offset += sizeof(data);
  //
  //        break;
  //      }
  //      case type::TypeId::TINYINT: {
  //        int8_t data = tuple->GetInlinedDataOfType<int8_t>(tuple_column_id);
  //
  //        AddInteger<int8_t>(data, offset);
  //        offset += sizeof(data);
  //
  //        break;
  //      }
  //      default: {
  //        throw IndexException(
  //            "We currently only support a specific set of "
  //            "column index sizes...");
  //        break;
  //      }  // default
  //    }    // switch
  //
  //    return offset;
  //  }

  // The next are functions specific to Peloton
 public:
  /*
   * SetFromKey() - Sets the compact internal storage from a tuple
   *                only comtaining key columns
   *
   * Since we assume this tuple only contains key columns and there is no
   * other column, it is not necessary to specify a vector of object IDs
   * to indicate index column
   */
  //  void SetFromKey(const storage::Tuple *tuple) {
  //    PELOTON_ASSERT(tuple != nullptr);
  //    PELOTON_ASSERT(tuple->GetSchema() != nullptr);
  //
  //    // Must clear previous result first
  //    ZeroOut();
  //
  //    // This returns schema of the tuple
  //    // Note that the schema must contain only integral type
  //    const catalog::Schema *key_schema = tuple->GetSchema();
  //
  //    // Need this to loop through columns
  //    oid_t column_count = key_schema->GetColumnCount();
  //
  //    // Use this to arrange bytes into the key
  //    size_t offset = 0;
  //
  //    // **************************************************************
  //    // NOTE: Avoid using tuple->GetValue()
  //    // Because here what we need is:
  //    //   (1) Type of the column;
  //    //   (2) Integer value
  //    // The former could be obtained in the schema, and the last is directly
  //    // available from the inlined tuple data
  //    // **************************************************************
  //
  //    // Loop from most significant column to least significant column
  //    for (oid_t column_id = 0; column_id < column_count; column_id++) {
  //      offset = SetFromColumn(column_id, column_id, key_schema, tuple, offset);
  //
  //      // We could either have it just after the array or inside the array
  //      PELOTON_ASSERT(offset <= key_size_byte);
  //    }
  //
  //    return;
  //  }
  //
  //  /*
  //   * SetFromTuple() - Sets an integer key from a tuple which contains a super
  //   *                  set of columns
  //   *
  //   * We need an extra parameter telling us the subset of columns we would like
  //   * include into the key.
  //   *
  //   * Argument "indices" maps the key column in the corresponding index to a
  //   * column in the given tuple
  //   */
  //  void SetFromTuple(const storage::Tuple *tuple, const int *indices, const catalog::Schema *key_schema) {
  //    PELOTON_ASSERT(tuple != nullptr);
  //    PELOTON_ASSERT(indices != nullptr);
  //    PELOTON_ASSERT(key_schema != nullptr);
  //
  //    ZeroOut();
  //
  //    oid_t column_count = key_schema->GetColumnCount();
  //    size_t offset = 0;
  //
  //    for (oid_t key_column_id = 0; key_column_id < column_count; key_column_id++) {
  //      // indices array maps key column to tuple column
  //      // and it must have the same length as key schema
  //      oid_t tuple_column_id = indices[key_column_id];
  //
  //      offset = SetFromColumn(key_column_id, tuple_column_id, key_schema, tuple, offset);
  //      PELOTON_ASSERT(offset <= key_size_byte);
  //    }
  //
  //    return;
  //  }

  /*
   * GetTupleForComparison() - Returns a tuple object for comparing function
   *
   * Given a schema we extract all fields from the compact integer key
   */
  //  const storage::Tuple GetTupleForComparison(const catalog::Schema *key_schema) const {
  //    PELOTON_ASSERT(key_schema != nullptr);
  //
  //    size_t offset = 0;
  //    // Yes the tuple has an allocated chunk of memory and is not just a wrapper
  //    storage::Tuple tuple(key_schema, true);
  //    oid_t column_count = key_schema->GetColumnCount();
  //
  //    for (oid_t column_id = 0; column_id < column_count; column_id++) {
  //      type::TypeId column_type = key_schema->GetColumn(column_id).GetType();
  //
  //      switch (column_type) {
  //        case type::TypeId::BIGINT: {
  //          int64_t data = GetInteger<int64_t>(offset);
  //
  //          tuple.SetValue(column_id, type::ValueFactory::GetBigIntValue(data));
  //
  //          offset += sizeof(data);
  //
  //          break;
  //        }
  //        case type::TypeId::INTEGER: {
  //          int32_t data = GetInteger<int32_t>(offset);
  //
  //          tuple.SetValue(column_id, type::ValueFactory::GetIntegerValue(data));
  //
  //          offset += sizeof(data);
  //
  //          break;
  //        }
  //        case type::TypeId::SMALLINT: {
  //          int16_t data = GetInteger<int16_t>(offset);
  //
  //          tuple.SetValue(column_id, type::ValueFactory::GetSmallIntValue(data));
  //
  //          offset += sizeof(data);
  //
  //          break;
  //        }
  //        case type::TypeId::TINYINT: {
  //          int8_t data = GetInteger<int8_t>(offset);
  //
  //          tuple.SetValue(column_id, type::ValueFactory::GetTinyIntValue(data));
  //
  //          offset += sizeof(data);
  //
  //          break;
  //        }
  //        default: {
  //          throw IndexException(
  //              "We currently only support a specific set of "
  //              "column index sizes...");
  //          break;
  //        }
  //      }  // switch
  //    }    // for
  //
  //    return tuple;
  //  }
};

/*
 * class CompactIntsComparator - Compares two compact integer key
 */
template <size_t KeySize>
class CompactIntsComparator {
 public:
  CompactIntsComparator() { TERRIER_ASSERT(KeySize <= INTSKEY_MAX_SLOTS, "Instantiating with too many slots."); }
  CompactIntsComparator(const CompactIntsComparator &) = default;

  /*
   * operator()() - Returns true if lhs < rhs
   */
  bool operator()(const CompactIntsKey<KeySize> &lhs, const CompactIntsKey<KeySize> &rhs) const {
    return CompactIntsKey<KeySize>::LessThan(lhs, rhs);
  }
};

/*
 * class CompactIntsEqualityChecker - Compares whether two integer keys are
 *                                    equivalent
 */
template <size_t KeySize>
class CompactIntsEqualityChecker {
 public:
  CompactIntsEqualityChecker() { TERRIER_ASSERT(KeySize <= INTSKEY_MAX_SLOTS, "Instantiating with too many slots."); }
  CompactIntsEqualityChecker(const CompactIntsEqualityChecker &) = default;

  bool operator()(const CompactIntsKey<KeySize> &lhs, const CompactIntsKey<KeySize> &rhs) const {
    return CompactIntsKey<KeySize>::Equals(lhs, rhs);
  }
};

/*
 * class CompactIntsHasher - Hash function for integer key
 *
 * This function assumes the length of the integer key is always multiples
 * of 64 bits (8 byte word).
 */
template <size_t KeySize>
class CompactIntsHasher {
 public:
  // Emphasize here that we want a 8 byte aligned object
  static_assert(sizeof(CompactIntsKey<KeySize>) % sizeof(uint64_t) == 0,
                "Please align the size of compact integer key");

  // Make sure there is no other field
  static_assert(sizeof(CompactIntsKey<KeySize>) == CompactIntsKey<KeySize>::key_size_byte,
                "Extra fields detected in class CompactIntsKey");

  CompactIntsHasher() { TERRIER_ASSERT(KeySize <= INTSKEY_MAX_SLOTS, "Instantiating with too many slots."); }
  CompactIntsHasher(const CompactIntsHasher &) = default;

  /*
   * operator()() - Hashes an object into size_t
   *
   * This function hashes integer key using 64 bit chunks. Chunks are
   * accumulated to the hash one by one. Since
   */
  size_t operator()(const CompactIntsKey<KeySize> &p) const {
    size_t seed = 0UL;
    const auto *const ptr = reinterpret_cast<const size_t *const>(p.GetRawData());

    // For every 8 byte word just combine it with the current seed
    for (size_t i = 0; i < (CompactIntsKey<KeySize>::key_size_byte / sizeof(uint64_t)); i++) {
      boost::hash_combine(seed, ptr[i]);
    }

    return seed;
  }
};

}  // namespace terrier::storage::index
