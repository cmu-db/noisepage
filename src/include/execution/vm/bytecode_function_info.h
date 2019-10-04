#pragma once

#include <cstdint>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "common/macros.h"
#include "execution/util/bitfield.h"
#include "execution/util/execution_common.h"

namespace terrier::execution {

namespace ast {
class FunctionType;
class Type;
}  // namespace ast

namespace vm {

/**
 * Function IDs are 16-bit numbers. These are used in encoded bytecode.
 */
using FunctionId = uint16_t;

/**
 * LocalInfo captures information about any local variable allocated in a
 * function, including genuine local variables in the source, function
 * parameters, and temporary variables required for expression evaluation.
 *
 * Locals have a fixed size, a static position (offset) in a function's
 * execution frame, and a fixed TPL type. A local variable's type also defines
 * its alignment in a function's execution frame. The TPL virtual machine
 * ensures that a local variable's memory has the correct alignment deemed by
 * its type and the machine architecture.
 */
class LocalInfo {
 public:
  /**
   * The kind of local variable
   */
  enum class Kind : uint8_t { Var, Parameter };

  /**
   * Construct a local with the given, name, type, offset and kind
   */
  LocalInfo(std::string name, ast::Type *type, uint32_t offset, Kind kind) noexcept;

  /**
   * Return true if this local variable a parameter to a function
   */
  bool IsParameter() const { return kind_ == Kind::Parameter; }

  /**
   * Return the name of this local variable
   */
  const std::string &Name() const { return name_; }

  /**
   * Return the TPL type of this local variable
   */
  ast::Type *GetType() const { return type_; }

  /**
   * Return the offset (in bytes) of this local in the function's stack frame
   */
  uint32_t Offset() const { return offset_; }

  /**
   * Return the size (in bytes) of this local variable
   */
  uint32_t Size() const { return size_; }

 private:
  // The name of the local
  std::string name_;
  // The TPL type of the local
  ast::Type *type_;
  // The offset (in bytes) of the local from the start of function's frame
  uint32_t offset_;
  // The size (in bytes) of the local
  uint32_t size_;
  // The kind of the local
  Kind kind_;
};

/**
 * Local access encapsulates how a given local will be accessed by pairing a
 * local ID and an addressing mode.
 */
class LocalVar {
  static const uint32_t K_INVALID_OFFSET = std::numeric_limits<uint32_t>::max() >> 1u;

 public:
  /**
   * The different local addressing modes
   */
  enum class AddressMode : uint8_t { Address = 0, Value = 1 };

  /**
   * An invalid local variable
   */
  LocalVar() : LocalVar(K_INVALID_OFFSET, AddressMode::Address) {}

  /**
   * A local variable with a given addressing mode
   * @param offset The byte-offset of the local variable in the function's
   *               execution/stack frame
   * @param address_mode The addressing mode for this variable
   */
  LocalVar(uint32_t offset, AddressMode address_mode)
      : bitfield_(AddressModeField::Encode(address_mode) | LocalOffsetField::Encode(offset)) {}

  /**
   * Return the addressing mode of for this local variable
   * @return The addressing mode (direct or indirect) of this local
   */
  AddressMode GetAddressMode() const { return AddressModeField::Decode(bitfield_); }

  /**
   * Return the offset of this local variable in the function's execution frame
   * @return The offset (in bytes) of this local in the function's frame
   */
  uint32_t GetOffset() const { return LocalOffsetField::Decode(bitfield_); }

  /**
   * Encode this local variable into an instruction stream
   * @return The encoded value (and its addressing mode)
   */
  uint32_t Encode() const { return bitfield_; }

  /**
   * Decode the provided value from an instruction stream into a local variable
   * that captures its offset and addressing more.
   * @param encoded_var The encoded value of the variable
   * @return The LocalVar representation
   */
  static LocalVar Decode(uint32_t encoded_var) { return LocalVar(encoded_var); }

  /**
   * Return a LocalVar that represents a dereferenced version of the local
   */
  LocalVar ValueOf() const { return LocalVar(GetOffset(), AddressMode::Value); }

  /**
   * Return a LocalVar that represents this address of this local
   */
  LocalVar AddressOf() const { return LocalVar(GetOffset(), AddressMode::Address); }

  /**
   * Is this a valid local variable?
   * @return True if valid; false otherwise
   */
  bool IsInvalid() const { return GetOffset() == K_INVALID_OFFSET; }

  /**
   * Is this local variable equal to @em other
   * @param other The variable to check against
   * @return True if equal; false otherwise
   */
  bool operator==(const LocalVar &other) const noexcept {
    return GetOffset() == other.GetOffset() && GetAddressMode() == other.GetAddressMode();
  }

 private:
  // Single bit indicating the addressing mode of the local
  class AddressModeField : public util::BitField32<AddressMode, 0, 1> {};

  // The offset of the local variable in the function's execution frame
  class LocalOffsetField : public util::BitField32<uint32_t, AddressModeField::K_NEXT_BIT, 31> {};

 private:
  explicit LocalVar(uint32_t bitfield) : bitfield_(bitfield) {}

 private:
  uint32_t bitfield_;
};

/**
 * FunctionInfo captures information about a TBC bytecode function. Bytecode
 * functions work slightly differently than C/C++ or even TPL functions: TBC
 * functions do not have a return value. Rather, callers allocate return value
 * and pass pointers to target functions to fill them in. This is true for both
 * primitive and complex/container types.
 *
 * TBC functions manage a set of locals. The first N locals are reserved for
 * the input (and output) parameters. The number of parameters tracks both
 * input and output arguments.
 */
class FunctionInfo {
 public:
  /**
   * Invalid id
   */
  static constexpr FunctionId K_INVALID_FUNC_ID = std::numeric_limits<uint16_t>::max();

  /**
   * Construct a function with the given ID and name @em name
   * @param id The ID of the function
   * @param name The name of the function in the module
   * @param func_type The TPL type of the function
   */
  FunctionInfo(FunctionId id, std::string name, ast::FunctionType *func_type);

  /**
   * Allocate a new function parameter
   * @param type The TPL type of the parameter
   * @param name The name of the parameter
   * @return A (logical) pointer to the input parameter
   */
  LocalVar NewParameterLocal(ast::Type *type, const std::string &name);

  /**
   * Allocate a new local variable with type @em type and name @em name. This
   * returns a LocalVar object with the Address addressing mode (i.e., a
   * pointer to the variable).
   * @param type The TPL type of the variable
   * @param name The name of the variable. If no name is given, the variable
   *             is assigned a synthesized one.
   * @return A (logical) pointer to the local variable
   */
  LocalVar NewLocal(ast::Type *type, const std::string &name = "");

  /**
   * Return the ID of the return value for the function
   */
  LocalVar GetReturnValueLocal() const;

  /**
   * Lookup a local variable by name
   * @param name The name of the local variable
   * @return A (logical) pointer to the local variable
   */
  LocalVar LookupLocal(const std::string &name) const;

  /**
   * Lookup the information for a local variable in this function by its name
   * @param name The name of the local variable to find
   * @return A possibly null pointer to the local's information
   */
  const LocalInfo *LookupLocalInfoByName(const std::string &name) const;

  /**
   * Lookup the information for a local variable in this function by the
   * variable's offset in the function's execution frame
   * @param offset The offset in bytes of the local
   * @return A possible nullptr to the local's information
   */
  const LocalInfo *LookupLocalInfoByOffset(uint32_t offset) const;

  /**
   * Return the unique ID of this function
   */
  FunctionId Id() const { return id_; }

  /**
   * Return the name of this function
   */
  const std::string &Name() const { return name_; }

  /**
   * Return the TPL function type
   */
  const ast::FunctionType *FuncType() const { return func_type_; }

  /**
   * Return the range of bytecode for this function in the bytecode module
   */
  std::pair<std::size_t, std::size_t> BytecodeRange() const { return bytecode_range_; }

  /**
   * Return a constant view of all the local variables in this function
   */
  const std::vector<LocalInfo> &Locals() const { return locals_; }

  /**
   * Return the size of the stack frame this function requires
   */
  std::size_t FrameSize() const { return frame_size_; }

  /**
   * Return the byte position where the first input argument exists in the
   * function's local execution frame
   */
  std::size_t ParamsStartPos() const { return params_start_pos_; }

  /**
   * Return the size (in bytes) of all input arguments, including alignment
   */
  std::size_t ParamsSize() const { return params_size_; }

  /**
   * Return the number of parameters to this function
   */
  uint32_t NumParams() const { return num_params_; }

 private:
  friend class BytecodeGenerator;

  // Mark the range of bytecode for this function in its module. This is set
  // by the BytecodeGenerator during code generation after this function's
  // bytecode range has been discovered.
  void SetBytecodeRange(std::size_t start_offset, std::size_t end_offset) {
    // Functions must have, at least, one bytecode instruction (i.e., RETURN)
    TERRIER_ASSERT(start_offset < end_offset, "Starting offset must be smaller than ending offset");
    bytecode_range_ = std::make_pair(start_offset, end_offset);
  }

  // Allocate a new local variable in the function
  LocalVar NewLocal(ast::Type *type, const std::string &name, LocalInfo::Kind kind);

 private:
  // The ID of the function in the module. IDs are unique within a module.
  FunctionId id_;
  // The name of the function
  std::string name_;
  // The TPL function type of this function
  ast::FunctionType *func_type_;
  // The range of bytecode for this function in the module's bytecode array
  std::pair<std::size_t, std::size_t> bytecode_range_;
  // List of all locals visible to this function
  std::vector<LocalInfo> locals_;
  // The size (in bytes) of this function's frame
  std::size_t frame_size_;
  // The start position within the frame where the first input argument is
  std::size_t params_start_pos_;
  // The size (in bytes) of all input arguments (including alignment)
  std::size_t params_size_;
  // The number of input parameters
  uint32_t num_params_;
  // The number of temporary variables
  uint32_t num_temps_;
};

}  // namespace vm
}  // namespace terrier::execution
