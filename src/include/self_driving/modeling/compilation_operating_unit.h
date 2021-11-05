#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "execution/exec_defs.h"
#include "execution/vm/bytecode_module.h"

namespace noisepage::selfdriving {

/**
 * CompilationOperatingUnit is used to record a compilation of a single bytecode
 * module. A bytecode module can contain multiple functions. The features for
 * a given CompilationOperatingUnit contains the following metadata:
 * - Size of the code section
 * - Size of the data section
 * - Number of functions in the module
 * - Number of static locals
 */
class CompilationOperatingUnit {
 public:
  CompilationOperatingUnit() = default;

  /**
   * Constructor for CompilationOperatingUnit
   * @param code_size Size of the code section
   * @param data_size Size of the static data section
   * @param functions_size Number of functions
   * @param static_locals_size Number of static locals
   */
  CompilationOperatingUnit(size_t code_size, size_t data_size, size_t functions_size, size_t static_locals_size)
      : code_size_(code_size),
        data_size_(data_size),
        functions_size_(functions_size),
        static_locals_size_(static_locals_size) {}

  /**
   * Constructor for CompilationOperatingUnit from an existing one
   * @param other Existing CompilationOperatingUnit to copy from
   */
  CompilationOperatingUnit(const CompilationOperatingUnit &other) = default;

  /**
   * Constructor for CompilationOperatingUnit from a bytecode module
   * @param module bytecode module
   */
  explicit CompilationOperatingUnit(const execution::vm::BytecodeModule *module)
      : code_size_(module->GetInstructionCount()),
        data_size_(module->GetDataSize()),
        functions_size_(module->GetFunctionCount()),
        static_locals_size_(module->GetStaticLocalsCount()) {}

  /**
   * Returns a vector of doubles consisting of the features
   */
  void GetAllAttributes(std::vector<double> *all_attributes) const {
    all_attributes->push_back(code_size_);
    all_attributes->push_back(data_size_);
    all_attributes->push_back(functions_size_);
    all_attributes->push_back(static_locals_size_);
  }

  /** @return size of the code section */
  size_t GetCodeSize() const { return code_size_; }

  /** @return size of the data section */
  size_t GetDataSize() const { return data_size_; }

  /** @return number of the functions */
  size_t GetFunctionsSize() const { return functions_size_; }

  /** @return number of static locals */
  size_t GetStaticLocalsSize() const { return static_locals_size_; }

 private:
  size_t code_size_;
  size_t data_size_;
  size_t functions_size_;
  size_t static_locals_size_;
};

/**
 * CompilationOperatingUnits manages the storage/association of modules to the features
 * for a given query execution/compilation. This class is required since a query can
 * potentially have multiple modules.
 */
class CompilationOperatingUnits {
 public:
  /**
   * Constructor
   */
  CompilationOperatingUnits() = default;

  /** Adds a compilation module to the features being tracked */
  void RecordCompilationModule(const execution::vm::BytecodeModule *module) {
    auto ou = CompilationOperatingUnit(module);
    UNUSED_ATTRIBUTE auto res = units_.insert(std::make_pair(module->GetName(), ou));
    NOISEPAGE_ASSERT(res.second, "Recording duplicate module commpilation entry");
  }

  /** @return Gets the mapping from module name to CompilationOperatingUnit */
  const std::unordered_map<std::string, CompilationOperatingUnit> &GetCompilationUnits() const { return units_; }

  /** @return all tracked CompilationOperatingUnit as a vector */
  std::vector<CompilationOperatingUnit> GetCompilationUnitsVector() const {
    std::vector<CompilationOperatingUnit> units;
    for (auto &it : units_) {
      units.emplace_back(it.second);
    }

    return units;
  }

 private:
  std::unordered_map<std::string, CompilationOperatingUnit> units_{};
};

}  // namespace noisepage::selfdriving
