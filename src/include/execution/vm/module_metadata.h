#pragma once

#include <string>
#include <unordered_map>
#include <utility>

namespace noisepage::execution::vm {

/** Compile-time information about a module. */
class CompileTimeModuleMetadata {
 public:
  /** Set the TPL in this module. */
  void SetTPL(std::string &&tpl) { map_["tpl"] = std::move(tpl); }
  /** Set the TBC in this module. */
  void SetTBC(std::string &&tbc) { map_["tbc"] = std::move(tbc); }

  /** @return The TPL in the module, if it exists. Otherwise empty string. */
  [[nodiscard]] std::string GetTPL() const { return map_.find("tpl") == map_.end() ? "" : map_.at("tpl"); }
  /** @return The TBC in the module, if it exists. Otherwise empty string. */
  [[nodiscard]] std::string GetTBC() const { return map_.find("tbc") == map_.end() ? "" : map_.at("tbc"); }

 private:
  // TODO(WAN): The value-type may change in the future depending on what tagged dictionaries need.
  std::unordered_map<std::string, std::string> map_;  ///< Compile-time information representable as strings.
};

/** Non-essential metadata associated with a module. */
class ModuleMetadata {
 public:
  /** Constructor for no-information case. */
  ModuleMetadata() = default;

  /** Constructor. */
  explicit ModuleMetadata(CompileTimeModuleMetadata &&compile_time_metadata)
      : compile_time_metadata_(compile_time_metadata) {}

  /** @return The metadata collected at compile-time for this module. */
  [[nodiscard]] const CompileTimeModuleMetadata &GetCompileTimeMetadata() const { return compile_time_metadata_; }

 private:
  CompileTimeModuleMetadata compile_time_metadata_;  ///< Metadata that was gathered at compile-time.
};

}  // namespace noisepage::execution::vm
