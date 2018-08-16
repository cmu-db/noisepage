#include <string>
#include <vector>
#include "common/macros.h"
#include "common/performance_counter.h"
#include "nlohmann/json.hpp"

namespace terrier::common {

/**
 * A StatisticsRegistry which maintains various PerformanceCounters under a modular JSON map.
 *
 * Only a uintptr_t is stored for every performance counter, i.e. it is assumed that the pointer
 * to the counter will always remain valid unless told otherwise.
 *
 * Note that the methods in this class are heavyweight. Never call this code in a critical path.
 */
class StatisticsRegistry {
 private:
  /**
   * A hack so that we know when we should convert integers to pointers.
   */
  static constexpr const char *RESERVED_PC_PTR_STRING = "__PC_PTRS";
  /**
   * The root registry object. It stores strings and integers, where strings denote new modules and
   * integers are actually pointers to PerformanceCounter objects.
   */
  json *root_registry_ = new json;

  /**
   * Returns a pointer to the JSON object rooted at the specified module.
   * @param module_path path to destination module
   * @param create_if_missing if true, will create missing modules along the path
   * @return the module obtained by following the module path
   */
  json *FindModule(const std::vector<std::string> &module_path, bool create_if_missing) {
    json *node = root_registry_;
    for (const auto &mod : module_path) {
      PELOTON_ASSERT(mod != RESERVED_PC_PTR_STRING, "We reserve that name for pointers.");
      if (create_if_missing) {
        node->emplace(mod, "{}"_json);
      }
      node = &node->at(mod);
    }
    return node;
  }

  /**
   * Returns the true JSON representation of the current registry,
   * i.e. follows all the pointers to the performance counters and reads them as JSON.
   */
  json GetTrueJson(json *root) {
    json output = "{}"_json;

    for (const auto &item : root->items()) {
      const auto &key = item.key();

      if (key == RESERVED_PC_PTR_STRING) {
        auto val = root->at(key).get<uintptr_t>();
        output = reinterpret_cast<PerformanceCounter *>(val)->ToJson();
      } else {
        auto val = root->at(key).get<json>();
        output[key] = GetTrueJson(&val);
      }
    }

    return output;
  }

 public:
  /**
   * Registers the given performance counter pc at the module specified.
   * If the specified module contains a binding which conflicts with pc's name,
   * pc's name will be modified until the binding succeeds.
   * @param module_path path to destination module
   * @param pc performance counter to be registered
   */
  void Register(const std::vector<std::string> &module_path, PerformanceCounter *pc) {
    json *mod = FindModule(module_path, true);

    // generate a new insertion name
    std::string insert_name = pc->GetName();
    uint32_t index = 1;
    while (mod->find(insert_name) != mod->end()) {
      insert_name = pc->GetName();
      insert_name.append(std::to_string(index++));
    }
    pc->SetName(insert_name);

    // create if doesn't exist
    mod->emplace(insert_name, "{}"_json);
    mod->at(insert_name)[RESERVED_PC_PTR_STRING] = reinterpret_cast<uintptr_t>(pc);
  }

  /**
   * Deregister and remove the named performance counter from the module specified.
   * @param module_path path to destination module
   * @param name name of the performance counter to be deregistered
   * @return true if the performance counter was successfully removed, false otherwise
   */
  bool Deregister(const std::vector<std::string> &module_path, const std::string &name) {
    json *mod = FindModule(module_path, false);
    auto deleted = static_cast<bool>(mod->erase(name));

    // delete the enclosing parent if it was the last thing
    if (deleted && mod->empty() && !module_path.empty()) {
      std::vector<std::string> parent(module_path.begin(), module_path.end() - 1);
      std::string last = module_path.back();
      FindModule(parent, false)->erase(last);
    }

    return deleted;
  }

  /**
   * Return a pointer to the performance counter at the specified module.
   * @param module_path path to destination module
   * @param name name of the performance counter requested
   * @return pointer to said performance counter
   */
  PerformanceCounter *GetPerformanceCounter(const std::vector<std::string> &module_path, const std::string &name) {
    json *mod = FindModule(module_path, false);
    auto val = mod->at(name).get<uintptr_t>();
    return reinterpret_cast<PerformanceCounter *>(val);
  }

  /**
   * Return all the items registered at the specified module.
   * @param module_path path to destination module
   * @return names of all items at the specified module
   */
  std::vector<std::string> GetRegistryListing(const std::vector<std::string> &module_path) {
    json *mod = FindModule(module_path, false);

    // get the items
    std::vector<std::string> output;
    for (const auto &item : mod->items()) {
      output.emplace_back(item.key());
    }
    return output;
  }

  /**
   * Convenience method to return all the items registered at the root module.
   * @return names of all items at the root module
   */
  std::vector<std::string> GetRegistryListing() { return GetRegistryListing({}); }

  /**
   * Dumps out a formatted JSON string snapshot of the stats available at the specified module.
   * @param module_path path to destination module
   * @param num_indents number of indents to use
   * @return a formatted JSON string representing a snapshot of all the stats at the specified module
   */
  std::string DumpStats(const std::vector<std::string> &module_path, uint32_t num_indents) {
    json *mod = FindModule(module_path, false);
    return GetTrueJson(mod).dump(num_indents);
  }

  /**
   * Dumps out a formatted JSON string snapshot of the stats available from the root module.
   * @return a formatted JSON string representing a snapshot of all the stats at the root module
   */
  std::string DumpStats() { return DumpStats({}, 4); }
};

}  // namespace terrier::common
