#pragma once

#include <atomic>
#include <string>
#include "common/json.h"

namespace terrier::common {

/**
 * An abstract PerformanceCounter that can be converted to and from JSON.
 * The actual counters will be class members defined by the MAKE_PERFORMANCE_COUNTER macro.
 * Accessing and updating counters is minimal cost as they are implemented as atomic int class members.
 */
class PerformanceCounter {
 public:
  virtual ~PerformanceCounter() = default;
  /**
   * Return the name of the performance counter.
   * Names may change
   */
  virtual std::string GetName() = 0;
  /**
   * Set the name of the performance counter.
   */
  virtual void SetName(const std::string &) = 0;
  /**
   * Return the current state of the performance counter in JSON format.
   * Note that the JSON object does not automatically update, i.e. result immediately stale.
   * @return JSON snapshot of the current state of the performance counter
   */
  virtual json ToJson() const = 0;
  /**
   * Restores the state of the performance counter to the JSON snapshot.
   * Undefined behavior occurs if the JSON snapshot and the performance counter differ in structure.
   */
  virtual void FromJson(const json &) = 0;
};
}  // namespace terrier::common

/*
 * Every helper macro needs to appear in both NDEBUG and DEBUG branches.
 */
#ifndef NDEBUG
/*
 * Performance counter helper macros.
 * These auxiliary macros do not add any new functionality.
 */

/**
 * This macro defines class members by wrapping MemberType in std::atomic and initializing it to 0.
 * MemberType should be an integral type and MemberName should be a valid variable name.
 *
 * We did not find use-cases for non-zero default values and therefore removed that functionality,
 * but extending this macro to support them is straightforward if need arises.
 */
#define PC_HELPER_DEFINE_MEMBERS(MemberType, MemberName) std::atomic<MemberType> MemberName{0};

/**
 * This macro defines a GetMemberName() function which returns the value of MemberName.
 * If performance counters are disabled, it always returns 0.
 */
#define PC_HELPER_DEFINE_GET(MemberType, MemberName) \
  MemberType Get##MemberName() { return MemberName.load(); }

/**
 * This macro defines a SetMemberName(MemberType x) function which sets the value of MemberName to x.
 * If performance counters are disabled, it should do nothing.
 */
#define PC_HELPER_DEFINE_SET(MemberType, MemberName) \
  void Set##MemberName(MemberType x) { return MemberName.store(x); }

/**
 * This macro defines an IncrementMemberName(MemberType x) function which increments the value of MemberName by x.
 * If performance counters are disabled, it should do nothing.
 */
#define PC_HELPER_DEFINE_INCREMENT(MemberType, MemberName) \
  void Increment##MemberName(MemberType x) { return MemberName.store(static_cast<MemberType>(MemberName.load() + x)); }

/**
 * This macro defines a DecrementMemberName(MemberType x) function which decrements the value of MemberName by x.
 * If performance counters are disabled, it should do nothing.
 */
#define PC_HELPER_DEFINE_DECREMENT(MemberType, MemberName) \
  void Decrement##MemberName(MemberType x) { return MemberName.store(static_cast<MemberType>(MemberName.load() - x)); }

/*
 * Performance counter functions.
 * These macros add new functionality.
 * Please maintain alphabetical A-Z order.
 */
/**
 * This macro loads ClassName.MemberName from the JSON object.
 *
 * Assumed in scope:
 *  json &j
 */
#define PC_FN_JSON_FROM(MemberType, MemberName) MemberName.store(j.at("Counters").at(#MemberName).get<MemberType>());

/**
 * This macro writes ClassName.MemberName into the JSON object.
 *
 * Assumed in scope:
 *  json output
 */
#define PC_FN_JSON_TO(MemberType, MemberName) output["Counters"][#MemberName] = MemberName.load();

/**
 * This macro zeroes out ClassName.MemberName.
 */
#define PC_FN_ZERO(MemberType, MemberName) MemberName.store(0);
#else
#define PC_HELPER_DEFINE_MEMBERS(MemberType, MemberName)
#define PC_HELPER_DEFINE_GET(MemberType, MemberName) \
  std::atomic<MemberType> Get##MemberName() { return 0; }
#define PC_HELPER_DEFINE_SET(MemberType, MemberName) \
  void Set##MemberName(MemberType x) {}
#define PC_HELPER_DEFINE_INCREMENT(MemberType, MemberName) \
  void Increment##MemberName(MemberType x) {}
#define PC_HELPER_DEFINE_DECREMENT(MemberType, MemberName) \
  void Decrement##MemberName(MemberType x) {}
#define PC_FN_JSON_FROM(MemberType, MemberName)
#define PC_FN_JSON_TO(MemberType, MemberName)
#define PC_FN_ZERO(MemberType, MemberName)
#endif  // NDEBUG

/*
 * PerformanceCounter implementation details.
 *
 * We rely on the fact that macros can call macros as arguments.
 * More concretely, given the following macro definitions:
 *
 * #define NETWORK_MEMBERS(f) \
 *    f(uint64_t, RequestsReceived) \
 *    f(uint32_t, ConnectionsOpened)
 *
 * #define MAKE_MEMBER(MemberType, MemberName) \
 *    std::atomic<MemberType> MemberName{0};
 *
 * The preprocessor will expand NETWORK_MEMBERS(MAKE_MEMBER) to
 *    std::atomic<uint64_t> RequestsReceived{0};
 *    std::atomic<uint32_t> ConnectionsOpened{0};
 *
 * Furthermore, you have access to the surrounding scope's variables,
 * though you have to be careful with any assumptions you make there.
 */

/*
 * This macro creates a PerformanceCounter in its current namespace as the class ClassName.
 *
 * The actual counters are defined by passing in MemberList, a macro of the form
 *      #define MEMBER_LIST(f) f(type1, name1) f(type2, name2) ... f(typeN, nameN)
 * Note that all of the types should be integral types. An example:
 *      #define NETWORK_MEMBERS(f) f(uint64_t, RequestsReceived) f(uint32_t, ConnectionsOpened)
 *
 * You can use the resulting PerformanceCounter as a class in its own right. For example,
 *      #define DEFINE_PERFORMANCE_CLASS(NetworkCounter, NETWORK_MEMBERS)
 * will make the following code valid:
 *      NetworkCounter nc;
 *      nc.GetRequestsReceived(); // returns the std::atomic<uint64_t>
 *
 * In general, every declared member XYZ has GetXYZ() defined
 * to access the underlying std::atomic.
 * We need a function call so that we can compile this out in release mode.
 *
 * Note that every class member is wrapped in std::atomic.
 */
#define DEFINE_PERFORMANCE_CLASS(ClassName, MemberList)                                        \
  class ClassName : public terrier::common::PerformanceCounter {                               \
   private:                                                                                    \
    std::string name = #ClassName;                                                             \
    MemberList(PC_HELPER_DEFINE_MEMBERS);                                                      \
                                                                                               \
   public:                                                                                     \
    MemberList(PC_HELPER_DEFINE_GET);                                                          \
    MemberList(PC_HELPER_DEFINE_SET);                                                          \
    MemberList(PC_HELPER_DEFINE_INCREMENT);                                                    \
    MemberList(PC_HELPER_DEFINE_DECREMENT);                                                    \
                                                                                               \
    std::string GetName() override { return name; }                                            \
    void SetName(const std::string &name) override { this->name = name; }                      \
                                                                                               \
    nlohmann::json ToJson() const override {                                                   \
      nlohmann::json output;                                                                   \
      output["CounterName"] = #ClassName;                                                      \
      MemberList(PC_FN_JSON_TO);                                                               \
      return output;                                                                           \
    };                                                                                         \
    void FromJson(const nlohmann::json &j) override { MemberList(PC_FN_JSON_FROM); };          \
                                                                                               \
    void ZeroCounters() { MemberList(PC_FN_ZERO) }                                             \
  };                                                                                           \
                                                                                               \
  inline void to_json(nlohmann::json &j, const ClassName &c) { j = c.ToJson(); }  /* NOLINT */ \
  inline void from_json(const nlohmann::json &j, ClassName &c) { c.FromJson(j); } /* NOLINT */

// note: this is a rare correct usage of inline in modern C++
// If you include a header file in multiple translation units, the multiple definitions conflict
// and you get linker errors. We mark it inline so that the compiler figures it out.
