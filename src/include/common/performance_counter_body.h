#pragma once

#include <atomic>
#include <string>
#include "common/performance_counter.h"
#include "common/json.h"

/*
 * Every helper macro needs to appear in both NDEBUG and DEBUG branches.
 */
#ifndef NDEBUG
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
#define PC_FN_JSON_FROM(MemberType, MemberName)
#define PC_FN_JSON_TO(MemberType, MemberName)
#define PC_FN_ZERO(MemberType, MemberName)
#endif  // NDEBUG

#define DEFINE_PERFORMANCE_CLASS_BODY(ClassName, MemberList)                                              \
  nlohmann::json ClassName::ToJson() const {                                                              \
      nlohmann::json output;                                                                              \
      output["CounterName"] = #ClassName;                                                                 \
      MemberList(PC_FN_JSON_TO);                                                                          \
      return output;                                                                                      \
  }                                                                                                       \
  void ClassName::FromJson(const nlohmann::json &j) { MemberList(PC_FN_JSON_FROM); }                      \
                                                                                                          \
  void ClassName::ZeroCounters() { MemberList(PC_FN_ZERO) }                                               \
  void to_json(nlohmann::json &j, const ClassName &c) { j = c.ToJson(); }  /* NOLINT */                   \
  void from_json(const nlohmann::json &j, ClassName &c) { c.FromJson(j); } /* NOLINT */

// note: this is a rare correct usage of inline in modern C++
// If you include a header file in multiple translation units, the multiple definitions conflict
// and you get linker errors. We mark it inline so that the compiler figures it out.
