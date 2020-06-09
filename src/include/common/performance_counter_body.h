#pragma once

#include <atomic>
#include <string>
#include "common/json.h"
#include "common/performance_counter.h"

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

/*
 * Using this macro also requires using the matching macro DEFINE_PERFORMANCE_CLASS_HEADER
 * from performance_counter.h.
 */

#define DEFINE_PERFORMANCE_CLASS_BODY(ClassName, MemberList)                            \
  nlohmann::json ClassName::ToJson() const {                                            \
    nlohmann::json output;                                                              \
    output["CounterName"] = #ClassName;                                                 \
    MemberList(PC_FN_JSON_TO);                                                          \
    return output;                                                                      \
  }                                                                                     \
  void ClassName::FromJson(const nlohmann::json &j) { MemberList(PC_FN_JSON_FROM); }    \
                                                                                        \
  void ClassName::ZeroCounters() { MemberList(PC_FN_ZERO) }                             \
  void to_json(nlohmann::json &j, const ClassName &c) { j = c.ToJson(); }  /* NOLINT */ \
  void from_json(const nlohmann::json &j, ClassName &c) { c.FromJson(j); } /* NOLINT */
