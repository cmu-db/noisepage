/***************************************************************************
 *   Copyright (C) 2008 by H-Store Project                                 *
 *   Brown University                                                      *
 *   Massachusetts Institute of Technology                                 *
 *   Yale University                                                       *
 *                                                                         *
 *   This software may be modified and distributed under the terms         *
 *   of the MIT license.  See the LICENSE file for details.                *
 *                                                                         *
 ***************************************************************************/

#ifndef HSTOREDEBUGLOG_H
#define HSTOREDEBUGLOG_H

/**
 * Debug logging functions for EE. Unlike the performance counters,
 * these are just StringUtil::Format() turned on/off by LOG_LEVEL compile option.
 * The main concern here is not to add any overhead on runtime performance
 * when the logging is turned off. Use LOG_XXX_ENABLED macros defined here to
 * eliminate all instructions in the final binary.
 * @author Hideaki
*/

/**
 * To be more useful debugging, added command arguments for the server to control
 * log levels on runtime. Note that the log level disabled by compile option
 * doesn't work on runtime even if enables the log level on runtime.
 *   e.g.) compiled with -DLOG_LEVEL=LOG_LEVEL_DEBUG
 *          => ./terrier -log_level=TRACE  =>  LOG_TRACE() is never called
 */

#include "easylogging++/easylogging++.h"
#include "util/string_util.h"

#include <ctime>
#include <string>

// Fix for PRId64 (See https://stackoverflow.com/a/18719205)
#if defined(__cplusplus) && !defined(__STDC_FORMAT_MACROS)
#define __STDC_FORMAT_MACROS 1 // Not sure where to put this
#endif 
#include <inttypes.h>

// Note that __PELOTONFILE__ is a special pre-processor macro that we
// generate for shorter path names using CMake.

// Log levels for compile option.
#define LOG_LEVEL_OFF 1000
#define LOG_LEVEL_ERROR 500
#define LOG_LEVEL_WARN 400
#define LOG_LEVEL_INFO 300
#define LOG_LEVEL_DEBUG 200
#define LOG_LEVEL_TRACE 100
#define LOG_LEVEL_ALL 0

// Compile Option
#ifndef LOG_LEVEL
// TODO : any way to use pragma message in GCC?
//#pragma message("Warning: LOG_LEVEL compile option was not explicitly
// given.")
#ifndef NDEBUG
// In debug mode, all log macros are usable.
#define LOG_LEVEL LOG_LEVEL_ALL
#else
#define LOG_LEVEL LOG_LEVEL_INFO
#endif
#endif

// For compilers which do not support __FUNCTION__
#if !defined(__FUNCTION__) && !defined(__GNUC__)
#define __FUNCTION__ ""
#endif

// Two convenient macros for debugging
// 1. Logging macros.
// 2. LOG_XXX_ENABLED macros. Use these to "eliminate" all the debug blocks from
// release binary.
#ifdef LOG_ERROR_ENABLED
#undef LOG_ERROR_ENABLED
#endif
#if LOG_LEVEL <= LOG_LEVEL_ERROR
#define LOG_ERROR_ENABLED
#define LOG_ERROR(...) CLOG(ERROR, LOGGER_NAME) << terrier::StringUtil::Format(__VA_ARGS__);
#else
#define LOG_ERROR(...) ((void)0)
#endif

#ifdef LOG_WARN_ENABLED
#undef LOG_WARN_ENABLED
#endif
#if LOG_LEVEL <= LOG_LEVEL_WARN
#define LOG_WARN_ENABLED
#define LOG_WARN(...) CLOG(WARNING, LOGGER_NAME) << terrier::StringUtil::Format(__VA_ARGS__);
#else
#define LOG_WARN(...) ((void)0)
#endif

#ifdef LOG_INFO_ENABLED
#undef LOG_INFO_ENABLED
#endif
#if LOG_LEVEL <= LOG_LEVEL_INFO
#define LOG_INFO_ENABLED
#define LOG_INFO(...) CLOG(INFO, LOGGER_NAME) << terrier::StringUtil::Format(__VA_ARGS__);
#else
#define LOG_INFO(...) ((void)0)
#endif

#ifdef LOG_DEBUG_ENABLED
#undef LOG_DEBUG_ENABLED
#endif
#if LOG_LEVEL <= LOG_LEVEL_DEBUG
#define LOG_DEBUG_ENABLED
#define LOG_DEBUG(...) CLOG(DEBUG, LOGGER_NAME) << terrier::StringUtil::Format(__VA_ARGS__);
#else
#define LOG_DEBUG(...) ((void)0)
#endif

#ifdef LOG_TRACE_ENABLED
#undef LOG_TRACE_ENABLED
#endif
#if LOG_LEVEL <= LOG_LEVEL_TRACE
#define LOG_TRACE_ENABLED
#define LOG_TRACE(...) CLOG(TRACE, LOGGER_NAME) << terrier::StringUtil::Format(__VA_ARGS__);
#else
#define LOG_TRACE(...) ((void)0)
#endif


namespace terrier {

// Macros for logger initialization
#define LOGGER_NAME "terrier"
#define LOG_FORMAT "%datetime [%file:%line] %level - %msg"
#define LOG_FLUSH_THERESHOLD "1"

class Logger {
 public:
  static void InitializeLogger();
};

}  // namespace terrier

#endif
