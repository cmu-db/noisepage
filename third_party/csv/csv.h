#pragma once
/*
CSV for C++, version 1.2.0
https://github.com/vincentlaucsb/csv-parser

MIT License

Copyright (c) 2017-2019 Vincent La

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

#ifndef CSV_HPP
#define CSV_HPP


#endif
// Copyright 2017-2019 by Martin Moene
//
// string-view lite, a C++17-like string_view for C++98 and later.
// For more information see https://github.com/martinmoene/string-view-lite
//
// Distributed under the Boost Software License, Version 1.0.
// (See accompanying file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)


#ifndef NONSTD_SV_LITE_H_INCLUDED
#define NONSTD_SV_LITE_H_INCLUDED

#define string_view_lite_MAJOR  1
#define string_view_lite_MINOR  1
#define string_view_lite_PATCH  0

#define string_view_lite_VERSION  nssv_STRINGIFY(string_view_lite_MAJOR) "." nssv_STRINGIFY(string_view_lite_MINOR) "." nssv_STRINGIFY(string_view_lite_PATCH)

#define nssv_STRINGIFY(  x )  nssv_STRINGIFY_( x )
#define nssv_STRINGIFY_( x )  #x

// string-view lite configuration:

#define nssv_STRING_VIEW_DEFAULT  0
#define nssv_STRING_VIEW_NONSTD   1
#define nssv_STRING_VIEW_STD      2

#if !defined( nssv_CONFIG_SELECT_STRING_VIEW )
# define nssv_CONFIG_SELECT_STRING_VIEW  ( nssv_HAVE_STD_STRING_VIEW ? nssv_STRING_VIEW_STD : nssv_STRING_VIEW_NONSTD )
#endif

#if defined( nssv_CONFIG_SELECT_STD_STRING_VIEW ) || defined( nssv_CONFIG_SELECT_NONSTD_STRING_VIEW )
# error nssv_CONFIG_SELECT_STD_STRING_VIEW and nssv_CONFIG_SELECT_NONSTD_STRING_VIEW are deprecated and removed, please use nssv_CONFIG_SELECT_STRING_VIEW=nssv_STRING_VIEW_...
#endif

#ifndef  nssv_CONFIG_STD_SV_OPERATOR
# define nssv_CONFIG_STD_SV_OPERATOR  0
#endif

#ifndef  nssv_CONFIG_USR_SV_OPERATOR
# define nssv_CONFIG_USR_SV_OPERATOR  1
#endif

#ifdef   nssv_CONFIG_CONVERSION_STD_STRING
# define nssv_CONFIG_CONVERSION_STD_STRING_CLASS_METHODS   nssv_CONFIG_CONVERSION_STD_STRING
# define nssv_CONFIG_CONVERSION_STD_STRING_FREE_FUNCTIONS  nssv_CONFIG_CONVERSION_STD_STRING
#endif

#ifndef  nssv_CONFIG_CONVERSION_STD_STRING_CLASS_METHODS
# define nssv_CONFIG_CONVERSION_STD_STRING_CLASS_METHODS  1
#endif

#ifndef  nssv_CONFIG_CONVERSION_STD_STRING_FREE_FUNCTIONS
# define nssv_CONFIG_CONVERSION_STD_STRING_FREE_FUNCTIONS  1
#endif

// Control presence of exception handling (try and auto discover):

#ifndef nssv_CONFIG_NO_EXCEPTIONS
# if defined(__cpp_exceptions) || defined(__EXCEPTIONS) || defined(_CPPUNWIND)
#  define nssv_CONFIG_NO_EXCEPTIONS  0
# else
#  define nssv_CONFIG_NO_EXCEPTIONS  1
# endif
#endif

// C++ language version detection (C++20 is speculative):
// Note: VC14.0/1900 (VS2015) lacks too much from C++14.

#ifndef   nssv_CPLUSPLUS
# if defined(_MSVC_LANG ) && !defined(__clang__)
#  define nssv_CPLUSPLUS  (_MSC_VER == 1900 ? 201103L : _MSVC_LANG )
# else
#  define nssv_CPLUSPLUS  __cplusplus
# endif
#endif

#define nssv_CPP98_OR_GREATER  ( nssv_CPLUSPLUS >= 199711L )
#define nssv_CPP11_OR_GREATER  ( nssv_CPLUSPLUS >= 201103L )
#define nssv_CPP11_OR_GREATER_ ( nssv_CPLUSPLUS >= 201103L )
#define nssv_CPP14_OR_GREATER  ( nssv_CPLUSPLUS >= 201402L )
#define nssv_CPP17_OR_GREATER  ( nssv_CPLUSPLUS >= 201703L )
#define nssv_CPP20_OR_GREATER  ( nssv_CPLUSPLUS >= 202000L )

// use C++17 std::string_view if available and requested:

#if nssv_CPP17_OR_GREATER && defined(__has_include )
# if __has_include( <string_view> )
#  define nssv_HAVE_STD_STRING_VIEW  1
# else
#  define nssv_HAVE_STD_STRING_VIEW  0
# endif
#else
# define  nssv_HAVE_STD_STRING_VIEW  0
#endif

#define  nssv_USES_STD_STRING_VIEW  ( (nssv_CONFIG_SELECT_STRING_VIEW == nssv_STRING_VIEW_STD) || ((nssv_CONFIG_SELECT_STRING_VIEW == nssv_STRING_VIEW_DEFAULT) && nssv_HAVE_STD_STRING_VIEW) )

#define nssv_HAVE_STARTS_WITH ( nssv_CPP20_OR_GREATER || !nssv_USES_STD_STRING_VIEW )
#define nssv_HAVE_ENDS_WITH     nssv_HAVE_STARTS_WITH

//
// Use C++17 std::string_view:
//

#if nssv_USES_STD_STRING_VIEW

#include <string_view>

// Extensions for std::string:

#if nssv_CONFIG_CONVERSION_STD_STRING_FREE_FUNCTIONS

namespace nonstd {

template< class CharT, class Traits, class Allocator = std::allocator<CharT> >
std::basic_string<CharT, Traits, Allocator>
to_string( std::basic_string_view<CharT, Traits> v, Allocator const & a = Allocator() )
{
  return std::basic_string<CharT,Traits, Allocator>( v.begin(), v.end(), a );
}

template< class CharT, class Traits, class Allocator >
std::basic_string_view<CharT, Traits>
to_string_view( std::basic_string<CharT, Traits, Allocator> const & s )
{
  return std::basic_string_view<CharT, Traits>( s.data(), s.size() );
}

// Literal operators sv and _sv:

#if nssv_CONFIG_STD_SV_OPERATOR

using namespace std::literals::string_view_literals;

#endif

#if nssv_CONFIG_USR_SV_OPERATOR

inline namespace literals {
inline namespace string_view_literals {


constexpr std::string_view operator "" _sv( const char* str, size_t len ) noexcept  // (1)
{
  return std::string_view{ str, len };
}

constexpr std::u16string_view operator "" _sv( const char16_t* str, size_t len ) noexcept  // (2)
{
  return std::u16string_view{ str, len };
}

constexpr std::u32string_view operator "" _sv( const char32_t* str, size_t len ) noexcept  // (3)
{
  return std::u32string_view{ str, len };
}

constexpr std::wstring_view operator "" _sv( const wchar_t* str, size_t len ) noexcept  // (4)
{
  return std::wstring_view{ str, len };
}

}} // namespace literals::string_view_literals

#endif // nssv_CONFIG_USR_SV_OPERATOR

} // namespace nonstd

#endif // nssv_CONFIG_CONVERSION_STD_STRING_FREE_FUNCTIONS

namespace nonstd {

using std::string_view;
using std::wstring_view;
using std::u16string_view;
using std::u32string_view;
using std::basic_string_view;

// literal "sv" and "_sv", see above

using std::operator==;
using std::operator!=;
using std::operator<;
using std::operator<=;
using std::operator>;
using std::operator>=;

using std::operator<<;

} // namespace nonstd

#else // nssv_HAVE_STD_STRING_VIEW

//
// Before C++17: use string_view lite:
//

// Compiler versions:
//
// MSVC++ 6.0  _MSC_VER == 1200 (Visual Studio 6.0)
// MSVC++ 7.0  _MSC_VER == 1300 (Visual Studio .NET 2002)
// MSVC++ 7.1  _MSC_VER == 1310 (Visual Studio .NET 2003)
// MSVC++ 8.0  _MSC_VER == 1400 (Visual Studio 2005)
// MSVC++ 9.0  _MSC_VER == 1500 (Visual Studio 2008)
// MSVC++ 10.0 _MSC_VER == 1600 (Visual Studio 2010)
// MSVC++ 11.0 _MSC_VER == 1700 (Visual Studio 2012)
// MSVC++ 12.0 _MSC_VER == 1800 (Visual Studio 2013)
// MSVC++ 14.0 _MSC_VER == 1900 (Visual Studio 2015)
// MSVC++ 14.1 _MSC_VER >= 1910 (Visual Studio 2017)

#if defined(_MSC_VER ) && !defined(__clang__)
# define nssv_COMPILER_MSVC_VER      (_MSC_VER )
# define nssv_COMPILER_MSVC_VERSION  (_MSC_VER / 10 - 10 * ( 5 + (_MSC_VER < 1900 ) ) )
#else
# define nssv_COMPILER_MSVC_VER      0
# define nssv_COMPILER_MSVC_VERSION  0
#endif

#define nssv_COMPILER_VERSION( major, minor, patch )  (10 * ( 10 * major + minor) + patch)

#if defined(__clang__)
# define nssv_COMPILER_CLANG_VERSION  nssv_COMPILER_VERSION(__clang_major__, __clang_minor__, __clang_patchlevel__)
#else
# define nssv_COMPILER_CLANG_VERSION    0
#endif

#if defined(__GNUC__) && !defined(__clang__)
# define nssv_COMPILER_GNUC_VERSION  nssv_COMPILER_VERSION(__GNUC__, __GNUC_MINOR__, __GNUC_PATCHLEVEL__)
#else
# define nssv_COMPILER_GNUC_VERSION    0
#endif

// half-open range [lo..hi):
#define nssv_BETWEEN( v, lo, hi ) ( (lo) <= (v) && (v) < (hi) )

// Presence of language and library features:

#ifdef _HAS_CPP0X
# define nssv_HAS_CPP0X  _HAS_CPP0X
#else
# define nssv_HAS_CPP0X  0
#endif

// Unless defined otherwise below, consider VC14 as C++11 for variant-lite:

#if nssv_COMPILER_MSVC_VER >= 1900
# undef  nssv_CPP11_OR_GREATER
# define nssv_CPP11_OR_GREATER  1
#endif

#define nssv_CPP11_90   (nssv_CPP11_OR_GREATER_ || nssv_COMPILER_MSVC_VER >= 1500)
#define nssv_CPP11_100  (nssv_CPP11_OR_GREATER_ || nssv_COMPILER_MSVC_VER >= 1600)
#define nssv_CPP11_110  (nssv_CPP11_OR_GREATER_ || nssv_COMPILER_MSVC_VER >= 1700)
#define nssv_CPP11_120  (nssv_CPP11_OR_GREATER_ || nssv_COMPILER_MSVC_VER >= 1800)
#define nssv_CPP11_140  (nssv_CPP11_OR_GREATER_ || nssv_COMPILER_MSVC_VER >= 1900)
#define nssv_CPP11_141  (nssv_CPP11_OR_GREATER_ || nssv_COMPILER_MSVC_VER >= 1910)

#define nssv_CPP14_000  (nssv_CPP14_OR_GREATER)
#define nssv_CPP17_000  (nssv_CPP17_OR_GREATER)

// Presence of C++11 language features:

#define nssv_HAVE_CONSTEXPR_11          nssv_CPP11_140
#define nssv_HAVE_EXPLICIT_CONVERSION   nssv_CPP11_140
#define nssv_HAVE_INLINE_NAMESPACE      nssv_CPP11_140
#define nssv_HAVE_NOEXCEPT              nssv_CPP11_140
#define nssv_HAVE_NULLPTR               nssv_CPP11_100
#define nssv_HAVE_REF_QUALIFIER         nssv_CPP11_140
#define nssv_HAVE_UNICODE_LITERALS      nssv_CPP11_140
#define nssv_HAVE_USER_DEFINED_LITERALS nssv_CPP11_140
#define nssv_HAVE_WCHAR16_T             nssv_CPP11_100
#define nssv_HAVE_WCHAR32_T             nssv_CPP11_100

#if ! ( ( nssv_CPP11 && nssv_COMPILER_CLANG_VERSION ) || nssv_BETWEEN( nssv_COMPILER_CLANG_VERSION, 300, 400 ) )
# define nssv_HAVE_STD_DEFINED_LITERALS  nssv_CPP11_140
#endif

// Presence of C++14 language features:

#define nssv_HAVE_CONSTEXPR_14          nssv_CPP14_000

// Presence of C++17 language features:

#define nssv_HAVE_NODISCARD             nssv_CPP17_000

// Presence of C++ library features:

#define nssv_HAVE_STD_HASH              nssv_CPP11_120

// C++ feature usage:

#if nssv_HAVE_CONSTEXPR_11
# define nssv_constexpr  constexpr
#else
# define nssv_constexpr  /*constexpr*/
#endif

#if  nssv_HAVE_CONSTEXPR_14
# define nssv_constexpr14  constexpr
#else
# define nssv_constexpr14  /*constexpr*/
#endif

#if nssv_HAVE_EXPLICIT_CONVERSION
# define nssv_explicit  explicit
#else
# define nssv_explicit  /*explicit*/
#endif

#if nssv_HAVE_INLINE_NAMESPACE
# define nssv_inline_ns  inline
#else
# define nssv_inline_ns  /*inline*/
#endif

#if nssv_HAVE_NOEXCEPT
# define nssv_noexcept  noexcept
#else
# define nssv_noexcept  /*noexcept*/
#endif

//#if nssv_HAVE_REF_QUALIFIER
//# define nssv_ref_qual  &
//# define nssv_refref_qual  &&
//#else
//# define nssv_ref_qual  /*&*/
//# define nssv_refref_qual  /*&&*/
//#endif

#if nssv_HAVE_NULLPTR
# define nssv_nullptr  nullptr
#else
# define nssv_nullptr  NULL
#endif

#if nssv_HAVE_NODISCARD
# define nssv_nodiscard  [[nodiscard]]
#else
# define nssv_nodiscard  /*[[nodiscard]]*/
#endif

// Additional includes:

#include <algorithm>
#include <cassert>
#include <iterator>
#include <limits>
#include <ostream>
#include <string>   // std::char_traits<>

#if ! nssv_CONFIG_NO_EXCEPTIONS
# include <stdexcept>
#endif

#if nssv_CPP11_OR_GREATER
# include <type_traits>
#endif

// Clang, GNUC, MSVC warning suppression macros:

#if defined(__clang__)
# pragma clang diagnostic ignored "-Wreserved-user-defined-literal"
# pragma clang diagnostic push
# pragma clang diagnostic ignored "-Wuser-defined-literals"
#elif defined(__GNUC__)
# pragma  GCC  diagnostic push
# pragma  GCC  diagnostic ignored "-Wliteral-suffix"
#endif // __clang__

#if nssv_COMPILER_MSVC_VERSION >= 140
# define nssv_SUPPRESS_MSGSL_WARNING(expr)        [[gsl::suppress(expr)]]
# define nssv_SUPPRESS_MSVC_WARNING(code, descr)  __pragma(warning(suppress: code) )
# define nssv_DISABLE_MSVC_WARNINGS(codes)        __pragma(warning(push))  __pragma(warning(disable: codes))
#else
# define nssv_SUPPRESS_MSGSL_WARNING(expr)
# define nssv_SUPPRESS_MSVC_WARNING(code, descr)
# define nssv_DISABLE_MSVC_WARNINGS(codes)
#endif

#if defined(__clang__)
# define nssv_RESTORE_WARNINGS()  _Pragma("clang diagnostic pop")
#elif defined(__GNUC__)
# define nssv_RESTORE_WARNINGS()  _Pragma("GCC diagnostic pop")
#elif nssv_COMPILER_MSVC_VERSION >= 140
# define nssv_RESTORE_WARNINGS()  __pragma(warning(pop ))
#else
# define nssv_RESTORE_WARNINGS()
#endif

// Suppress the following MSVC (GSL) warnings:
// - C4455, non-gsl   : 'operator ""sv': literal suffix identifiers that do not
//                      start with an underscore are reserved
// - C26472, gsl::t.1 : don't use a static_cast for arithmetic conversions;
//                      use brace initialization, gsl::narrow_cast or gsl::narow
// - C26481: gsl::b.1 : don't use pointer arithmetic. Use span instead

nssv_DISABLE_MSVC_WARNINGS( 4455 26481 26472 )
//nssv_DISABLE_CLANG_WARNINGS( "-Wuser-defined-literals" )
//nssv_DISABLE_GNUC_WARNINGS( -Wliteral-suffix )

namespace nonstd { namespace sv_lite {

template
<
    class CharT,
    class Traits = std::char_traits<CharT>
>
class basic_string_view;

//
// basic_string_view:
//

template
<
    class CharT,
    class Traits /* = std::char_traits<CharT> */
>
class basic_string_view
{
public:
    // Member types:

    typedef Traits traits_type;
    typedef CharT  value_type;

    typedef CharT       * pointer;
    typedef CharT const * const_pointer;
    typedef CharT       & reference;
    typedef CharT const & const_reference;

    typedef const_pointer iterator;
    typedef const_pointer const_iterator;
    typedef std::reverse_iterator< const_iterator > reverse_iterator;
    typedef	std::reverse_iterator< const_iterator > const_reverse_iterator;

    typedef std::size_t     size_type;
    typedef std::ptrdiff_t  difference_type;

    // 24.4.2.1 Construction and assignment:

    nssv_constexpr basic_string_view() nssv_noexcept
        : data_( nssv_nullptr )
        , size_( 0 )
    {}

#if nssv_CPP11_OR_GREATER
    nssv_constexpr basic_string_view( basic_string_view const & other ) nssv_noexcept = default;
#else
    nssv_constexpr basic_string_view( basic_string_view const & other ) nssv_noexcept
        : data_( other.data_)
        , size_( other.size_)
    {}
#endif

    nssv_constexpr basic_string_view( CharT const * s, size_type count )
        : data_( s )
        , size_( count )
    {}

    nssv_constexpr basic_string_view( CharT const * s)
        : data_( s )
        , size_( Traits::length(s) )
    {}

    // Assignment:

#if nssv_CPP11_OR_GREATER
    nssv_constexpr14 basic_string_view & operator=( basic_string_view const & other ) nssv_noexcept = default;
#else
    nssv_constexpr14 basic_string_view & operator=( basic_string_view const & other ) nssv_noexcept
    {
        data_ = other.data_;
        size_ = other.size_;
        return *this;
    }
#endif

    // 24.4.2.2 Iterator support:

    nssv_constexpr const_iterator begin()  const nssv_noexcept { return data_;         }
    nssv_constexpr const_iterator end()    const nssv_noexcept { return data_ + size_; }

    nssv_constexpr const_iterator cbegin() const nssv_noexcept { return begin(); }
    nssv_constexpr const_iterator cend()   const nssv_noexcept { return end();   }

    nssv_constexpr const_reverse_iterator rbegin()  const nssv_noexcept { return const_reverse_iterator( end() );   }
    nssv_constexpr const_reverse_iterator rend()    const nssv_noexcept { return const_reverse_iterator( begin() ); }

    nssv_constexpr const_reverse_iterator crbegin() const nssv_noexcept { return rbegin(); }
    nssv_constexpr const_reverse_iterator crend()   const nssv_noexcept { return rend();   }

    // 24.4.2.3 Capacity:

    nssv_constexpr size_type size()     const nssv_noexcept { return size_; }
    nssv_constexpr size_type length()   const nssv_noexcept { return size_; }
    nssv_constexpr size_type max_size() const nssv_noexcept { return (std::numeric_limits< size_type >::max)(); }

    // since C++20
    nssv_nodiscard nssv_constexpr bool empty() const nssv_noexcept
    {
        return 0 == size_;
    }

    // 24.4.2.4 Element access:

    nssv_constexpr const_reference operator[]( size_type pos ) const
    {
        return data_at( pos );
    }

    nssv_constexpr14 const_reference at( size_type pos ) const
    {
#if nssv_CONFIG_NO_EXCEPTIONS
        assert( pos < size() );
#else
        if ( pos >= size() )
        {
            throw std::out_of_range("nonst::string_view::at()");
        }
#endif
        return data_at( pos );
    }

    nssv_constexpr const_reference front() const { return data_at( 0 );          }
    nssv_constexpr const_reference back()  const { return data_at( size() - 1 ); }

    nssv_constexpr const_pointer   data()  const nssv_noexcept { return data_; }

    // 24.4.2.5 Modifiers:

    nssv_constexpr14 void remove_prefix( size_type n )
    {
        assert( n <= size() );
        data_ += n;
        size_ -= n;
    }

    nssv_constexpr14 void remove_suffix( size_type n )
    {
        assert( n <= size() );
        size_ -= n;
    }

    nssv_constexpr14 void swap( basic_string_view & other ) nssv_noexcept
    {
        using std::swap;
        swap( data_, other.data_ );
        swap( size_, other.size_ );
    }

    // 24.4.2.6 String operations:

    size_type copy( CharT * dest, size_type n, size_type pos = 0 ) const
    {
#if nssv_CONFIG_NO_EXCEPTIONS
        assert( pos <= size() );
#else
        if ( pos > size() )
        {
            throw std::out_of_range("nonst::string_view::copy()");
        }
#endif
        const size_type rlen = (std::min)( n, size() - pos );

        (void) Traits::copy( dest, data() + pos, rlen );

        return rlen;
    }

    nssv_constexpr14 basic_string_view substr( size_type pos = 0, size_type n = npos ) const
    {
#if nssv_CONFIG_NO_EXCEPTIONS
        assert( pos <= size() );
#else
        if ( pos > size() )
        {
            throw std::out_of_range("nonst::string_view::substr()");
        }
#endif
        return basic_string_view( data() + pos, (std::min)( n, size() - pos ) );
    }

    // compare(), 6x:

    nssv_constexpr14 int compare( basic_string_view other ) const nssv_noexcept // (1)
    {
        if ( const int result = Traits::compare( data(), other.data(), (std::min)( size(), other.size() ) ) )
            return result;

        return size() == other.size() ? 0 : size() < other.size() ? -1 : 1;
    }

    nssv_constexpr int compare( size_type pos1, size_type n1, basic_string_view other ) const // (2)
    {
        return substr( pos1, n1 ).compare( other );
    }

    nssv_constexpr int compare( size_type pos1, size_type n1, basic_string_view other, size_type pos2, size_type n2 ) const // (3)
    {
        return substr( pos1, n1 ).compare( other.substr( pos2, n2 ) );
    }

    nssv_constexpr int compare( CharT const * s ) const // (4)
    {
        return compare( basic_string_view( s ) );
    }

    nssv_constexpr int compare( size_type pos1, size_type n1, CharT const * s ) const // (5)
    {
        return substr( pos1, n1 ).compare( basic_string_view( s ) );
    }

    nssv_constexpr int compare( size_type pos1, size_type n1, CharT const * s, size_type n2 ) const // (6)
    {
        return substr( pos1, n1 ).compare( basic_string_view( s, n2 ) );
    }

    // 24.4.2.7 Searching:

    // starts_with(), 3x, since C++20:

    nssv_constexpr bool starts_with( basic_string_view v ) const nssv_noexcept  // (1)
    {
        return size() >= v.size() && compare( 0, v.size(), v ) == 0;
    }

    nssv_constexpr bool starts_with( CharT c ) const nssv_noexcept  // (2)
    {
        return starts_with( basic_string_view( &c, 1 ) );
    }

    nssv_constexpr bool starts_with( CharT const * s ) const  // (3)
    {
        return starts_with( basic_string_view( s ) );
    }

    // ends_with(), 3x, since C++20:

    nssv_constexpr bool ends_with( basic_string_view v ) const nssv_noexcept  // (1)
    {
        return size() >= v.size() && compare( size() - v.size(), npos, v ) == 0;
    }

    nssv_constexpr bool ends_with( CharT c ) const nssv_noexcept  // (2)
    {
        return ends_with( basic_string_view( &c, 1 ) );
    }

    nssv_constexpr bool ends_with( CharT const * s ) const  // (3)
    {
        return ends_with( basic_string_view( s ) );
    }

    // find(), 4x:

    nssv_constexpr14 size_type find( basic_string_view v, size_type pos = 0 ) const nssv_noexcept  // (1)
    {
        return assert( v.size() == 0 || v.data() != nssv_nullptr )
            , pos >= size()
            ? npos
            : to_pos( std::search( cbegin() + pos, cend(), v.cbegin(), v.cend(), Traits::eq ) );
    }

    nssv_constexpr14 size_type find( CharT c, size_type pos = 0 ) const nssv_noexcept  // (2)
    {
        return find( basic_string_view( &c, 1 ), pos );
    }

    nssv_constexpr14 size_type find( CharT const * s, size_type pos, size_type n ) const  // (3)
    {
        return find( basic_string_view( s, n ), pos );
    }

    nssv_constexpr14 size_type find( CharT const * s, size_type pos = 0 ) const  // (4)
    {
        return find( basic_string_view( s ), pos );
    }

    // rfind(), 4x:

    nssv_constexpr14 size_type rfind( basic_string_view v, size_type pos = npos ) const nssv_noexcept  // (1)
    {
        if ( size() < v.size() )
            return npos;

        if ( v.empty() )
            return (std::min)( size(), pos );

        const_iterator last   = cbegin() + (std::min)( size() - v.size(), pos ) + v.size();
        const_iterator result = std::find_end( cbegin(), last, v.cbegin(), v.cend(), Traits::eq );

        return result != last ? size_type( result - cbegin() ) : npos;
    }

    nssv_constexpr14 size_type rfind( CharT c, size_type pos = npos ) const nssv_noexcept  // (2)
    {
        return rfind( basic_string_view( &c, 1 ), pos );
    }

    nssv_constexpr14 size_type rfind( CharT const * s, size_type pos, size_type n ) const  // (3)
    {
        return rfind( basic_string_view( s, n ), pos );
    }

    nssv_constexpr14 size_type rfind( CharT const * s, size_type pos = npos ) const  // (4)
    {
        return rfind( basic_string_view( s ), pos );
    }

    // find_first_of(), 4x:

    nssv_constexpr size_type find_first_of( basic_string_view v, size_type pos = 0 ) const nssv_noexcept  // (1)
    {
        return pos >= size()
            ? npos
            : to_pos( std::find_first_of( cbegin() + pos, cend(), v.cbegin(), v.cend(), Traits::eq ) );
    }

    nssv_constexpr size_type find_first_of( CharT c, size_type pos = 0 ) const nssv_noexcept  // (2)
    {
        return find_first_of( basic_string_view( &c, 1 ), pos );
    }

    nssv_constexpr size_type find_first_of( CharT const * s, size_type pos, size_type n ) const  // (3)
    {
        return find_first_of( basic_string_view( s, n ), pos );
    }

    nssv_constexpr size_type find_first_of(  CharT const * s, size_type pos = 0 ) const  // (4)
    {
        return find_first_of( basic_string_view( s ), pos );
    }

    // find_last_of(), 4x:

    nssv_constexpr size_type find_last_of( basic_string_view v, size_type pos = npos ) const nssv_noexcept  // (1)
    {
        return empty()
            ? npos
            : pos >= size()
            ? find_last_of( v, size() - 1 )
            : to_pos( std::find_first_of( const_reverse_iterator( cbegin() + pos + 1 ), crend(), v.cbegin(), v.cend(), Traits::eq ) );
    }

    nssv_constexpr size_type find_last_of( CharT c, size_type pos = npos ) const nssv_noexcept  // (2)
    {
        return find_last_of( basic_string_view( &c, 1 ), pos );
    }

    nssv_constexpr size_type find_last_of( CharT const * s, size_type pos, size_type count ) const  // (3)
    {
        return find_last_of( basic_string_view( s, count ), pos );
    }

    nssv_constexpr size_type find_last_of( CharT const * s, size_type pos = npos ) const  // (4)
    {
        return find_last_of( basic_string_view( s ), pos );
    }

    // find_first_not_of(), 4x:

    nssv_constexpr size_type find_first_not_of( basic_string_view v, size_type pos = 0 ) const nssv_noexcept  // (1)
    {
        return pos >= size()
            ? npos
            : to_pos( std::find_if( cbegin() + pos, cend(), not_in_view( v ) ) );
    }

    nssv_constexpr size_type find_first_not_of( CharT c, size_type pos = 0 ) const nssv_noexcept  // (2)
    {
        return find_first_not_of( basic_string_view( &c, 1 ), pos );
    }

    nssv_constexpr size_type find_first_not_of( CharT const * s, size_type pos, size_type count ) const  // (3)
    {
        return find_first_not_of( basic_string_view( s, count ), pos );
    }

    nssv_constexpr size_type find_first_not_of( CharT const * s, size_type pos = 0 ) const  // (4)
    {
        return find_first_not_of( basic_string_view( s ), pos );
    }

    // find_last_not_of(), 4x:

    nssv_constexpr size_type find_last_not_of( basic_string_view v, size_type pos = npos ) const nssv_noexcept  // (1)
    {
        return empty()
            ? npos
            : pos >= size()
            ? find_last_not_of( v, size() - 1 )
            : to_pos( std::find_if( const_reverse_iterator( cbegin() + pos + 1 ), crend(), not_in_view( v ) ) );
    }

    nssv_constexpr size_type find_last_not_of( CharT c, size_type pos = npos ) const nssv_noexcept  // (2)
    {
        return find_last_not_of( basic_string_view( &c, 1 ), pos );
    }

    nssv_constexpr size_type find_last_not_of( CharT const * s, size_type pos, size_type count ) const  // (3)
    {
        return find_last_not_of( basic_string_view( s, count ), pos );
    }

    nssv_constexpr size_type find_last_not_of( CharT const * s, size_type pos = npos ) const  // (4)
    {
        return find_last_not_of( basic_string_view( s ), pos );
    }

    // Constants:

#if nssv_CPP17_OR_GREATER
    static nssv_constexpr size_type npos = size_type(-1);
#elif nssv_CPP11_OR_GREATER
    enum : size_type { npos = size_type(-1) };
#else
    enum { npos = size_type(-1) };
#endif

private:
    struct not_in_view
    {
        const basic_string_view v;

        nssv_constexpr not_in_view( basic_string_view v ) : v( v ) {}

        nssv_constexpr bool operator()( CharT c ) const
        {
            return npos == v.find_first_of( c );
        }
    };

    nssv_constexpr size_type to_pos( const_iterator it ) const
    {
        return it == cend() ? npos : size_type( it - cbegin() );
    }

    nssv_constexpr size_type to_pos( const_reverse_iterator it ) const
    {
        return it == crend() ? npos : size_type( crend() - it - 1 );
    }

    nssv_constexpr const_reference data_at( size_type pos ) const
    {
#if nssv_BETWEEN( nssv_COMPILER_GNUC_VERSION, 1, 500 )
        return data_[pos];
#else
        return assert( pos < size() ), data_[pos];
#endif
    }

private:
    const_pointer data_;
    size_type     size_;

public:
#if nssv_CONFIG_CONVERSION_STD_STRING_CLASS_METHODS

    template< class Allocator >
    basic_string_view( std::basic_string<CharT, Traits, Allocator> const & s ) nssv_noexcept
        : data_( s.data() )
        , size_( s.size() )
    {}

#if nssv_HAVE_EXPLICIT_CONVERSION

    template< class Allocator >
    explicit operator std::basic_string<CharT, Traits, Allocator>() const
    {
        return to_string( Allocator() );
    }

#endif // nssv_HAVE_EXPLICIT_CONVERSION

#if nssv_CPP11_OR_GREATER

    template< class Allocator = std::allocator<CharT> >
    std::basic_string<CharT, Traits, Allocator>
    to_string( Allocator const & a = Allocator() ) const
    {
        return std::basic_string<CharT, Traits, Allocator>( begin(), end(), a );
    }

#else

    std::basic_string<CharT, Traits>
    to_string() const
    {
        return std::basic_string<CharT, Traits>( begin(), end() );
    }

    template< class Allocator >
    std::basic_string<CharT, Traits, Allocator>
    to_string( Allocator const & a ) const
    {
        return std::basic_string<CharT, Traits, Allocator>( begin(), end(), a );
    }

#endif // nssv_CPP11_OR_GREATER

#endif // nssv_CONFIG_CONVERSION_STD_STRING_CLASS_METHODS
};

//
// Non-member functions:
//

// 24.4.3 Non-member comparison functions:
// lexicographically compare two string views (function template):

template< class CharT, class Traits >
nssv_constexpr bool operator== (
    basic_string_view <CharT, Traits> lhs,
    basic_string_view <CharT, Traits> rhs ) nssv_noexcept
{ return lhs.compare( rhs ) == 0 ; }

template< class CharT, class Traits >
nssv_constexpr bool operator!= (
    basic_string_view <CharT, Traits> lhs,
    basic_string_view <CharT, Traits> rhs ) nssv_noexcept
{ return lhs.compare( rhs ) != 0 ; }

template< class CharT, class Traits >
nssv_constexpr bool operator< (
    basic_string_view <CharT, Traits> lhs,
    basic_string_view <CharT, Traits> rhs ) nssv_noexcept
{ return lhs.compare( rhs ) < 0 ; }

template< class CharT, class Traits >
nssv_constexpr bool operator<= (
    basic_string_view <CharT, Traits> lhs,
    basic_string_view <CharT, Traits> rhs ) nssv_noexcept
{ return lhs.compare( rhs ) <= 0 ; }

template< class CharT, class Traits >
nssv_constexpr bool operator> (
    basic_string_view <CharT, Traits> lhs,
    basic_string_view <CharT, Traits> rhs ) nssv_noexcept
{ return lhs.compare( rhs ) > 0 ; }

template< class CharT, class Traits >
nssv_constexpr bool operator>= (
    basic_string_view <CharT, Traits> lhs,
    basic_string_view <CharT, Traits> rhs ) nssv_noexcept
{ return lhs.compare( rhs ) >= 0 ; }

// Let S be basic_string_view<CharT, Traits>, and sv be an instance of S.
// Implementations shall provide sufficient additional overloads marked
// constexpr and noexcept so that an object t with an implicit conversion
// to S can be compared according to Table 67.

#if nssv_CPP11_OR_GREATER && ! nssv_BETWEEN( nssv_COMPILER_MSVC_VERSION, 100, 141 )

#define nssv_BASIC_STRING_VIEW_I(T,U)  typename std::decay< basic_string_view<T,U> >::type

#if nssv_BETWEEN( nssv_COMPILER_MSVC_VERSION, 140, 150 )
# define nssv_MSVC_ORDER(x)  , int=x
#else
# define nssv_MSVC_ORDER(x)  /*, int=x*/
#endif

// ==

template< class CharT, class Traits  nssv_MSVC_ORDER(1) >
nssv_constexpr bool operator==(
         basic_string_view  <CharT, Traits> lhs,
    nssv_BASIC_STRING_VIEW_I(CharT, Traits) rhs ) nssv_noexcept
{ return lhs.compare( rhs ) == 0; }

template< class CharT, class Traits  nssv_MSVC_ORDER(2) >
nssv_constexpr bool operator==(
    nssv_BASIC_STRING_VIEW_I(CharT, Traits) lhs,
         basic_string_view  <CharT, Traits> rhs ) nssv_noexcept
{ return lhs.size() == rhs.size() && lhs.compare( rhs ) == 0; }

// !=

template< class CharT, class Traits  nssv_MSVC_ORDER(1) >
nssv_constexpr bool operator!= (
         basic_string_view  < CharT, Traits > lhs,
    nssv_BASIC_STRING_VIEW_I( CharT, Traits ) rhs ) nssv_noexcept
{ return lhs.size() != rhs.size() || lhs.compare( rhs ) != 0 ; }

template< class CharT, class Traits  nssv_MSVC_ORDER(2) >
nssv_constexpr bool operator!= (
    nssv_BASIC_STRING_VIEW_I( CharT, Traits ) lhs,
         basic_string_view  < CharT, Traits > rhs ) nssv_noexcept
{ return lhs.compare( rhs ) != 0 ; }

// <

template< class CharT, class Traits  nssv_MSVC_ORDER(1) >
nssv_constexpr bool operator< (
         basic_string_view  < CharT, Traits > lhs,
    nssv_BASIC_STRING_VIEW_I( CharT, Traits ) rhs ) nssv_noexcept
{ return lhs.compare( rhs ) < 0 ; }

template< class CharT, class Traits  nssv_MSVC_ORDER(2) >
nssv_constexpr bool operator< (
    nssv_BASIC_STRING_VIEW_I( CharT, Traits ) lhs,
         basic_string_view  < CharT, Traits > rhs ) nssv_noexcept
{ return lhs.compare( rhs ) < 0 ; }

// <=

template< class CharT, class Traits  nssv_MSVC_ORDER(1) >
nssv_constexpr bool operator<= (
         basic_string_view  < CharT, Traits > lhs,
    nssv_BASIC_STRING_VIEW_I( CharT, Traits ) rhs ) nssv_noexcept
{ return lhs.compare( rhs ) <= 0 ; }

template< class CharT, class Traits  nssv_MSVC_ORDER(2) >
nssv_constexpr bool operator<= (
    nssv_BASIC_STRING_VIEW_I( CharT, Traits ) lhs,
         basic_string_view  < CharT, Traits > rhs ) nssv_noexcept
{ return lhs.compare( rhs ) <= 0 ; }

// >

template< class CharT, class Traits  nssv_MSVC_ORDER(1) >
nssv_constexpr bool operator> (
         basic_string_view  < CharT, Traits > lhs,
    nssv_BASIC_STRING_VIEW_I( CharT, Traits ) rhs ) nssv_noexcept
{ return lhs.compare( rhs ) > 0 ; }

template< class CharT, class Traits  nssv_MSVC_ORDER(2) >
nssv_constexpr bool operator> (
    nssv_BASIC_STRING_VIEW_I( CharT, Traits ) lhs,
         basic_string_view  < CharT, Traits > rhs ) nssv_noexcept
{ return lhs.compare( rhs ) > 0 ; }

// >=

template< class CharT, class Traits  nssv_MSVC_ORDER(1) >
nssv_constexpr bool operator>= (
         basic_string_view  < CharT, Traits > lhs,
    nssv_BASIC_STRING_VIEW_I( CharT, Traits ) rhs ) nssv_noexcept
{ return lhs.compare( rhs ) >= 0 ; }

template< class CharT, class Traits  nssv_MSVC_ORDER(2) >
nssv_constexpr bool operator>= (
    nssv_BASIC_STRING_VIEW_I( CharT, Traits ) lhs,
         basic_string_view  < CharT, Traits > rhs ) nssv_noexcept
{ return lhs.compare( rhs ) >= 0 ; }

#undef nssv_MSVC_ORDER
#undef nssv_BASIC_STRING_VIEW_I

#endif // nssv_CPP11_OR_GREATER

// 24.4.4 Inserters and extractors:

namespace detail {

template< class Stream >
void write_padding( Stream & os, std::streamsize n )
{
    for ( std::streamsize i = 0; i < n; ++i )
        os.rdbuf()->sputc( os.fill() );
}

template< class Stream, class View >
Stream & write_to_stream( Stream & os, View const & sv )
{
    typename Stream::sentry sentry( os );

    if ( !os )
        return os;

    const std::streamsize length = static_cast<std::streamsize>( sv.length() );

    // Whether, and how, to pad:
    const bool      pad = ( length < os.width() );
    const bool left_pad = pad && ( os.flags() & std::ios_base::adjustfield ) == std::ios_base::right;

    if ( left_pad )
        write_padding( os, os.width() - length );

    // Write span characters:
    os.rdbuf()->sputn( sv.begin(), length );

    if ( pad && !left_pad )
        write_padding( os, os.width() - length );

    // Reset output stream width:
    os.width( 0 );

    return os;
}

} // namespace detail

template< class CharT, class Traits >
std::basic_ostream<CharT, Traits> &
operator<<(
    std::basic_ostream<CharT, Traits>& os,
    basic_string_view <CharT, Traits> sv )
{
    return detail::write_to_stream( os, sv );
}

// Several typedefs for common character types are provided:

typedef basic_string_view<char>      string_view;
typedef basic_string_view<wchar_t>   wstring_view;
#if nssv_HAVE_WCHAR16_T
typedef basic_string_view<char16_t>  u16string_view;
typedef basic_string_view<char32_t>  u32string_view;
#endif

}} // namespace nonstd::sv_lite

//
// 24.4.6 Suffix for basic_string_view literals:
//

#if nssv_HAVE_USER_DEFINED_LITERALS

namespace nonstd {
nssv_inline_ns namespace literals {
nssv_inline_ns namespace string_view_literals {

#if nssv_CONFIG_STD_SV_OPERATOR && nssv_HAVE_STD_DEFINED_LITERALS

nssv_constexpr nonstd::sv_lite::string_view operator "" sv( const char* str, size_t len ) nssv_noexcept  // (1)
{
    return nonstd::sv_lite::string_view{ str, len };
}

nssv_constexpr nonstd::sv_lite::u16string_view operator "" sv( const char16_t* str, size_t len ) nssv_noexcept  // (2)
{
    return nonstd::sv_lite::u16string_view{ str, len };
}

nssv_constexpr nonstd::sv_lite::u32string_view operator "" sv( const char32_t* str, size_t len ) nssv_noexcept  // (3)
{
    return nonstd::sv_lite::u32string_view{ str, len };
}

nssv_constexpr nonstd::sv_lite::wstring_view operator "" sv( const wchar_t* str, size_t len ) nssv_noexcept  // (4)
{
    return nonstd::sv_lite::wstring_view{ str, len };
}

#endif // nssv_CONFIG_STD_SV_OPERATOR && nssv_HAVE_STD_DEFINED_LITERALS

#if nssv_CONFIG_USR_SV_OPERATOR

nssv_constexpr nonstd::sv_lite::string_view operator "" _sv( const char* str, size_t len ) nssv_noexcept  // (1)
{
    return nonstd::sv_lite::string_view{ str, len };
}

nssv_constexpr nonstd::sv_lite::u16string_view operator "" _sv( const char16_t* str, size_t len ) nssv_noexcept  // (2)
{
    return nonstd::sv_lite::u16string_view{ str, len };
}

nssv_constexpr nonstd::sv_lite::u32string_view operator "" _sv( const char32_t* str, size_t len ) nssv_noexcept  // (3)
{
    return nonstd::sv_lite::u32string_view{ str, len };
}

nssv_constexpr nonstd::sv_lite::wstring_view operator "" _sv( const wchar_t* str, size_t len ) nssv_noexcept  // (4)
{
    return nonstd::sv_lite::wstring_view{ str, len };
}

#endif // nssv_CONFIG_USR_SV_OPERATOR

}}} // namespace nonstd::literals::string_view_literals

#endif

//
// Extensions for std::string:
//

#if nssv_CONFIG_CONVERSION_STD_STRING_FREE_FUNCTIONS

namespace nonstd {
namespace sv_lite {

// Exclude MSVC 14 (19.00): it yields ambiguous to_string():

#if nssv_CPP11_OR_GREATER && nssv_COMPILER_MSVC_VERSION != 140

template< class CharT, class Traits, class Allocator = std::allocator<CharT> >
std::basic_string<CharT, Traits, Allocator>
to_string( basic_string_view<CharT, Traits> v, Allocator const & a = Allocator() )
{
    return std::basic_string<CharT,Traits, Allocator>( v.begin(), v.end(), a );
}

#else

template< class CharT, class Traits >
std::basic_string<CharT, Traits>
to_string( basic_string_view<CharT, Traits> v )
{
    return std::basic_string<CharT, Traits>( v.begin(), v.end() );
}

template< class CharT, class Traits, class Allocator >
std::basic_string<CharT, Traits, Allocator>
to_string( basic_string_view<CharT, Traits> v, Allocator const & a )
{
    return std::basic_string<CharT, Traits, Allocator>( v.begin(), v.end(), a );
}

#endif // nssv_CPP11_OR_GREATER

template< class CharT, class Traits, class Allocator >
basic_string_view<CharT, Traits>
to_string_view( std::basic_string<CharT, Traits, Allocator> const & s )
{
    return basic_string_view<CharT, Traits>( s.data(), s.size() );
}

}} // namespace nonstd::sv_lite

#endif // nssv_CONFIG_CONVERSION_STD_STRING_FREE_FUNCTIONS

//
// make types and algorithms available in namespace nonstd:
//

namespace nonstd {

using sv_lite::basic_string_view;
using sv_lite::string_view;
using sv_lite::wstring_view;

#if nssv_HAVE_WCHAR16_T
using sv_lite::u16string_view;
#endif
#if nssv_HAVE_WCHAR32_T
using sv_lite::u32string_view;
#endif

// literal "sv"

using sv_lite::operator==;
using sv_lite::operator!=;
using sv_lite::operator<;
using sv_lite::operator<=;
using sv_lite::operator>;
using sv_lite::operator>=;

using sv_lite::operator<<;

#if nssv_CONFIG_CONVERSION_STD_STRING_FREE_FUNCTIONS
using sv_lite::to_string;
using sv_lite::to_string_view;
#endif

} // namespace nonstd

// 24.4.5 Hash support (C++11):

// Note: The hash value of a string view object is equal to the hash value of
// the corresponding string object.

#if nssv_HAVE_STD_HASH

#include <functional>

namespace std {

template<>
struct hash< nonstd::string_view >
{
public:
    std::size_t operator()( nonstd::string_view v ) const nssv_noexcept
    {
        return std::hash<std::string>()( std::string( v.data(), v.size() ) );
    }
};

template<>
struct hash< nonstd::wstring_view >
{
public:
    std::size_t operator()( nonstd::wstring_view v ) const nssv_noexcept
    {
        return std::hash<std::wstring>()( std::wstring( v.data(), v.size() ) );
    }
};

template<>
struct hash< nonstd::u16string_view >
{
public:
    std::size_t operator()( nonstd::u16string_view v ) const nssv_noexcept
    {
        return std::hash<std::u16string>()( std::u16string( v.data(), v.size() ) );
    }
};

template<>
struct hash< nonstd::u32string_view >
{
public:
    std::size_t operator()( nonstd::u32string_view v ) const nssv_noexcept
    {
        return std::hash<std::u32string>()( std::u32string( v.data(), v.size() ) );
    }
};

} // namespace std

#endif // nssv_HAVE_STD_HASH

nssv_RESTORE_WARNINGS()

#endif // nssv_HAVE_STD_STRING_VIEW
#endif // NONSTD_SV_LITE_H_INCLUDED

/* Hedley - https://nemequ.github.io/hedley
 * Created by Evan Nemerson <evan@nemerson.com>
 *
 * To the extent possible under law, the author(s) have dedicated all
 * copyright and related and neighboring rights to this software to
 * the public domain worldwide. This software is distributed without
 * any warranty.
 *
 * For details, see <http://creativecommons.org/publicdomain/zero/1.0/>.
 * SPDX-License-Identifier: CC0-1.0
 */

#if !defined(HEDLEY_VERSION) || (HEDLEY_VERSION < 9)
#if defined(HEDLEY_VERSION)
#  undef HEDLEY_VERSION
#endif
#define HEDLEY_VERSION 9

#if defined(HEDLEY_STRINGIFY_EX)
#  undef HEDLEY_STRINGIFY_EX
#endif
#define HEDLEY_STRINGIFY_EX(x) #x

#if defined(HEDLEY_STRINGIFY)
#  undef HEDLEY_STRINGIFY
#endif
#define HEDLEY_STRINGIFY(x) HEDLEY_STRINGIFY_EX(x)

#if defined(HEDLEY_CONCAT_EX)
#  undef HEDLEY_CONCAT_EX
#endif
#define HEDLEY_CONCAT_EX(a,b) a##b

#if defined(HEDLEY_CONCAT)
#  undef HEDLEY_CONCAT
#endif
#define HEDLEY_CONCAT(a,b) HEDLEY_CONCAT_EX(a,b)

#if defined(HEDLEY_VERSION_ENCODE)
#  undef HEDLEY_VERSION_ENCODE
#endif
#define HEDLEY_VERSION_ENCODE(major,minor,revision) (((major) * 1000000) + ((minor) * 1000) + (revision))

#if defined(HEDLEY_VERSION_DECODE_MAJOR)
#  undef HEDLEY_VERSION_DECODE_MAJOR
#endif
#define HEDLEY_VERSION_DECODE_MAJOR(version) ((version) / 1000000)

#if defined(HEDLEY_VERSION_DECODE_MINOR)
#  undef HEDLEY_VERSION_DECODE_MINOR
#endif
#define HEDLEY_VERSION_DECODE_MINOR(version) (((version) % 1000000) / 1000)

#if defined(HEDLEY_VERSION_DECODE_REVISION)
#  undef HEDLEY_VERSION_DECODE_REVISION
#endif
#define HEDLEY_VERSION_DECODE_REVISION(version) ((version) % 1000)

#if defined(HEDLEY_GNUC_VERSION)
#  undef HEDLEY_GNUC_VERSION
#endif
#if defined(__GNUC__) && defined(__GNUC_PATCHLEVEL__)
#  define HEDLEY_GNUC_VERSION HEDLEY_VERSION_ENCODE(__GNUC__, __GNUC_MINOR__, __GNUC_PATCHLEVEL__)
#elif defined(__GNUC__)
#  define HEDLEY_GNUC_VERSION HEDLEY_VERSION_ENCODE(__GNUC__, __GNUC_MINOR__, 0)
#endif

#if defined(HEDLEY_GNUC_VERSION_CHECK)
#  undef HEDLEY_GNUC_VERSION_CHECK
#endif
#if defined(HEDLEY_GNUC_VERSION)
#  define HEDLEY_GNUC_VERSION_CHECK(major,minor,patch) (HEDLEY_GNUC_VERSION >= HEDLEY_VERSION_ENCODE(major, minor, patch))
#else
#  define HEDLEY_GNUC_VERSION_CHECK(major,minor,patch) (0)
#endif

#if defined(HEDLEY_MSVC_VERSION)
#  undef HEDLEY_MSVC_VERSION
#endif
#if defined(_MSC_FULL_VER) && (_MSC_FULL_VER >= 140000000)
#  define HEDLEY_MSVC_VERSION HEDLEY_VERSION_ENCODE(_MSC_FULL_VER / 10000000, (_MSC_FULL_VER % 10000000) / 100000, (_MSC_FULL_VER % 100000) / 100)
#elif defined(_MSC_FULL_VER)
#  define HEDLEY_MSVC_VERSION HEDLEY_VERSION_ENCODE(_MSC_FULL_VER / 1000000, (_MSC_FULL_VER % 1000000) / 10000, (_MSC_FULL_VER % 10000) / 10)
#elif defined(_MSC_VER)
#  define HEDLEY_MSVC_VERSION HEDLEY_VERSION_ENCODE(_MSC_VER / 100, _MSC_VER % 100, 0)
#endif

#if defined(HEDLEY_MSVC_VERSION_CHECK)
#  undef HEDLEY_MSVC_VERSION_CHECK
#endif
#if !defined(_MSC_VER)
#  define HEDLEY_MSVC_VERSION_CHECK(major,minor,patch) (0)
#elif defined(_MSC_VER) && (_MSC_VER >= 1400)
#  define HEDLEY_MSVC_VERSION_CHECK(major,minor,patch) (_MSC_FULL_VER >= ((major * 10000000) + (minor * 100000) + (patch)))
#elif defined(_MSC_VER) && (_MSC_VER >= 1200)
#  define HEDLEY_MSVC_VERSION_CHECK(major,minor,patch) (_MSC_FULL_VER >= ((major * 1000000) + (minor * 10000) + (patch)))
#else
#  define HEDLEY_MSVC_VERSION_CHECK(major,minor,patch) (_MSC_VER >= ((major * 100) + (minor)))
#endif

#if defined(HEDLEY_INTEL_VERSION)
#  undef HEDLEY_INTEL_VERSION
#endif
#if defined(__INTEL_COMPILER) && defined(__INTEL_COMPILER_UPDATE)
#  define HEDLEY_INTEL_VERSION HEDLEY_VERSION_ENCODE(__INTEL_COMPILER / 100, __INTEL_COMPILER % 100, __INTEL_COMPILER_UPDATE)
#elif defined(__INTEL_COMPILER)
#  define HEDLEY_INTEL_VERSION HEDLEY_VERSION_ENCODE(__INTEL_COMPILER / 100, __INTEL_COMPILER % 100, 0)
#endif

#if defined(HEDLEY_INTEL_VERSION_CHECK)
#  undef HEDLEY_INTEL_VERSION_CHECK
#endif
#if defined(HEDLEY_INTEL_VERSION)
#  define HEDLEY_INTEL_VERSION_CHECK(major,minor,patch) (HEDLEY_INTEL_VERSION >= HEDLEY_VERSION_ENCODE(major, minor, patch))
#else
#  define HEDLEY_INTEL_VERSION_CHECK(major,minor,patch) (0)
#endif

#if defined(HEDLEY_PGI_VERSION)
#  undef HEDLEY_PGI_VERSION
#endif
#if defined(__PGI) && defined(__PGIC__) && defined(__PGIC_MINOR__) && defined(__PGIC_PATCHLEVEL__)
#  define HEDLEY_PGI_VERSION HEDLEY_VERSION_ENCODE(__PGIC__, __PGIC_MINOR__, __PGIC_PATCHLEVEL__)
#endif

#if defined(HEDLEY_PGI_VERSION_CHECK)
#  undef HEDLEY_PGI_VERSION_CHECK
#endif
#if defined(HEDLEY_PGI_VERSION)
#  define HEDLEY_PGI_VERSION_CHECK(major,minor,patch) (HEDLEY_PGI_VERSION >= HEDLEY_VERSION_ENCODE(major, minor, patch))
#else
#  define HEDLEY_PGI_VERSION_CHECK(major,minor,patch) (0)
#endif

#if defined(HEDLEY_SUNPRO_VERSION)
#  undef HEDLEY_SUNPRO_VERSION
#endif
#if defined(__SUNPRO_C) && (__SUNPRO_C > 0x1000)
#  define HEDLEY_SUNPRO_VERSION HEDLEY_VERSION_ENCODE((((__SUNPRO_C >> 16) & 0xf) * 10) + ((__SUNPRO_C >> 12) & 0xf), (((__SUNPRO_C >> 8) & 0xf) * 10) + ((__SUNPRO_C >> 4) & 0xf), (__SUNPRO_C & 0xf) * 10)
#elif defined(__SUNPRO_C)
#  define HEDLEY_SUNPRO_VERSION HEDLEY_VERSION_ENCODE((__SUNPRO_C >> 8) & 0xf, (__SUNPRO_C >> 4) & 0xf, (__SUNPRO_C) & 0xf)
#elif defined(__SUNPRO_CC) && (__SUNPRO_CC > 0x1000)
#  define HEDLEY_SUNPRO_VERSION HEDLEY_VERSION_ENCODE((((__SUNPRO_CC >> 16) & 0xf) * 10) + ((__SUNPRO_CC >> 12) & 0xf), (((__SUNPRO_CC >> 8) & 0xf) * 10) + ((__SUNPRO_CC >> 4) & 0xf), (__SUNPRO_CC & 0xf) * 10)
#elif defined(__SUNPRO_CC)
#  define HEDLEY_SUNPRO_VERSION HEDLEY_VERSION_ENCODE((__SUNPRO_CC >> 8) & 0xf, (__SUNPRO_CC >> 4) & 0xf, (__SUNPRO_CC) & 0xf)
#endif

#if defined(HEDLEY_SUNPRO_VERSION_CHECK)
#  undef HEDLEY_SUNPRO_VERSION_CHECK
#endif
#if defined(HEDLEY_SUNPRO_VERSION)
#  define HEDLEY_SUNPRO_VERSION_CHECK(major,minor,patch) (HEDLEY_SUNPRO_VERSION >= HEDLEY_VERSION_ENCODE(major, minor, patch))
#else
#  define HEDLEY_SUNPRO_VERSION_CHECK(major,minor,patch) (0)
#endif

#if defined(HEDLEY_EMSCRIPTEN_VERSION)
#  undef HEDLEY_EMSCRIPTEN_VERSION
#endif
#if defined(__EMSCRIPTEN__)
#  define HEDLEY_EMSCRIPTEN_VERSION HEDLEY_VERSION_ENCODE(__EMSCRIPTEN_major__, __EMSCRIPTEN_minor__, __EMSCRIPTEN_tiny__)
#endif

#if defined(HEDLEY_EMSCRIPTEN_VERSION_CHECK)
#  undef HEDLEY_EMSCRIPTEN_VERSION_CHECK
#endif
#if defined(HEDLEY_EMSCRIPTEN_VERSION)
#  define HEDLEY_EMSCRIPTEN_VERSION_CHECK(major,minor,patch) (HEDLEY_EMSCRIPTEN_VERSION >= HEDLEY_VERSION_ENCODE(major, minor, patch))
#else
#  define HEDLEY_EMSCRIPTEN_VERSION_CHECK(major,minor,patch) (0)
#endif

#if defined(HEDLEY_ARM_VERSION)
#  undef HEDLEY_ARM_VERSION
#endif
#if defined(__CC_ARM) && defined(__ARMCOMPILER_VERSION)
#  define HEDLEY_ARM_VERSION HEDLEY_VERSION_ENCODE(__ARMCOMPILER_VERSION / 1000000, (__ARMCOMPILER_VERSION % 1000000) / 10000, (__ARMCOMPILER_VERSION % 10000) / 100)
#elif defined(__CC_ARM) && defined(__ARMCC_VERSION)
#  define HEDLEY_ARM_VERSION HEDLEY_VERSION_ENCODE(__ARMCC_VERSION / 1000000, (__ARMCC_VERSION % 1000000) / 10000, (__ARMCC_VERSION % 10000) / 100)
#endif

#if defined(HEDLEY_ARM_VERSION_CHECK)
#  undef HEDLEY_ARM_VERSION_CHECK
#endif
#if defined(HEDLEY_ARM_VERSION)
#  define HEDLEY_ARM_VERSION_CHECK(major,minor,patch) (HEDLEY_ARM_VERSION >= HEDLEY_VERSION_ENCODE(major, minor, patch))
#else
#  define HEDLEY_ARM_VERSION_CHECK(major,minor,patch) (0)
#endif

#if defined(HEDLEY_IBM_VERSION)
#  undef HEDLEY_IBM_VERSION
#endif
#if defined(__ibmxl__)
#  define HEDLEY_IBM_VERSION HEDLEY_VERSION_ENCODE(__ibmxl_version__, __ibmxl_release__, __ibmxl_modification__)
#elif defined(__xlC__) && defined(__xlC_ver__)
#  define HEDLEY_IBM_VERSION HEDLEY_VERSION_ENCODE(__xlC__ >> 8, __xlC__ & 0xff, (__xlC_ver__ >> 8) & 0xff)
#elif defined(__xlC__)
#  define HEDLEY_IBM_VERSION HEDLEY_VERSION_ENCODE(__xlC__ >> 8, __xlC__ & 0xff, 0)
#endif

#if defined(HEDLEY_IBM_VERSION_CHECK)
#  undef HEDLEY_IBM_VERSION_CHECK
#endif
#if defined(HEDLEY_IBM_VERSION)
#  define HEDLEY_IBM_VERSION_CHECK(major,minor,patch) (HEDLEY_IBM_VERSION >= HEDLEY_VERSION_ENCODE(major, minor, patch))
#else
#  define HEDLEY_IBM_VERSION_CHECK(major,minor,patch) (0)
#endif

#if defined(HEDLEY_TI_VERSION)
#  undef HEDLEY_TI_VERSION
#endif
#if defined(__TI_COMPILER_VERSION__)
#  define HEDLEY_TI_VERSION HEDLEY_VERSION_ENCODE(__TI_COMPILER_VERSION__ / 1000000, (__TI_COMPILER_VERSION__ % 1000000) / 1000, (__TI_COMPILER_VERSION__ % 1000))
#endif

#if defined(HEDLEY_TI_VERSION_CHECK)
#  undef HEDLEY_TI_VERSION_CHECK
#endif
#if defined(HEDLEY_TI_VERSION)
#  define HEDLEY_TI_VERSION_CHECK(major,minor,patch) (HEDLEY_TI_VERSION >= HEDLEY_VERSION_ENCODE(major, minor, patch))
#else
#  define HEDLEY_TI_VERSION_CHECK(major,minor,patch) (0)
#endif

#if defined(HEDLEY_CRAY_VERSION)
#  undef HEDLEY_CRAY_VERSION
#endif
#if defined(_CRAYC)
#  if defined(_RELEASE_PATCHLEVEL)
#    define HEDLEY_CRAY_VERSION HEDLEY_VERSION_ENCODE(_RELEASE_MAJOR, _RELEASE_MINOR, _RELEASE_PATCHLEVEL)
#  else
#    define HEDLEY_CRAY_VERSION HEDLEY_VERSION_ENCODE(_RELEASE_MAJOR, _RELEASE_MINOR, 0)
#  endif
#endif

#if defined(HEDLEY_CRAY_VERSION_CHECK)
#  undef HEDLEY_CRAY_VERSION_CHECK
#endif
#if defined(HEDLEY_CRAY_VERSION)
#  define HEDLEY_CRAY_VERSION_CHECK(major,minor,patch) (HEDLEY_CRAY_VERSION >= HEDLEY_VERSION_ENCODE(major, minor, patch))
#else
#  define HEDLEY_CRAY_VERSION_CHECK(major,minor,patch) (0)
#endif

#if defined(HEDLEY_IAR_VERSION)
#  undef HEDLEY_IAR_VERSION
#endif
#if defined(__IAR_SYSTEMS_ICC__)
#  if __VER__ > 1000
#    define HEDLEY_IAR_VERSION HEDLEY_VERSION_ENCODE((__VER__ / 1000000), ((__VER__ / 1000) % 1000), (__VER__ % 1000))
#  else
#    define HEDLEY_IAR_VERSION HEDLEY_VERSION_ENCODE(VER / 100, __VER__ % 100, 0)
#  endif
#endif

#if defined(HEDLEY_IAR_VERSION_CHECK)
#  undef HEDLEY_IAR_VERSION_CHECK
#endif
#if defined(HEDLEY_IAR_VERSION)
#  define HEDLEY_IAR_VERSION_CHECK(major,minor,patch) (HEDLEY_IAR_VERSION >= HEDLEY_VERSION_ENCODE(major, minor, patch))
#else
#  define HEDLEY_IAR_VERSION_CHECK(major,minor,patch) (0)
#endif

#if defined(HEDLEY_TINYC_VERSION)
#  undef HEDLEY_TINYC_VERSION
#endif
#if defined(__TINYC__)
#  define HEDLEY_TINYC_VERSION HEDLEY_VERSION_ENCODE(__TINYC__ / 1000, (__TINYC__ / 100) % 10, __TINYC__ % 100)
#endif

#if defined(HEDLEY_TINYC_VERSION_CHECK)
#  undef HEDLEY_TINYC_VERSION_CHECK
#endif
#if defined(HEDLEY_TINYC_VERSION)
#  define HEDLEY_TINYC_VERSION_CHECK(major,minor,patch) (HEDLEY_TINYC_VERSION >= HEDLEY_VERSION_ENCODE(major, minor, patch))
#else
#  define HEDLEY_TINYC_VERSION_CHECK(major,minor,patch) (0)
#endif

#if defined(HEDLEY_DMC_VERSION)
#  undef HEDLEY_DMC_VERSION
#endif
#if defined(__DMC__)
#  define HEDLEY_DMC_VERSION HEDLEY_VERSION_ENCODE(__DMC__ >> 8, (__DMC__ >> 4) & 0xf, __DMC__ & 0xf)
#endif

#if defined(HEDLEY_DMC_VERSION_CHECK)
#  undef HEDLEY_DMC_VERSION_CHECK
#endif
#if defined(HEDLEY_DMC_VERSION)
#  define HEDLEY_DMC_VERSION_CHECK(major,minor,patch) (HEDLEY_DMC_VERSION >= HEDLEY_VERSION_ENCODE(major, minor, patch))
#else
#  define HEDLEY_DMC_VERSION_CHECK(major,minor,patch) (0)
#endif

#if defined(HEDLEY_COMPCERT_VERSION)
#  undef HEDLEY_COMPCERT_VERSION
#endif
#if defined(__COMPCERT_VERSION__)
#  define HEDLEY_COMPCERT_VERSION HEDLEY_VERSION_ENCODE(__COMPCERT_VERSION__ / 10000, (__COMPCERT_VERSION__ / 100) % 100, __COMPCERT_VERSION__ % 100)
#endif

#if defined(HEDLEY_COMPCERT_VERSION_CHECK)
#  undef HEDLEY_COMPCERT_VERSION_CHECK
#endif
#if defined(HEDLEY_COMPCERT_VERSION)
#  define HEDLEY_COMPCERT_VERSION_CHECK(major,minor,patch) (HEDLEY_COMPCERT_VERSION >= HEDLEY_VERSION_ENCODE(major, minor, patch))
#else
#  define HEDLEY_COMPCERT_VERSION_CHECK(major,minor,patch) (0)
#endif

#if defined(HEDLEY_PELLES_VERSION)
#  undef HEDLEY_PELLES_VERSION
#endif
#if defined(__POCC__)
#  define HEDLEY_PELLES_VERSION HEDLEY_VERSION_ENCODE(__POCC__ / 100, __POCC__ % 100, 0)
#endif

#if defined(HEDLEY_PELLES_VERSION_CHECK)
#  undef HEDLEY_PELLES_VERSION_CHECK
#endif
#if defined(HEDLEY_PELLES_VERSION)
#  define HEDLEY_PELLES_VERSION_CHECK(major,minor,patch) (HEDLEY_PELLES_VERSION >= HEDLEY_VERSION_ENCODE(major, minor, patch))
#else
#  define HEDLEY_PELLES_VERSION_CHECK(major,minor,patch) (0)
#endif

#if defined(HEDLEY_GCC_VERSION)
#  undef HEDLEY_GCC_VERSION
#endif
#if \
  defined(HEDLEY_GNUC_VERSION) && \
  !defined(__clang__) && \
  !defined(HEDLEY_INTEL_VERSION) && \
  !defined(HEDLEY_PGI_VERSION) && \
  !defined(HEDLEY_ARM_VERSION) && \
  !defined(HEDLEY_TI_VERSION) && \
  !defined(__COMPCERT__)
#  define HEDLEY_GCC_VERSION HEDLEY_GNUC_VERSION
#endif

#if defined(HEDLEY_GCC_VERSION_CHECK)
#  undef HEDLEY_GCC_VERSION_CHECK
#endif
#if defined(HEDLEY_GCC_VERSION)
#  define HEDLEY_GCC_VERSION_CHECK(major,minor,patch) (HEDLEY_GCC_VERSION >= HEDLEY_VERSION_ENCODE(major, minor, patch))
#else
#  define HEDLEY_GCC_VERSION_CHECK(major,minor,patch) (0)
#endif

#if defined(HEDLEY_HAS_ATTRIBUTE)
#  undef HEDLEY_HAS_ATTRIBUTE
#endif
#if defined(__has_attribute)
#  define HEDLEY_HAS_ATTRIBUTE(attribute) __has_attribute(attribute)
#else
#  define HEDLEY_HAS_ATTRIBUTE(attribute) (0)
#endif

#if defined(HEDLEY_GNUC_HAS_ATTRIBUTE)
#  undef HEDLEY_GNUC_HAS_ATTRIBUTE
#endif
#if defined(__has_attribute)
#  define HEDLEY_GNUC_HAS_ATTRIBUTE(attribute,major,minor,patch) __has_attribute(attribute)
#else
#  define HEDLEY_GNUC_HAS_ATTRIBUTE(attribute,major,minor,patch) HEDLEY_GNUC_VERSION_CHECK(major,minor,patch)
#endif

#if defined(HEDLEY_GCC_HAS_ATTRIBUTE)
#  undef HEDLEY_GCC_HAS_ATTRIBUTE
#endif
#if defined(__has_attribute)
#  define HEDLEY_GCC_HAS_ATTRIBUTE(attribute,major,minor,patch) __has_attribute(attribute)
#else
#  define HEDLEY_GCC_HAS_ATTRIBUTE(attribute,major,minor,patch) HEDLEY_GCC_VERSION_CHECK(major,minor,patch)
#endif

#if defined(HEDLEY_HAS_CPP_ATTRIBUTE)
#  undef HEDLEY_HAS_CPP_ATTRIBUTE
#endif
#if defined(__has_cpp_attribute) && defined(__cplusplus)
#  define HEDLEY_HAS_CPP_ATTRIBUTE(attribute) __has_cpp_attribute(attribute)
#else
#  define HEDLEY_HAS_CPP_ATTRIBUTE(attribute) (0)
#endif

#if defined(HEDLEY_GNUC_HAS_CPP_ATTRIBUTE)
#  undef HEDLEY_GNUC_HAS_CPP_ATTRIBUTE
#endif
#if defined(__has_cpp_attribute) && defined(__cplusplus)
#  define HEDLEY_GNUC_HAS_CPP_ATTRIBUTE(attribute,major,minor,patch) __has_cpp_attribute(attribute)
#else
#  define HEDLEY_GNUC_HAS_CPP_ATTRIBUTE(attribute,major,minor,patch) HEDLEY_GNUC_VERSION_CHECK(major,minor,patch)
#endif

#if defined(HEDLEY_GCC_HAS_CPP_ATTRIBUTE)
#  undef HEDLEY_GCC_HAS_CPP_ATTRIBUTE
#endif
#if defined(__has_cpp_attribute) && defined(__cplusplus)
#  define HEDLEY_GCC_HAS_CPP_ATTRIBUTE(attribute,major,minor,patch) __has_cpp_attribute(attribute)
#else
#  define HEDLEY_GCC_HAS_CPP_ATTRIBUTE(attribute,major,minor,patch) HEDLEY_GCC_VERSION_CHECK(major,minor,patch)
#endif

#if defined(HEDLEY_HAS_BUILTIN)
#  undef HEDLEY_HAS_BUILTIN
#endif
#if defined(__has_builtin)
#  define HEDLEY_HAS_BUILTIN(builtin) __has_builtin(builtin)
#else
#  define HEDLEY_HAS_BUILTIN(builtin) (0)
#endif

#if defined(HEDLEY_GNUC_HAS_BUILTIN)
#  undef HEDLEY_GNUC_HAS_BUILTIN
#endif
#if defined(__has_builtin)
#  define HEDLEY_GNUC_HAS_BUILTIN(builtin,major,minor,patch) __has_builtin(builtin)
#else
#  define HEDLEY_GNUC_HAS_BUILTIN(builtin,major,minor,patch) HEDLEY_GNUC_VERSION_CHECK(major,minor,patch)
#endif

#if defined(HEDLEY_GCC_HAS_BUILTIN)
#  undef HEDLEY_GCC_HAS_BUILTIN
#endif
#if defined(__has_builtin)
#  define HEDLEY_GCC_HAS_BUILTIN(builtin,major,minor,patch) __has_builtin(builtin)
#else
#  define HEDLEY_GCC_HAS_BUILTIN(builtin,major,minor,patch) HEDLEY_GCC_VERSION_CHECK(major,minor,patch)
#endif

#if defined(HEDLEY_HAS_FEATURE)
#  undef HEDLEY_HAS_FEATURE
#endif
#if defined(__has_feature)
#  define HEDLEY_HAS_FEATURE(feature) __has_feature(feature)
#else
#  define HEDLEY_HAS_FEATURE(feature) (0)
#endif

#if defined(HEDLEY_GNUC_HAS_FEATURE)
#  undef HEDLEY_GNUC_HAS_FEATURE
#endif
#if defined(__has_feature)
#  define HEDLEY_GNUC_HAS_FEATURE(feature,major,minor,patch) __has_feature(feature)
#else
#  define HEDLEY_GNUC_HAS_FEATURE(feature,major,minor,patch) HEDLEY_GNUC_VERSION_CHECK(major,minor,patch)
#endif

#if defined(HEDLEY_GCC_HAS_FEATURE)
#  undef HEDLEY_GCC_HAS_FEATURE
#endif
#if defined(__has_feature)
#  define HEDLEY_GCC_HAS_FEATURE(feature,major,minor,patch) __has_feature(feature)
#else
#  define HEDLEY_GCC_HAS_FEATURE(feature,major,minor,patch) HEDLEY_GCC_VERSION_CHECK(major,minor,patch)
#endif

#if defined(HEDLEY_HAS_EXTENSION)
#  undef HEDLEY_HAS_EXTENSION
#endif
#if defined(__has_extension)
#  define HEDLEY_HAS_EXTENSION(extension) __has_extension(extension)
#else
#  define HEDLEY_HAS_EXTENSION(extension) (0)
#endif

#if defined(HEDLEY_GNUC_HAS_EXTENSION)
#  undef HEDLEY_GNUC_HAS_EXTENSION
#endif
#if defined(__has_extension)
#  define HEDLEY_GNUC_HAS_EXTENSION(extension,major,minor,patch) __has_extension(extension)
#else
#  define HEDLEY_GNUC_HAS_EXTENSION(extension,major,minor,patch) HEDLEY_GNUC_VERSION_CHECK(major,minor,patch)
#endif

#if defined(HEDLEY_GCC_HAS_EXTENSION)
#  undef HEDLEY_GCC_HAS_EXTENSION
#endif
#if defined(__has_extension)
#  define HEDLEY_GCC_HAS_EXTENSION(extension,major,minor,patch) __has_extension(extension)
#else
#  define HEDLEY_GCC_HAS_EXTENSION(extension,major,minor,patch) HEDLEY_GCC_VERSION_CHECK(major,minor,patch)
#endif

#if defined(HEDLEY_HAS_DECLSPEC_ATTRIBUTE)
#  undef HEDLEY_HAS_DECLSPEC_ATTRIBUTE
#endif
#if defined(__has_declspec_attribute)
#  define HEDLEY_HAS_DECLSPEC_ATTRIBUTE(attribute) __has_declspec_attribute(attribute)
#else
#  define HEDLEY_HAS_DECLSPEC_ATTRIBUTE(attribute) (0)
#endif

#if defined(HEDLEY_GNUC_HAS_DECLSPEC_ATTRIBUTE)
#  undef HEDLEY_GNUC_HAS_DECLSPEC_ATTRIBUTE
#endif
#if defined(__has_declspec_attribute)
#  define HEDLEY_GNUC_HAS_DECLSPEC_ATTRIBUTE(attribute,major,minor,patch) __has_declspec_attribute(attribute)
#else
#  define HEDLEY_GNUC_HAS_DECLSPEC_ATTRIBUTE(attribute,major,minor,patch) HEDLEY_GNUC_VERSION_CHECK(major,minor,patch)
#endif

#if defined(HEDLEY_GCC_HAS_DECLSPEC_ATTRIBUTE)
#  undef HEDLEY_GCC_HAS_DECLSPEC_ATTRIBUTE
#endif
#if defined(__has_declspec_attribute)
#  define HEDLEY_GCC_HAS_DECLSPEC_ATTRIBUTE(attribute,major,minor,patch) __has_declspec_attribute(attribute)
#else
#  define HEDLEY_GCC_HAS_DECLSPEC_ATTRIBUTE(attribute,major,minor,patch) HEDLEY_GCC_VERSION_CHECK(major,minor,patch)
#endif

#if defined(HEDLEY_HAS_WARNING)
#  undef HEDLEY_HAS_WARNING
#endif
#if defined(__has_warning)
#  define HEDLEY_HAS_WARNING(warning) __has_warning(warning)
#else
#  define HEDLEY_HAS_WARNING(warning) (0)
#endif

#if defined(HEDLEY_GNUC_HAS_WARNING)
#  undef HEDLEY_GNUC_HAS_WARNING
#endif
#if defined(__has_warning)
#  define HEDLEY_GNUC_HAS_WARNING(warning,major,minor,patch) __has_warning(warning)
#else
#  define HEDLEY_GNUC_HAS_WARNING(warning,major,minor,patch) HEDLEY_GNUC_VERSION_CHECK(major,minor,patch)
#endif

#if defined(HEDLEY_GCC_HAS_WARNING)
#  undef HEDLEY_GCC_HAS_WARNING
#endif
#if defined(__has_warning)
#  define HEDLEY_GCC_HAS_WARNING(warning,major,minor,patch) __has_warning(warning)
#else
#  define HEDLEY_GCC_HAS_WARNING(warning,major,minor,patch) HEDLEY_GCC_VERSION_CHECK(major,minor,patch)
#endif

#if \
  (defined(__STDC_VERSION__) && (__STDC_VERSION__ >= 199901L)) || \
  defined(__clang__) || \
  HEDLEY_GCC_VERSION_CHECK(3,0,0) || \
  HEDLEY_INTEL_VERSION_CHECK(13,0,0) || \
  HEDLEY_IAR_VERSION_CHECK(8,0,0) || \
  HEDLEY_PGI_VERSION_CHECK(18,4,0) || \
  HEDLEY_ARM_VERSION_CHECK(4,1,0) || \
  HEDLEY_TI_VERSION_CHECK(6,0,0) || \
  HEDLEY_CRAY_VERSION_CHECK(5,0,0) || \
  HEDLEY_TINYC_VERSION_CHECK(0,9,17) || \
  HEDLEY_SUNPRO_VERSION_CHECK(8,0,0) || \
  (HEDLEY_IBM_VERSION_CHECK(10,1,0) && defined(__C99_PRAGMA_OPERATOR))
#  define HEDLEY_PRAGMA(value) _Pragma(#value)
#elif HEDLEY_MSVC_VERSION_CHECK(15,0,0)
#  define HEDLEY_PRAGMA(value) __pragma(value)
#else
#  define HEDLEY_PRAGMA(value)
#endif

#if defined(HEDLEY_DIAGNOSTIC_PUSH)
#  undef HEDLEY_DIAGNOSTIC_PUSH
#endif
#if defined(HEDLEY_DIAGNOSTIC_POP)
#  undef HEDLEY_DIAGNOSTIC_POP
#endif
#if defined(__clang__)
#  define HEDLEY_DIAGNOSTIC_PUSH _Pragma("clang diagnostic push")
#  define HEDLEY_DIAGNOSTIC_POP _Pragma("clang diagnostic pop")
#elif HEDLEY_INTEL_VERSION_CHECK(13,0,0)
#  define HEDLEY_DIAGNOSTIC_PUSH _Pragma("warning(push)")
#  define HEDLEY_DIAGNOSTIC_POP _Pragma("warning(pop)")
#elif HEDLEY_GCC_VERSION_CHECK(4,6,0)
#  define HEDLEY_DIAGNOSTIC_PUSH _Pragma("GCC diagnostic push")
#  define HEDLEY_DIAGNOSTIC_POP _Pragma("GCC diagnostic pop")
#elif HEDLEY_MSVC_VERSION_CHECK(15,0,0)
#  define HEDLEY_DIAGNOSTIC_PUSH __pragma(warning(push))
#  define HEDLEY_DIAGNOSTIC_POP __pragma(warning(pop))
#elif HEDLEY_ARM_VERSION_CHECK(5,6,0)
#  define HEDLEY_DIAGNOSTIC_PUSH _Pragma("push")
#  define HEDLEY_DIAGNOSTIC_POP _Pragma("pop")
#elif HEDLEY_TI_VERSION_CHECK(8,1,0)
#  define HEDLEY_DIAGNOSTIC_PUSH _Pragma("diag_push")
#  define HEDLEY_DIAGNOSTIC_POP _Pragma("diag_pop")
#elif HEDLEY_PELLES_VERSION_CHECK(2,90,0)
#  define HEDLEY_DIAGNOSTIC_PUSH _Pragma("warning(push)")
#  define HEDLEY_DIAGNOSTIC_POP _Pragma("warning(pop)")
#else
#  define HEDLEY_DIAGNOSTIC_PUSH
#  define HEDLEY_DIAGNOSTIC_POP
#endif

#if defined(HEDLEY_DIAGNOSTIC_DISABLE_DEPRECATED)
#  undef HEDLEY_DIAGNOSTIC_DISABLE_DEPRECATED
#endif
#if HEDLEY_HAS_WARNING("-Wdeprecated-declarations")
#  define HEDLEY_DIAGNOSTIC_DISABLE_DEPRECATED _Pragma("clang diagnostic ignored \"-Wdeprecated-declarations\"")
#elif HEDLEY_INTEL_VERSION_CHECK(13,0,0)
#  define HEDLEY_DIAGNOSTIC_DISABLE_DEPRECATED _Pragma("warning(disable:1478 1786)")
#elif HEDLEY_PGI_VERSION_CHECK(17,10,0)
#  define HEDLEY_DIAGNOSTIC_DISABLE_DEPRECATED _Pragma("diag_suppress 1215,1444")
#elif HEDLEY_GCC_VERSION_CHECK(4,3,0)
#  define HEDLEY_DIAGNOSTIC_DISABLE_DEPRECATED _Pragma("GCC diagnostic ignored \"-Wdeprecated-declarations\"")
#elif HEDLEY_MSVC_VERSION_CHECK(15,0,0)
#  define HEDLEY_DIAGNOSTIC_DISABLE_DEPRECATED __pragma(warning(disable:4996))
#elif HEDLEY_TI_VERSION_CHECK(8,0,0)
#  define HEDLEY_DIAGNOSTIC_DISABLE_DEPRECATED _Pragma("diag_suppress 1291,1718")
#elif HEDLEY_SUNPRO_VERSION_CHECK(5,13,0) && !defined(__cplusplus)
#  define HEDLEY_DIAGNOSTIC_DISABLE_DEPRECATED _Pragma("error_messages(off,E_DEPRECATED_ATT,E_DEPRECATED_ATT_MESS)")
#elif HEDLEY_SUNPRO_VERSION_CHECK(5,13,0) && defined(__cplusplus)
#  define HEDLEY_DIAGNOSTIC_DISABLE_DEPRECATED _Pragma("error_messages(off,symdeprecated,symdeprecated2)")
#elif HEDLEY_IAR_VERSION_CHECK(8,0,0)
#  define HEDLEY_DIAGNOSTIC_DISABLE_DEPRECATED _Pragma("diag_suppress=Pe1444,Pe1215")
#elif HEDLEY_PELLES_VERSION_CHECK(2,90,0)
#  define HEDLEY_DIAGNOSTIC_DISABLE_DEPRECATED _Pragma("warn(disable:2241)")
#else
#  define HEDLEY_DIAGNOSTIC_DISABLE_DEPRECATED
#endif

#if defined(HEDLEY_DIAGNOSTIC_DISABLE_UNKNOWN_PRAGMAS)
#  undef HEDLEY_DIAGNOSTIC_DISABLE_UNKNOWN_PRAGMAS
#endif
#if HEDLEY_HAS_WARNING("-Wunknown-pragmas")
#  define HEDLEY_DIAGNOSTIC_DISABLE_UNKNOWN_PRAGMAS _Pragma("clang diagnostic ignored \"-Wunknown-pragmas\"")
#elif HEDLEY_INTEL_VERSION_CHECK(13,0,0)
#  define HEDLEY_DIAGNOSTIC_DISABLE_UNKNOWN_PRAGMAS _Pragma("warning(disable:161)")
#elif HEDLEY_PGI_VERSION_CHECK(17,10,0)
#  define HEDLEY_DIAGNOSTIC_DISABLE_UNKNOWN_PRAGMAS _Pragma("diag_suppress 1675")
#elif HEDLEY_GCC_VERSION_CHECK(4,3,0)
#  define HEDLEY_DIAGNOSTIC_DISABLE_UNKNOWN_PRAGMAS _Pragma("GCC diagnostic ignored \"-Wunknown-pragmas\"")
#elif HEDLEY_MSVC_VERSION_CHECK(15,0,0)
#  define HEDLEY_DIAGNOSTIC_DISABLE_UNKNOWN_PRAGMAS __pragma(warning(disable:4068))
#elif HEDLEY_TI_VERSION_CHECK(8,0,0)
#  define HEDLEY_DIAGNOSTIC_DISABLE_UNKNOWN_PRAGMAS _Pragma("diag_suppress 163")
#elif HEDLEY_IAR_VERSION_CHECK(8,0,0)
#  define HEDLEY_DIAGNOSTIC_DISABLE_UNKNOWN_PRAGMAS _Pragma("diag_suppress=Pe161")
#else
#  define HEDLEY_DIAGNOSTIC_DISABLE_UNKNOWN_PRAGMAS
#endif

#if defined(HEDLEY_DIAGNOSTIC_DISABLE_CAST_QUAL)
#  undef HEDLEY_DIAGNOSTIC_DISABLE_CAST_QUAL
#endif
#if HEDLEY_HAS_WARNING("-Wcast-qual")
#  define HEDLEY_DIAGNOSTIC_DISABLE_CAST_QUAL _Pragma("clang diagnostic ignored \"-Wcast-qual\"")
#elif HEDLEY_INTEL_VERSION_CHECK(13,0,0)
#  define HEDLEY_DIAGNOSTIC_DISABLE_CAST_QUAL _Pragma("warning(disable:2203 2331)")
#elif HEDLEY_GCC_VERSION_CHECK(3,0,0)
#  define HEDLEY_DIAGNOSTIC_DISABLE_CAST_QUAL _Pragma("GCC diagnostic ignored \"-Wcast-qual\"")
#else
#  define HEDLEY_DIAGNOSTIC_DISABLE_CAST_QUAL
#endif

#if defined(HEDLEY_DEPRECATED)
#  undef HEDLEY_DEPRECATED
#endif
#if defined(HEDLEY_DEPRECATED_FOR)
#  undef HEDLEY_DEPRECATED_FOR
#endif
#if defined(__cplusplus) && (__cplusplus >= 201402L)
#  define HEDLEY_DEPRECATED(since) [[deprecated("Since " #since)]]
#  define HEDLEY_DEPRECATED_FOR(since, replacement) [[deprecated("Since " #since "; use " #replacement)]]
#elif \
  HEDLEY_HAS_EXTENSION(attribute_deprecated_with_message) || \
  HEDLEY_GCC_VERSION_CHECK(4,5,0) || \
  HEDLEY_INTEL_VERSION_CHECK(13,0,0) || \
  HEDLEY_ARM_VERSION_CHECK(5,6,0) || \
  HEDLEY_SUNPRO_VERSION_CHECK(5,13,0) || \
  HEDLEY_PGI_VERSION_CHECK(17,10,0) || \
  HEDLEY_TI_VERSION_CHECK(8,3,0)
#  define HEDLEY_DEPRECATED(since) __attribute__((__deprecated__("Since " #since)))
#  define HEDLEY_DEPRECATED_FOR(since, replacement) __attribute__((__deprecated__("Since " #since "; use " #replacement)))
#elif \
  HEDLEY_HAS_ATTRIBUTE(deprecated) || \
  HEDLEY_GCC_VERSION_CHECK(3,1,0) || \
  HEDLEY_ARM_VERSION_CHECK(4,1,0) || \
  HEDLEY_TI_VERSION_CHECK(8,0,0) || \
  (HEDLEY_TI_VERSION_CHECK(7,3,0) && defined(__TI_GNU_ATTRIBUTE_SUPPORT__))
#  define HEDLEY_DEPRECATED(since) __attribute__((__deprecated__))
#  define HEDLEY_DEPRECATED_FOR(since, replacement) __attribute__((__deprecated__))
#elif HEDLEY_MSVC_VERSION_CHECK(14,0,0)
#  define HEDLEY_DEPRECATED(since) __declspec(deprecated("Since " # since))
#  define HEDLEY_DEPRECATED_FOR(since, replacement) __declspec(deprecated("Since " #since "; use " #replacement))
#elif \
  HEDLEY_MSVC_VERSION_CHECK(13,10,0) || \
  HEDLEY_PELLES_VERSION_CHECK(6,50,0)
#  define HEDLEY_DEPRECATED(since) _declspec(deprecated)
#  define HEDLEY_DEPRECATED_FOR(since, replacement) __declspec(deprecated)
#elif HEDLEY_IAR_VERSION_CHECK(8,0,0)
#  define HEDLEY_DEPRECATED(since) _Pragma("deprecated")
#  define HEDLEY_DEPRECATED_FOR(since, replacement) _Pragma("deprecated")
#else
#  define HEDLEY_DEPRECATED(since)
#  define HEDLEY_DEPRECATED_FOR(since, replacement)
#endif

#if defined(HEDLEY_UNAVAILABLE)
#  undef HEDLEY_UNAVAILABLE
#endif
#if \
  HEDLEY_HAS_ATTRIBUTE(warning) || \
  HEDLEY_GCC_VERSION_CHECK(4,3,0) || \
  HEDLEY_INTEL_VERSION_CHECK(13,0,0)
#  define HEDLEY_UNAVAILABLE(available_since) __attribute__((__warning__("Not available until " #available_since)))
#else
#  define HEDLEY_UNAVAILABLE(available_since)
#endif

#if defined(HEDLEY_WARN_UNUSED_RESULT)
#  undef HEDLEY_WARN_UNUSED_RESULT
#endif
#if defined(__cplusplus) && (__cplusplus >= 201703L)
#  define HEDLEY_WARN_UNUSED_RESULT [[nodiscard]]
#elif \
  HEDLEY_HAS_ATTRIBUTE(warn_unused_result) || \
  HEDLEY_GCC_VERSION_CHECK(3,4,0) || \
  HEDLEY_INTEL_VERSION_CHECK(13,0,0) || \
  HEDLEY_TI_VERSION_CHECK(8,0,0) || \
  (HEDLEY_TI_VERSION_CHECK(7,3,0) && defined(__TI_GNU_ATTRIBUTE_SUPPORT__)) || \
  (HEDLEY_SUNPRO_VERSION_CHECK(5,15,0) && defined(__cplusplus)) || \
  HEDLEY_PGI_VERSION_CHECK(17,10,0)
#  define HEDLEY_WARN_UNUSED_RESULT __attribute__((__warn_unused_result__))
#elif defined(_Check_return_) /* SAL */
#  define HEDLEY_WARN_UNUSED_RESULT _Check_return_
#else
#  define HEDLEY_WARN_UNUSED_RESULT
#endif

#if defined(HEDLEY_SENTINEL)
#  undef HEDLEY_SENTINEL
#endif
#if \
  HEDLEY_HAS_ATTRIBUTE(sentinel) || \
  HEDLEY_GCC_VERSION_CHECK(4,0,0) || \
  HEDLEY_INTEL_VERSION_CHECK(13,0,0) || \
  HEDLEY_ARM_VERSION_CHECK(5,4,0)
#  define HEDLEY_SENTINEL(position) __attribute__((__sentinel__(position)))
#else
#  define HEDLEY_SENTINEL(position)
#endif

#if defined(HEDLEY_NO_RETURN)
#  undef HEDLEY_NO_RETURN
#endif
#if HEDLEY_IAR_VERSION_CHECK(8,0,0)
#  define HEDLEY_NO_RETURN __noreturn
#elif HEDLEY_INTEL_VERSION_CHECK(13,0,0)
#  define HEDLEY_NO_RETURN __attribute__((__noreturn__))
#elif defined(__STDC_VERSION__) && __STDC_VERSION__ >= 201112L
#  define HEDLEY_NO_RETURN _Noreturn
#elif defined(__cplusplus) && (__cplusplus >= 201103L)
#  define HEDLEY_NO_RETURN [[noreturn]]
#elif \
  HEDLEY_HAS_ATTRIBUTE(noreturn) || \
  HEDLEY_GCC_VERSION_CHECK(3,2,0) || \
  HEDLEY_SUNPRO_VERSION_CHECK(5,11,0) || \
  HEDLEY_ARM_VERSION_CHECK(4,1,0) || \
  HEDLEY_IBM_VERSION_CHECK(10,1,0) || \
  HEDLEY_TI_VERSION_CHECK(18,0,0) || \
  (HEDLEY_TI_VERSION_CHECK(17,3,0) && defined(__TI_GNU_ATTRIBUTE_SUPPORT__))
#  define HEDLEY_NO_RETURN __attribute__((__noreturn__))
#elif HEDLEY_MSVC_VERSION_CHECK(13,10,0)
#  define HEDLEY_NO_RETURN __declspec(noreturn)
#elif HEDLEY_TI_VERSION_CHECK(6,0,0) && defined(__cplusplus)
#  define HEDLEY_NO_RETURN _Pragma("FUNC_NEVER_RETURNS;")
#elif HEDLEY_COMPCERT_VERSION_CHECK(3,2,0)
#  define HEDLEY_NO_RETURN __attribute((noreturn))
#elif HEDLEY_PELLES_VERSION_CHECK(9,0,0)
#  define HEDLEY_NO_RETURN __declspec(noreturn)
#else
#  define HEDLEY_NO_RETURN
#endif

#if defined(HEDLEY_UNREACHABLE)
#  undef HEDLEY_UNREACHABLE
#endif
#if defined(HEDLEY_UNREACHABLE_RETURN)
#  undef HEDLEY_UNREACHABLE_RETURN
#endif
#if \
  (HEDLEY_HAS_BUILTIN(__builtin_unreachable) && (!defined(HEDLEY_ARM_VERSION))) || \
  HEDLEY_GCC_VERSION_CHECK(4,5,0) || \
  HEDLEY_INTEL_VERSION_CHECK(13,0,0) || \
  HEDLEY_IBM_VERSION_CHECK(13,1,5)
#  define HEDLEY_UNREACHABLE() __builtin_unreachable()
#elif HEDLEY_MSVC_VERSION_CHECK(13,10,0)
#  define HEDLEY_UNREACHABLE() __assume(0)
#elif HEDLEY_TI_VERSION_CHECK(6,0,0)
#  if defined(__cplusplus)
#    define HEDLEY_UNREACHABLE() std::_nassert(0)
#  else
#    define HEDLEY_UNREACHABLE() _nassert(0)
#  endif
#  define HEDLEY_UNREACHABLE_RETURN(value) return value
#elif defined(EXIT_FAILURE)
#  define HEDLEY_UNREACHABLE() abort()
#else
#  define HEDLEY_UNREACHABLE()
#  define HEDLEY_UNREACHABLE_RETURN(value) return value
#endif
#if !defined(HEDLEY_UNREACHABLE_RETURN)
#  define HEDLEY_UNREACHABLE_RETURN(value) HEDLEY_UNREACHABLE()
#endif

#if defined(HEDLEY_ASSUME)
#  undef HEDLEY_ASSUME
#endif
#if \
  HEDLEY_MSVC_VERSION_CHECK(13,10,0) || \
  HEDLEY_INTEL_VERSION_CHECK(13,0,0)
#  define HEDLEY_ASSUME(expr) __assume(expr)
#elif HEDLEY_HAS_BUILTIN(__builtin_assume)
#  define HEDLEY_ASSUME(expr) __builtin_assume(expr)
#elif HEDLEY_TI_VERSION_CHECK(6,0,0)
#  if defined(__cplusplus)
#    define HEDLEY_ASSUME(expr) std::_nassert(expr)
#  else
#    define HEDLEY_ASSUME(expr) _nassert(expr)
#  endif
#elif \
  (HEDLEY_HAS_BUILTIN(__builtin_unreachable) && !defined(HEDLEY_ARM_VERSION)) || \
  HEDLEY_GCC_VERSION_CHECK(4,5,0) || \
  HEDLEY_INTEL_VERSION_CHECK(13,0,0) || \
  HEDLEY_IBM_VERSION_CHECK(13,1,5)
#  define HEDLEY_ASSUME(expr) ((void) ((expr) ? 1 : (__builtin_unreachable(), 1)))
#else
#  define HEDLEY_ASSUME(expr) ((void) (expr))
#endif


HEDLEY_DIAGNOSTIC_PUSH
#if \
  HEDLEY_HAS_WARNING("-Wvariadic-macros") || \
  HEDLEY_GCC_VERSION_CHECK(4,0,0)
#  if defined(__clang__)
#    pragma clang diagnostic ignored "-Wvariadic-macros"
#  elif defined(HEDLEY_GCC_VERSION)
#    pragma GCC diagnostic ignored "-Wvariadic-macros"
#  endif
#endif
#if defined(HEDLEY_NON_NULL)
#  undef HEDLEY_NON_NULL
#endif
#if \
  HEDLEY_HAS_ATTRIBUTE(nonnull) || \
  HEDLEY_GCC_VERSION_CHECK(3,3,0) || \
  HEDLEY_INTEL_VERSION_CHECK(13,0,0) || \
  HEDLEY_ARM_VERSION_CHECK(4,1,0)
#  define HEDLEY_NON_NULL(...) __attribute__((__nonnull__(__VA_ARGS__)))
#else
#  define HEDLEY_NON_NULL(...)
#endif
HEDLEY_DIAGNOSTIC_POP

#if defined(HEDLEY_PRINTF_FORMAT)
#  undef HEDLEY_PRINTF_FORMAT
#endif
#if defined(__MINGW32__) && HEDLEY_GCC_HAS_ATTRIBUTE(format,4,4,0) && !defined(__USE_MINGW_ANSI_STDIO)
#  define HEDLEY_PRINTF_FORMAT(string_idx,first_to_check) __attribute__((__format__(ms_printf, string_idx, first_to_check)))
#elif defined(__MINGW32__) && HEDLEY_GCC_HAS_ATTRIBUTE(format,4,4,0) && defined(__USE_MINGW_ANSI_STDIO)
#  define HEDLEY_PRINTF_FORMAT(string_idx,first_to_check) __attribute__((__format__(gnu_printf, string_idx, first_to_check)))
#elif \
  HEDLEY_HAS_ATTRIBUTE(format) || \
  HEDLEY_GCC_VERSION_CHECK(3,1,0) || \
  HEDLEY_INTEL_VERSION_CHECK(13,0,0) || \
  HEDLEY_ARM_VERSION_CHECK(5,6,0) || \
  HEDLEY_IBM_VERSION_CHECK(10,1,0) || \
  HEDLEY_TI_VERSION_CHECK(8,0,0) || \
  (HEDLEY_TI_VERSION_CHECK(7,3,0) && defined(__TI_GNU_ATTRIBUTE_SUPPORT__))
#  define HEDLEY_PRINTF_FORMAT(string_idx,first_to_check) __attribute__((__format__(__printf__, string_idx, first_to_check)))
#elif HEDLEY_PELLES_VERSION_CHECK(6,0,0)
#  define HEDLEY_PRINTF_FORMAT(string_idx,first_to_check) __declspec(vaformat(printf,string_idx,first_to_check))
#else
#  define HEDLEY_PRINTF_FORMAT(string_idx,first_to_check)
#endif

#if defined(HEDLEY_CONSTEXPR)
#  undef HEDLEY_CONSTEXPR
#endif
#if defined(__cplusplus)
#  if __cplusplus >= 201103L
#    define HEDLEY_CONSTEXPR constexpr
#  endif
#endif
#if !defined(HEDLEY_CONSTEXPR)
#  define HEDLEY_CONSTEXPR
#endif

#if defined(HEDLEY_PREDICT)
#  undef HEDLEY_PREDICT
#endif
#if defined(HEDLEY_LIKELY)
#  undef HEDLEY_LIKELY
#endif
#if defined(HEDLEY_UNLIKELY)
#  undef HEDLEY_UNLIKELY
#endif
#if defined(HEDLEY_UNPREDICTABLE)
#  undef HEDLEY_UNPREDICTABLE
#endif
#if HEDLEY_HAS_BUILTIN(__builtin_unpredictable)
#  define HEDLEY_UNPREDICTABLE(expr) __builtin_unpredictable(!!(expr))
#endif
#if \
  HEDLEY_HAS_BUILTIN(__builtin_expect_with_probability) || \
  HEDLEY_GCC_VERSION_CHECK(9,0,0)
#  define HEDLEY_PREDICT(expr, value, probability) __builtin_expect_with_probability(expr, value, probability)
#  define HEDLEY_PREDICT_TRUE(expr, probability) __builtin_expect_with_probability(!!(expr), 1, probability)
#  define HEDLEY_PREDICT_FALSE(expr, probability) __builtin_expect_with_probability(!!(expr), 0, probability)
#  define HEDLEY_LIKELY(expr) __builtin_expect(!!(expr), 1)
#  define HEDLEY_UNLIKELY(expr) __builtin_expect(!!(expr), 0)
#  if !defined(HEDLEY_BUILTIN_UNPREDICTABLE)
#    define HEDLEY_BUILTIN_UNPREDICTABLE(expr) __builtin_expect_with_probability(!!(expr), 1, 0.5)
#  endif
#elif \
  HEDLEY_HAS_BUILTIN(__builtin_expect) || \
  HEDLEY_GCC_VERSION_CHECK(3,0,0) || \
  HEDLEY_INTEL_VERSION_CHECK(13,0,0) || \
  (HEDLEY_SUNPRO_VERSION_CHECK(5,15,0) && defined(__cplusplus)) || \
  HEDLEY_ARM_VERSION_CHECK(4,1,0) || \
  HEDLEY_IBM_VERSION_CHECK(10,1,0) || \
  HEDLEY_TI_VERSION_CHECK(6,1,0) || \
  HEDLEY_TINYC_VERSION_CHECK(0,9,27)
#  define HEDLEY_PREDICT(expr, expected, probability) \
  (((probability) >= 0.9) ? __builtin_expect(!!(expr), (expected)) : (((void) (expected)), !!(expr)))
#  define HEDLEY_PREDICT_TRUE(expr, probability) \
     (__extension__ ({ \
       HEDLEY_CONSTEXPR double hedley_probability_ = (probability); \
       ((hedley_probability_ >= 0.9) ? __builtin_expect(!!(expr), 1) : ((hedley_probability_ <= 0.1) ? __builtin_expect(!!(expr), 0) : !!(expr))); \
     }))
#  define HEDLEY_PREDICT_FALSE(expr, probability) \
     (__extension__ ({ \
       HEDLEY_CONSTEXPR double hedley_probability_ = (probability); \
       ((hedley_probability_ >= 0.9) ? __builtin_expect(!!(expr), 0) : ((hedley_probability_ <= 0.1) ? __builtin_expect(!!(expr), 1) : !!(expr))); \
     }))
#  define HEDLEY_LIKELY(expr)   __builtin_expect(!!(expr), 1)
#  define HEDLEY_UNLIKELY(expr) __builtin_expect(!!(expr), 0)
#else
#  define HEDLEY_PREDICT(expr, expected, probability) (((void) (expected)), !!(expr))
#  define HEDLEY_PREDICT_TRUE(expr, probability) (!!(expr))
#  define HEDLEY_PREDICT_FALSE(expr, probability) (!!(expr))
#  define HEDLEY_LIKELY(expr) (!!(expr))
#  define HEDLEY_UNLIKELY(expr) (!!(expr))
#endif
#if !defined(HEDLEY_UNPREDICTABLE)
#  define HEDLEY_UNPREDICTABLE(expr) HEDLEY_PREDICT(expr, 1, 0.5)
#endif

#if defined(HEDLEY_MALLOC)
#  undef HEDLEY_MALLOC
#endif
#if \
  HEDLEY_HAS_ATTRIBUTE(malloc) || \
  HEDLEY_GCC_VERSION_CHECK(3,1,0) || \
  HEDLEY_INTEL_VERSION_CHECK(13,0,0) || \
  HEDLEY_SUNPRO_VERSION_CHECK(5,11,0) || \
  HEDLEY_ARM_VERSION_CHECK(4,1,0) || \
  HEDLEY_IBM_VERSION_CHECK(12,1,0) || \
  HEDLEY_TI_VERSION_CHECK(8,0,0) || \
  (HEDLEY_TI_VERSION_CHECK(7,3,0) && defined(__TI_GNU_ATTRIBUTE_SUPPORT__))
#  define HEDLEY_MALLOC __attribute__((__malloc__))
#elif HEDLEY_MSVC_VERSION_CHECK(14, 0, 0)
#  define HEDLEY_MALLOC __declspec(restrict)
#else
#  define HEDLEY_MALLOC
#endif

#if defined(HEDLEY_PURE)
#  undef HEDLEY_PURE
#endif
#if \
  HEDLEY_HAS_ATTRIBUTE(pure) || \
  HEDLEY_GCC_VERSION_CHECK(2,96,0) || \
  HEDLEY_INTEL_VERSION_CHECK(13,0,0) || \
  HEDLEY_SUNPRO_VERSION_CHECK(5,11,0) || \
  HEDLEY_ARM_VERSION_CHECK(4,1,0) || \
  HEDLEY_IBM_VERSION_CHECK(10,1,0) || \
  HEDLEY_TI_VERSION_CHECK(8,0,0) || \
  (HEDLEY_TI_VERSION_CHECK(7,3,0) && defined(__TI_GNU_ATTRIBUTE_SUPPORT__)) || \
  HEDLEY_PGI_VERSION_CHECK(17,10,0)
#  define HEDLEY_PURE __attribute__((__pure__))
#elif HEDLEY_TI_VERSION_CHECK(6,0,0) && defined(__cplusplus)
#  define HEDLEY_PURE _Pragma("FUNC_IS_PURE;")
#else
#  define HEDLEY_PURE
#endif

#if defined(HEDLEY_CONST)
#  undef HEDLEY_CONST
#endif
#if \
  HEDLEY_HAS_ATTRIBUTE(const) || \
  HEDLEY_GCC_VERSION_CHECK(2,5,0) || \
  HEDLEY_INTEL_VERSION_CHECK(13,0,0) || \
  HEDLEY_SUNPRO_VERSION_CHECK(5,11,0) || \
  HEDLEY_ARM_VERSION_CHECK(4,1,0) || \
  HEDLEY_IBM_VERSION_CHECK(10,1,0) || \
  HEDLEY_TI_VERSION_CHECK(8,0,0) || \
  (HEDLEY_TI_VERSION_CHECK(7,3,0) && defined(__TI_GNU_ATTRIBUTE_SUPPORT__)) || \
  HEDLEY_PGI_VERSION_CHECK(17,10,0)
#  define HEDLEY_CONST __attribute__((__const__))
#else
#  define HEDLEY_CONST HEDLEY_PURE
#endif

#if defined(HEDLEY_RESTRICT)
#  undef HEDLEY_RESTRICT
#endif
#if defined(__STDC_VERSION__) && (__STDC_VERSION__ >= 199901L) && !defined(__cplusplus)
#  define HEDLEY_RESTRICT restrict
#elif \
  HEDLEY_GCC_VERSION_CHECK(3,1,0) || \
  HEDLEY_MSVC_VERSION_CHECK(14,0,0) || \
  HEDLEY_INTEL_VERSION_CHECK(13,0,0) || \
  HEDLEY_ARM_VERSION_CHECK(4,1,0) || \
  HEDLEY_IBM_VERSION_CHECK(10,1,0) || \
  HEDLEY_PGI_VERSION_CHECK(17,10,0) || \
  HEDLEY_TI_VERSION_CHECK(8,0,0) || \
  (HEDLEY_SUNPRO_VERSION_CHECK(5,14,0) && defined(__cplusplus)) || \
  HEDLEY_IAR_VERSION_CHECK(8,0,0) || \
  defined(__clang__)
#  define HEDLEY_RESTRICT __restrict
#elif HEDLEY_SUNPRO_VERSION_CHECK(5,3,0) && !defined(__cplusplus)
#  define HEDLEY_RESTRICT _Restrict
#else
#  define HEDLEY_RESTRICT
#endif

#if defined(HEDLEY_INLINE)
#  undef HEDLEY_INLINE
#endif
#if \
  (defined(__STDC_VERSION__) && (__STDC_VERSION__ >= 199901L)) || \
  (defined(__cplusplus) && (__cplusplus >= 199711L))
#  define HEDLEY_INLINE inline
#elif \
  defined(HEDLEY_GCC_VERSION) || \
  HEDLEY_ARM_VERSION_CHECK(6,2,0)
#  define HEDLEY_INLINE __inline__
#elif \
  HEDLEY_MSVC_VERSION_CHECK(12,0,0) || \
  HEDLEY_ARM_VERSION_CHECK(4,1,0) || \
  HEDLEY_TI_VERSION_CHECK(8,0,0)
#  define HEDLEY_INLINE __inline
#else
#  define HEDLEY_INLINE
#endif

#if defined(HEDLEY_ALWAYS_INLINE)
#  undef HEDLEY_ALWAYS_INLINE
#endif
#if \
  HEDLEY_HAS_ATTRIBUTE(always_inline) || \
  HEDLEY_GCC_VERSION_CHECK(4,0,0) || \
  HEDLEY_INTEL_VERSION_CHECK(13,0,0) || \
  HEDLEY_SUNPRO_VERSION_CHECK(5,11,0) || \
  HEDLEY_ARM_VERSION_CHECK(4,1,0) || \
  HEDLEY_IBM_VERSION_CHECK(10,1,0) || \
  HEDLEY_TI_VERSION_CHECK(8,0,0) || \
  (HEDLEY_TI_VERSION_CHECK(7,3,0) && defined(__TI_GNU_ATTRIBUTE_SUPPORT__))
#  define HEDLEY_ALWAYS_INLINE __attribute__((__always_inline__)) HEDLEY_INLINE
#elif HEDLEY_MSVC_VERSION_CHECK(12,0,0)
#  define HEDLEY_ALWAYS_INLINE __forceinline
#elif HEDLEY_TI_VERSION_CHECK(7,0,0) && defined(__cplusplus)
#  define HEDLEY_ALWAYS_INLINE _Pragma("FUNC_ALWAYS_INLINE;")
#elif HEDLEY_IAR_VERSION_CHECK(8,0,0)
#  define HEDLEY_ALWAYS_INLINE _Pragma("inline=forced")
#else
#  define HEDLEY_ALWAYS_INLINE HEDLEY_INLINE
#endif

#if defined(HEDLEY_NEVER_INLINE)
#  undef HEDLEY_NEVER_INLINE
#endif
#if \
  HEDLEY_HAS_ATTRIBUTE(noinline) || \
  HEDLEY_GCC_VERSION_CHECK(4,0,0) || \
  HEDLEY_INTEL_VERSION_CHECK(13,0,0) || \
  HEDLEY_SUNPRO_VERSION_CHECK(5,11,0) || \
  HEDLEY_ARM_VERSION_CHECK(4,1,0) || \
  HEDLEY_IBM_VERSION_CHECK(10,1,0) || \
  HEDLEY_TI_VERSION_CHECK(8,0,0) || \
  (HEDLEY_TI_VERSION_CHECK(7,3,0) && defined(__TI_GNU_ATTRIBUTE_SUPPORT__))
#  define HEDLEY_NEVER_INLINE __attribute__((__noinline__))
#elif HEDLEY_MSVC_VERSION_CHECK(13,10,0)
#  define HEDLEY_NEVER_INLINE __declspec(noinline)
#elif HEDLEY_PGI_VERSION_CHECK(10,2,0)
#  define HEDLEY_NEVER_INLINE _Pragma("noinline")
#elif HEDLEY_TI_VERSION_CHECK(6,0,0) && defined(__cplusplus)
#  define HEDLEY_NEVER_INLINE _Pragma("FUNC_CANNOT_INLINE;")
#elif HEDLEY_IAR_VERSION_CHECK(8,0,0)
#  define HEDLEY_NEVER_INLINE _Pragma("inline=never")
#elif HEDLEY_COMPCERT_VERSION_CHECK(3,2,0)
#  define HEDLEY_NEVER_INLINE __attribute((noinline))
#elif HEDLEY_PELLES_VERSION_CHECK(9,0,0)
#  define HEDLEY_NEVER_INLINE __declspec(noinline)
#else
#  define HEDLEY_NEVER_INLINE
#endif

#if defined(HEDLEY_PRIVATE)
#  undef HEDLEY_PRIVATE
#endif
#if defined(HEDLEY_PUBLIC)
#  undef HEDLEY_PUBLIC
#endif
#if defined(HEDLEY_IMPORT)
#  undef HEDLEY_IMPORT
#endif
#if defined(_WIN32) || defined(__CYGWIN__)
#  define HEDLEY_PRIVATE
#  define HEDLEY_PUBLIC   __declspec(dllexport)
#  define HEDLEY_IMPORT   __declspec(dllimport)
#else
#  if \
    HEDLEY_HAS_ATTRIBUTE(visibility) || \
    HEDLEY_GCC_VERSION_CHECK(3,3,0) || \
    HEDLEY_SUNPRO_VERSION_CHECK(5,11,0) || \
    HEDLEY_INTEL_VERSION_CHECK(13,0,0) || \
    HEDLEY_ARM_VERSION_CHECK(4,1,0) || \
    HEDLEY_IBM_VERSION_CHECK(13,1,0) || \
    HEDLEY_TI_VERSION_CHECK(8,0,0) || \
    (HEDLEY_TI_VERSION_CHECK(7,3,0) && defined(__TI_EABI__) && defined(__TI_GNU_ATTRIBUTE_SUPPORT__))
#    define HEDLEY_PRIVATE __attribute__((__visibility__("hidden")))
#    define HEDLEY_PUBLIC  __attribute__((__visibility__("default")))
#  else
#    define HEDLEY_PRIVATE
#    define HEDLEY_PUBLIC
#  endif
#  define HEDLEY_IMPORT    extern
#endif

#if defined(HEDLEY_NO_THROW)
#  undef HEDLEY_NO_THROW
#endif
#if \
  HEDLEY_HAS_ATTRIBUTE(nothrow) || \
  HEDLEY_GCC_VERSION_CHECK(3,3,0) || \
  HEDLEY_INTEL_VERSION_CHECK(13,0,0)
#  define HEDLEY_NO_THROW __attribute__((__nothrow__))
#elif \
  HEDLEY_MSVC_VERSION_CHECK(13,1,0) || \
  HEDLEY_ARM_VERSION_CHECK(4,1,0)
#  define HEDLEY_NO_THROW __declspec(nothrow)
#else
#  define HEDLEY_NO_THROW
#endif

#if defined(HEDLEY_FALL_THROUGH)
#  undef HEDLEY_FALL_THROUGH
#endif
#if \
     defined(__cplusplus) && \
     (!defined(HEDLEY_SUNPRO_VERSION) || HEDLEY_SUNPRO_VERSION_CHECK(5,15,0)) && \
     !defined(HEDLEY_PGI_VERSION)
#  if \
     (__cplusplus >= 201703L) || \
     ((__cplusplus >= 201103L) && HEDLEY_HAS_CPP_ATTRIBUTE(fallthrough))
#    define HEDLEY_FALL_THROUGH [[fallthrough]]
#  elif (__cplusplus >= 201103L) && HEDLEY_HAS_CPP_ATTRIBUTE(clang::fallthrough)
#    define HEDLEY_FALL_THROUGH [[clang::fallthrough]]
#  elif (__cplusplus >= 201103L) && HEDLEY_GCC_VERSION_CHECK(7,0,0)
#    define HEDLEY_FALL_THROUGH [[gnu::fallthrough]]
#  endif
#endif
#if !defined(HEDLEY_FALL_THROUGH)
#  if HEDLEY_GNUC_HAS_ATTRIBUTE(fallthrough,7,0,0) && !defined(HEDLEY_PGI_VERSION)
#    define HEDLEY_FALL_THROUGH __attribute__((__fallthrough__))
#  elif defined(__fallthrough) /* SAL */
#    define HEDLEY_FALL_THROUGH __fallthrough
#  else
#    define HEDLEY_FALL_THROUGH
#  endif
#endif

#if defined(HEDLEY_RETURNS_NON_NULL)
#  undef HEDLEY_RETURNS_NON_NULL
#endif
#if \
  HEDLEY_HAS_ATTRIBUTE(returns_nonnull) || \
  HEDLEY_GCC_VERSION_CHECK(4,9,0)
#  define HEDLEY_RETURNS_NON_NULL __attribute__((__returns_nonnull__))
#elif defined(_Ret_notnull_) /* SAL */
#  define HEDLEY_RETURNS_NON_NULL _Ret_notnull_
#else
#  define HEDLEY_RETURNS_NON_NULL
#endif

#if defined(HEDLEY_ARRAY_PARAM)
#  undef HEDLEY_ARRAY_PARAM
#endif
#if \
  defined(__STDC_VERSION__) && (__STDC_VERSION__ >= 199901L) && \
  !defined(__STDC_NO_VLA__) && \
  !defined(__cplusplus) && \
  !defined(HEDLEY_PGI_VERSION) && \
  !defined(HEDLEY_TINYC_VERSION)
#  define HEDLEY_ARRAY_PARAM(name) (name)
#else
#  define HEDLEY_ARRAY_PARAM(name)
#endif

#if defined(HEDLEY_IS_CONSTANT)
#  undef HEDLEY_IS_CONSTANT
#endif
#if defined(HEDLEY_REQUIRE_CONSTEXPR)
#  undef HEDLEY_REQUIRE_CONSTEXPR
#endif
/* Note the double-underscore. For internal use only; no API
 * guarantees! */
#if defined(HEDLEY__IS_CONSTEXPR)
#  undef HEDLEY__IS_CONSTEXPR
#endif

#if \
  HEDLEY_HAS_BUILTIN(__builtin_constant_p) || \
  HEDLEY_GCC_VERSION_CHECK(3,4,0) || \
  HEDLEY_INTEL_VERSION_CHECK(13,0,0) || \
  HEDLEY_TINYC_VERSION_CHECK(0,9,19) || \
  HEDLEY_ARM_VERSION_CHECK(4,1,0) || \
  HEDLEY_IBM_VERSION_CHECK(13,1,0) || \
  HEDLEY_TI_VERSION_CHECK(6,1,0) || \
  HEDLEY_SUNPRO_VERSION_CHECK(5,10,0) || \
  HEDLEY_CRAY_VERSION_CHECK(8,1,0)
#  define HEDLEY_IS_CONSTANT(expr) __builtin_constant_p(expr)
#endif
#if !defined(__cplusplus)
#  if \
       HEDLEY_HAS_BUILTIN(__builtin_types_compatible_p) || \
       HEDLEY_GCC_VERSION_CHECK(3,4,0) || \
       HEDLEY_INTEL_VERSION_CHECK(13,0,0) || \
       HEDLEY_IBM_VERSION_CHECK(13,1,0) || \
       HEDLEY_CRAY_VERSION_CHECK(8,1,0) || \
       HEDLEY_ARM_VERSION_CHECK(5,4,0) || \
       HEDLEY_TINYC_VERSION_CHECK(0,9,24)
#    if defined(__INTPTR_TYPE__)
#      define HEDLEY__IS_CONSTEXPR(expr) __builtin_types_compatible_p(__typeof__((1 ? (void*) ((__INTPTR_TYPE__) ((expr) * 0)) : (int*) 0)), int*)
#    else
#      include <stdint.h>
#      define HEDLEY__IS_CONSTEXPR(expr) __builtin_types_compatible_p(__typeof__((1 ? (void*) ((intptr_t) ((expr) * 0)) : (int*) 0)), int*)
#    endif
#  elif \
       (defined(__STDC_VERSION__) && (__STDC_VERSION__ >= 201112L) && !defined(HEDLEY_SUNPRO_VERSION) && !defined(HEDLEY_PGI_VERSION)) || \
       HEDLEY_HAS_EXTENSION(c_generic_selections) || \
       HEDLEY_GCC_VERSION_CHECK(4,9,0) || \
       HEDLEY_INTEL_VERSION_CHECK(17,0,0) || \
       HEDLEY_IBM_VERSION_CHECK(12,1,0) || \
       HEDLEY_ARM_VERSION_CHECK(5,3,0)
#    if defined(__INTPTR_TYPE__)
#      define HEDLEY__IS_CONSTEXPR(expr) _Generic((1 ? (void*) ((__INTPTR_TYPE__) ((expr) * 0)) : (int*) 0), int*: 1, void*: 0)
#    else
#      include <stdint.h>
#      define HEDLEY__IS_CONSTEXPR(expr) _Generic((1 ? (void*) ((intptr_t) * 0) : (int*) 0), int*: 1, void*: 0)
#    endif
#  elif \
       defined(HEDLEY_GCC_VERSION) || \
       defined(HEDLEY_INTEL_VERSION) || \
       defined(HEDLEY_TINYC_VERSION) || \
       defined(HEDLEY_TI_VERSION) || \
       defined(__clang__)
#    define HEDLEY__IS_CONSTEXPR(expr) ( \
         sizeof(void) != \
         sizeof(*( \
           1 ? \
             ((void*) ((expr) * 0L) ) : \
             ((struct { char v[sizeof(void) * 2]; } *) 1) \
           ) \
         ) \
       )
#  endif
#endif
#if defined(HEDLEY__IS_CONSTEXPR)
#  if !defined(HEDLEY_IS_CONSTANT)
#    define HEDLEY_IS_CONSTANT(expr) HEDLEY__IS_CONSTEXPR(expr)
#  endif
#  define HEDLEY_REQUIRE_CONSTEXPR(expr) (HEDLEY__IS_CONSTEXPR(expr) ? (expr) : (-1))
#else
#  if !defined(HEDLEY_IS_CONSTANT)
#    define HEDLEY_IS_CONSTANT(expr) (0)
#  endif
#  define HEDLEY_REQUIRE_CONSTEXPR(expr) (expr)
#endif

#if defined(HEDLEY_BEGIN_C_DECLS)
#  undef HEDLEY_BEGIN_C_DECLS
#endif
#if defined(HEDLEY_END_C_DECLS)
#  undef HEDLEY_END_C_DECLS
#endif
#if defined(HEDLEY_C_DECL)
#  undef HEDLEY_C_DECL
#endif
#if defined(__cplusplus)
#  define HEDLEY_BEGIN_C_DECLS extern "C" {
#  define HEDLEY_END_C_DECLS }
#  define HEDLEY_C_DECL extern "C"
#else
#  define HEDLEY_BEGIN_C_DECLS
#  define HEDLEY_END_C_DECLS
#  define HEDLEY_C_DECL
#endif

#if defined(HEDLEY_STATIC_ASSERT)
#  undef HEDLEY_STATIC_ASSERT
#endif
#if \
  !defined(__cplusplus) && ( \
      (defined(__STDC_VERSION__) && (__STDC_VERSION__ >= 201112L)) || \
      HEDLEY_HAS_FEATURE(c_static_assert) || \
      HEDLEY_GCC_VERSION_CHECK(6,0,0) || \
      HEDLEY_INTEL_VERSION_CHECK(13,0,0) || \
      defined(_Static_assert) \
    )
#  define HEDLEY_STATIC_ASSERT(expr, message) _Static_assert(expr, message)
#elif \
  (defined(__cplusplus) && (__cplusplus >= 201703L)) || \
  HEDLEY_MSVC_VERSION_CHECK(16,0,0) || \
  (defined(__cplusplus) && HEDLEY_TI_VERSION_CHECK(8,3,0))
#  define HEDLEY_STATIC_ASSERT(expr, message) static_assert(expr, message)
#elif defined(__cplusplus) && (__cplusplus >= 201103L)
#  define HEDLEY_STATIC_ASSERT(expr, message) static_assert(expr)
#else
#  define HEDLEY_STATIC_ASSERT(expr, message)
#endif

#if defined(HEDLEY_CONST_CAST)
#  undef HEDLEY_CONST_CAST
#endif
#if defined(__cplusplus)
#  define HEDLEY_CONST_CAST(T, expr) (const_cast<T>(expr))
#elif \
  HEDLEY_HAS_WARNING("-Wcast-qual") || \
  HEDLEY_GCC_VERSION_CHECK(4,6,0) || \
  HEDLEY_INTEL_VERSION_CHECK(13,0,0)
#  define HEDLEY_CONST_CAST(T, expr) (__extension__ ({ \
      HEDLEY_DIAGNOSTIC_PUSH \
      HEDLEY_DIAGNOSTIC_DISABLE_CAST_QUAL \
      ((T) (expr)); \
      HEDLEY_DIAGNOSTIC_POP \
    }))
#else
#  define HEDLEY_CONST_CAST(T, expr) ((T) (expr))
#endif

#if defined(HEDLEY_REINTERPRET_CAST)
#  undef HEDLEY_REINTERPRET_CAST
#endif
#if defined(__cplusplus)
#  define HEDLEY_REINTERPRET_CAST(T, expr) (reinterpret_cast<T>(expr))
#else
#  define HEDLEY_REINTERPRET_CAST(T, expr) (*((T*) &(expr)))
#endif

#if defined(HEDLEY_STATIC_CAST)
#  undef HEDLEY_STATIC_CAST
#endif
#if defined(__cplusplus)
#  define HEDLEY_STATIC_CAST(T, expr) (static_cast<T>(expr))
#else
#  define HEDLEY_STATIC_CAST(T, expr) ((T) (expr))
#endif

#if defined(HEDLEY_CPP_CAST)
#  undef HEDLEY_CPP_CAST
#endif
#if defined(__cplusplus)
#  define HEDLEY_CPP_CAST(T, expr) static_cast<T>(expr)
#else
#  define HEDLEY_CPP_CAST(T, expr) (expr)
#endif

#if defined(HEDLEY_MESSAGE)
#  undef HEDLEY_MESSAGE
#endif
#if HEDLEY_HAS_WARNING("-Wunknown-pragmas")
#  define HEDLEY_MESSAGE(msg) \
  HEDLEY_DIAGNOSTIC_PUSH \
  HEDLEY_DIAGNOSTIC_DISABLE_UNKNOWN_PRAGMAS \
  HEDLEY_PRAGMA(message msg) \
  HEDLEY_DIAGNOSTIC_POP
#elif \
  HEDLEY_GCC_VERSION_CHECK(4,4,0) || \
  HEDLEY_INTEL_VERSION_CHECK(13,0,0)
#  define HEDLEY_MESSAGE(msg) HEDLEY_PRAGMA(message msg)
#elif HEDLEY_CRAY_VERSION_CHECK(5,0,0)
#  define HEDLEY_MESSAGE(msg) HEDLEY_PRAGMA(_CRI message msg)
#elif HEDLEY_IAR_VERSION_CHECK(8,0,0)
#  define HEDLEY_MESSAGE(msg) HEDLEY_PRAGMA(message(msg))
#elif HEDLEY_PELLES_VERSION_CHECK(2,0,0)
#  define HEDLEY_MESSAGE(msg) HEDLEY_PRAGMA(message(msg))
#else
#  define HEDLEY_MESSAGE(msg)
#endif

#if defined(HEDLEY_WARNING)
#  undef HEDLEY_WARNING
#endif
#if HEDLEY_HAS_WARNING("-Wunknown-pragmas")
#  define HEDLEY_WARNING(msg) \
  HEDLEY_DIAGNOSTIC_PUSH \
  HEDLEY_DIAGNOSTIC_DISABLE_UNKNOWN_PRAGMAS \
  HEDLEY_PRAGMA(clang warning msg) \
  HEDLEY_DIAGNOSTIC_POP
#elif \
  HEDLEY_GCC_VERSION_CHECK(4,8,0) || \
  HEDLEY_PGI_VERSION_CHECK(18,4,0)
#  define HEDLEY_WARNING(msg) HEDLEY_PRAGMA(GCC warning msg)
#elif HEDLEY_MSVC_VERSION_CHECK(15,0,0)
#  define HEDLEY_WARNING(msg) HEDLEY_PRAGMA(message(msg))
#else
#  define HEDLEY_WARNING(msg) HEDLEY_MESSAGE(msg)
#endif

#if defined(HEDLEY_REQUIRE_MSG)
#  undef HEDLEY_REQUIRE_MSG
#endif
#if HEDLEY_HAS_ATTRIBUTE(diagnose_if)
#  if HEDLEY_HAS_WARNING("-Wgcc-compat")
#    define HEDLEY_REQUIRE_MSG(expr, msg) \
  HEDLEY_DIAGNOSTIC_PUSH \
  _Pragma("clang diagnostic ignored \"-Wgcc-compat\"") \
  __attribute__((__diagnose_if__(!(expr), msg, "error"))) \
  HEDLEY_DIAGNOSTIC_POP
#  else
#    define HEDLEY_REQUIRE_MSG(expr, msg) __attribute__((__diagnose_if__(!(expr), msg, "error")))
#  endif
#else
#  define HEDLEY_REQUIRE_MSG(expr, msg)
#endif

#if defined(HEDLEY_REQUIRE)
#  undef HEDLEY_REQUIRE
#endif
#define HEDLEY_REQUIRE(expr) HEDLEY_REQUIRE_MSG(expr, #expr)

#if defined(HEDLEY_FLAGS)
#  undef HEDLEY_FLAGS
#endif
#if HEDLEY_HAS_ATTRIBUTE(flag_enum)
#  define HEDLEY_FLAGS __attribute__((__flag_enum__))
#endif

#if defined(HEDLEY_FLAGS_CAST)
#  undef HEDLEY_FLAGS_CAST
#endif
#if HEDLEY_INTEL_VERSION_CHECK(19,0,0)
#  define HEDLEY_FLAGS_CAST(T, expr) (__extension__ ({ \
  HEDLEY_DIAGNOSTIC_PUSH \
      _Pragma("warning(disable:188)") \
      ((T) (expr)); \
      HEDLEY_DIAGNOSTIC_POP \
    }))
#else
#  define HEDLEY_FLAGS_CAST(T, expr) HEDLEY_STATIC_CAST(T, expr)
#endif

/* Remaining macros are deprecated. */

#if defined(HEDLEY_GCC_NOT_CLANG_VERSION_CHECK)
#  undef HEDLEY_GCC_NOT_CLANG_VERSION_CHECK
#endif
#if defined(__clang__)
#  define HEDLEY_GCC_NOT_CLANG_VERSION_CHECK(major,minor,patch) (0)
#else
#  define HEDLEY_GCC_NOT_CLANG_VERSION_CHECK(major,minor,patch) HEDLEY_GCC_VERSION_CHECK(major,minor,patch)
#endif

#if defined(HEDLEY_CLANG_HAS_ATTRIBUTE)
#  undef HEDLEY_CLANG_HAS_ATTRIBUTE
#endif
#define HEDLEY_CLANG_HAS_ATTRIBUTE(attribute) HEDLEY_HAS_ATTRIBUTE(attribute)

#if defined(HEDLEY_CLANG_HAS_CPP_ATTRIBUTE)
#  undef HEDLEY_CLANG_HAS_CPP_ATTRIBUTE
#endif
#define HEDLEY_CLANG_HAS_CPP_ATTRIBUTE(attribute) HEDLEY_HAS_CPP_ATTRIBUTE(attribute)

#if defined(HEDLEY_CLANG_HAS_BUILTIN)
#  undef HEDLEY_CLANG_HAS_BUILTIN
#endif
#define HEDLEY_CLANG_HAS_BUILTIN(builtin) HEDLEY_HAS_BUILTIN(builtin)

#if defined(HEDLEY_CLANG_HAS_FEATURE)
#  undef HEDLEY_CLANG_HAS_FEATURE
#endif
#define HEDLEY_CLANG_HAS_FEATURE(feature) HEDLEY_HAS_FEATURE(feature)

#if defined(HEDLEY_CLANG_HAS_EXTENSION)
#  undef HEDLEY_CLANG_HAS_EXTENSION
#endif
#define HEDLEY_CLANG_HAS_EXTENSION(extension) HEDLEY_HAS_EXTENSION(extension)

#if defined(HEDLEY_CLANG_HAS_DECLSPEC_DECLSPEC_ATTRIBUTE)
#  undef HEDLEY_CLANG_HAS_DECLSPEC_DECLSPEC_ATTRIBUTE
#endif
#define HEDLEY_CLANG_HAS_DECLSPEC_ATTRIBUTE(attribute) HEDLEY_HAS_DECLSPEC_ATTRIBUTE(attribute)

#if defined(HEDLEY_CLANG_HAS_WARNING)
#  undef HEDLEY_CLANG_HAS_WARNING
#endif
#define HEDLEY_CLANG_HAS_WARNING(warning) HEDLEY_HAS_WARNING(warning)

#endif /* !defined(HEDLEY_VERSION) || (HEDLEY_VERSION < X) */

/** @file
 *  Defines various compatibility macros
 */


// If there is another version of Hedley, then the newer one
// takes precedence.
// See: https://github.com/nemequ/hedley

/** Used to supress unused variable warning in g++ */
#define SUPPRESS_UNUSED_WARNING(x) (void)x

namespace csv {
/**
 *  @def IF_CONSTEXPR
 *  Expands to `if constexpr` in C++17 and `if` otherwise
 *
 *  @def CONSTEXPR_VALUE
 *  Expands to `constexpr` in C++17 and `const` otherwise.
 *  Mainly used for global variables.
 *
 *  @def CONSTEXPR
 *  Expands to `constexpr` in C++17 and `inline` otherwise.
 *  Intended for functions and methods.
 */

#if CMAKE_CXX_STANDARD == 17 || __cplusplus >= 201703L
#define CSV_HAS_CXX17
#endif

#ifdef CSV_HAS_CXX17
#include <string_view>
/** @typedef string_view
 *  The string_view class used by this library.
 */
using string_view = std::string_view;
#else
/** @typedef string_view
         *  The string_view class used by this library.
         */
        using string_view = nonstd::string_view;
#endif

#ifdef CSV_HAS_CXX17
#define IF_CONSTEXPR if constexpr
#define CONSTEXPR_VALUE constexpr
#else
#define IF_CONSTEXPR if
        #define CONSTEXPR_VALUE const
#endif

// Resolves g++ bug with regard to constexpr methods
#ifdef __GNUC__
#if __GNUC__ >= 7
#if defined(CSV_HAS_CXX17) && (__GNUC_MINOR__ >= 2 || __GNUC__ >= 8)
#define CONSTEXPR constexpr
#endif
#endif
#else
#ifdef CSV_HAS_CXX17
            #define CONSTEXPR constexpr
        #endif
#endif

#ifndef CONSTEXPR
#define CONSTEXPR inline
#endif
}
/** @file
 *  Defines an object used to store CSV format settings
 */

#include <stdexcept>
#include <string>
#include <vector>

namespace csv {
class CSVReader;

/** Stores information about how to parse a CSV file.
 *  Can be used to construct a csv::CSVReader.
 */
class CSVFormat {
 public:
  /** Settings for parsing a RFC 4180 CSV file */
  CSVFormat() = default;

  /** Sets the delimiter of the CSV file */
  CSVFormat& delimiter(char delim);

  /** Sets a list of potential delimiters
   *
   *  @param[in] delim An array of possible delimiters to try parsing the CSV with
   */
  CSVFormat& delimiter(const std::vector<char> & delim);

  /** Sets the whitespace characters to be trimmed
   *
   *  @param[in] ws An array of whitespace characters that should be trimmed
   */
  CSVFormat& trim(const std::vector<char> & ws);

  /** Sets the quote character */
  CSVFormat& quote(char quote);

  /** Sets the column names.
   *
   *  @note Unsets any values set by header_row()
   */
  CSVFormat& column_names(const std::vector<std::string>& names);

  /** Sets the header row
   *
   *  @note Unsets any values set by column_names()
   */
  CSVFormat& header_row(int row);

  /** Tells the parser to throw an std::runtime_error if an
   *  invalid CSV sequence is found
   */
  CSVFormat& strict_parsing(bool strict = true);

  /** Tells the parser to detect and remove UTF-8 byte order marks */
  CSVFormat& detect_bom(bool detect = true);

#ifndef DOXYGEN_SHOULD_SKIP_THIS
  char get_delim() {
    // This error should never be received by end users.
    if (this->possible_delimiters.size() > 1) {
      throw std::runtime_error("There is more than one possible delimiter.");
    }

    return this->possible_delimiters.at(0);
  }

  int get_header() {
    return this->header;
  }
#endif

  /** CSVFormat for guessing the delimiter */
  static const CSVFormat GUESS_CSV;

  /** CSVFormat for strict RFC 4180 parsing */
  static const CSVFormat RFC4180_STRICT;

  friend CSVReader;
 private:
  bool guess_delim() {
    return this->possible_delimiters.size() > 1;
  }

  /**< Set of possible delimiters */
  std::vector<char> possible_delimiters = { ',' };

  /**< Set of whitespace characters to trim */
  std::vector<char> trim_chars = {};

  /**< Quote character */
  char quote_char = '"';

  /**< Row number with columns (ignored if col_names is non-empty) */
  int header = 0;

  /**< Should be left empty unless file doesn't include header */
  std::vector<std::string> col_names = {};

  /**< RFC 4180 non-compliance -> throw an error */
  bool strict = false;

  /**< Detect and strip out Unicode byte order marks */
  bool unicode_detect = true;
};
}
/** @file
  *  A standalone header file for writing delimiter-separated files
  */

#include <iostream>
#include <vector>
#include <string>
#include <fstream>

namespace csv {
/** @name CSV Writing */
///@{
#ifndef DOXYGEN_SHOULD_SKIP_THIS
template<char Delim = ',', char Quote = '"'>
inline std::string csv_escape(csv::string_view in, const bool quote_minimal = true) {
  /** Format a string to be RFC 4180-compliant
   *  @param[in]  in              String to be CSV-formatted
   *  @param[out] quote_minimal   Only quote fields if necessary.
   *                              If False, everything is quoted.
   */

  // Sequence used for escaping quote characters that appear in text
  constexpr char double_quote[3] = { Quote, Quote };

  std::string new_string;
  bool quote_escape = false;     // Do we need a quote escape
  new_string += Quote;           // Start initial quote escape sequence

  for (size_t i = 0; i < in.size(); i++) {
    switch (in[i]) {
      case Quote:
        new_string += double_quote;
        quote_escape = true;
        break;
      case Delim:
        quote_escape = true;
        HEDLEY_FALL_THROUGH;
      default:
        new_string += in[i];
    }
  }

  if (quote_escape || !quote_minimal) {
    new_string += Quote; // Finish off quote escape
    return new_string;
  }

  return std::string(in);
}
#endif

/**
 *  Class for writing delimiter separated values files
 *
 *  To write formatted strings, one should
 *   -# Initialize a DelimWriter with respect to some output stream
 *   -# Call write_row() on std::vector<std::string>s of unformatted text
 *
 *  @tparam OutputStream The output stream, e.g. `std::ofstream`, `std::stringstream`
 *  @tparam Delim        The delimiter character
 *  @tparam Quote        The quote character
 *
 *  @par Hint
 *  Use the aliases csv::CSVWriter<OutputStream> to write CSV
 *  formatted strings and csv::TSVWriter<OutputStream>
 *  to write tab separated strings
 *
 *  @par Example
 *  @snippet test_write_csv.cpp CSV Writer Example
 */
template<class OutputStream, char Delim, char Quote>
class DelimWriter {
 public:
  /** Construct a DelimWriter over the specified output stream */
  DelimWriter(OutputStream& _out) : out(_out) {};

  /** Construct a DelimWriter over the file
   *
   *  @param[out] filename  File to write to
   */
  DelimWriter(const std::string& filename) : DelimWriter(std::ifstream(filename)) {};

  /** Format a sequence of strings and write to CSV according to RFC 4180
   *
   *  @warning This does not check to make sure row lengths are consistent
   *
   *  @param[in]  record          Sequence of strings to be formatted
   *  @param      quote_minimal   Only quote fields if necessary
   */
  template<typename T, typename Alloc, template <typename, typename> class Container>
  void write_row(const Container<T, Alloc>& record, bool quote_minimal = true) {
    const size_t ilen = record.size();
    size_t i = 0;
    for (auto& field: record) {
      out << csv_escape<Delim, Quote>(field, quote_minimal);
      if (i + 1 != ilen) out << Delim;
      i++;
    }

    out << std::endl;
  }

  /** @copydoc write_row
   *  @return  The current DelimWriter instance (allowing for operator chaining)
   */
  template<typename T, typename Alloc, template <typename, typename> class Container>
  DelimWriter& operator<<(const Container<T, Alloc>& record) {
    this->write_row(record);
    return *this;
  }

 private:
  OutputStream & out;
};

/* Uncomment when C++17 support is better
template<class OutputStream>
DelimWriter(OutputStream&) -> DelimWriter<OutputStream>;
*/

/** Class for writing CSV files
 *
 *  @sa csv::DelimWriter::write_row()
 *  @sa csv::DelimWriter::operator<<()
 *
 *  @note Use `csv::make_csv_writer()` to in instatiate this class over
 *        an actual output stream.
 */
template<class OutputStream>
using CSVWriter = DelimWriter<OutputStream, ',', '"'>;

/** Class for writing tab-separated values files
*
 *  @sa csv::DelimWriter::write_row()
 *  @sa csv::DelimWriter::operator<<()
 *
 *  @note Use `csv::make_tsv_writer()` to in instatiate this class over
 *        an actual output stream.
 */
template<class OutputStream>
using TSVWriter = DelimWriter<OutputStream, '\t', '"'>;

//
// Temporary: Until more C++17 compilers support template deduction guides
//
template<class OutputStream>
inline CSVWriter<OutputStream> make_csv_writer(OutputStream& out) {
  /** Return a CSVWriter over the output stream */
  return CSVWriter<OutputStream>(out);
}

template<class OutputStream>
inline TSVWriter<OutputStream> make_tsv_writer(OutputStream& out) {
  /** Return a TSVWriter over the output stream */
  return TSVWriter<OutputStream>(out);
}

///@}
}
/** @file
 *  @brief Implements data type parsing functionality
 */

#include <math.h>
#include <cctype>
#include <string>
#include <cassert>


namespace csv {
/** Enumerates the different CSV field types that are
 *  recognized by this library
 *
 *  @note Overflowing integers will be stored and classified as doubles.
 *  @note Unlike previous releases, integer enums here are platform agnostic.
 */
enum DataType {
  UNKNOWN = -1,
  CSV_NULL,   /**< Empty string */
  CSV_STRING, /**< Non-numeric string */
  CSV_INT8,   /**< 8-bit integer */
  CSV_INT16,  /**< 16-bit integer (short on MSVC/GCC) */
  CSV_INT32,  /**< 32-bit integer (int on MSVC/GCC) */
  CSV_INT64,  /**< 64-bit integer (long long on MSVC/GCC) */
  CSV_DOUBLE  /**< Floating point value */
};

static_assert(CSV_STRING < CSV_INT8, "String type should come before numeric types.");
static_assert(CSV_INT8 < CSV_INT64, "Smaller integer types should come before larger integer types.");
static_assert(CSV_INT64 < CSV_DOUBLE, "Integer types should come before floating point value types.");

namespace internals {
/** Compute 10 to the power of n */
template<typename T>
HEDLEY_CONST CONSTEXPR
long double pow10(const T& n) noexcept {
  long double multiplicand = n > 0 ? 10 : 0.1,
      ret = 1;

  // Make all numbers positive
  T iterations = n > 0 ? n : -n;

  for (T i = 0; i < iterations; i++) {
    ret *= multiplicand;
  }

  return ret;
}

/** Compute 10 to the power of n */
template<>
HEDLEY_CONST CONSTEXPR
long double pow10(const unsigned& n) noexcept {
  long double multiplicand = n > 0 ? 10 : 0.1,
      ret = 1;

  for (unsigned i = 0; i < n; i++) {
    ret *= multiplicand;
  }

  return ret;
}

#ifndef DOXYGEN_SHOULD_SKIP_THIS
/** Private site-indexed array mapping byte sizes to an integer size enum */
constexpr DataType int_type_arr[8] = {
    CSV_INT8,  // 1
    CSV_INT16, // 2
    UNKNOWN,
    CSV_INT32, // 4
    UNKNOWN,
    UNKNOWN,
    UNKNOWN,
    CSV_INT64  // 8
};

template<typename T>
inline DataType type_num() {
  static_assert(std::is_integral<T>::value, "T should be an integral type.");
  static_assert(sizeof(T) <= 8, "Byte size must be no greater than 8.");
  return int_type_arr[sizeof(T) - 1];
}

template<> inline DataType type_num<float>() { return CSV_DOUBLE; }
template<> inline DataType type_num<double>() { return CSV_DOUBLE; }
template<> inline DataType type_num<long double>() { return CSV_DOUBLE; }
template<> inline DataType type_num<std::nullptr_t>() { return CSV_NULL; }
template<> inline DataType type_num<std::string>() { return CSV_STRING; }

inline std::string type_name(const DataType& dtype) {
  switch (dtype) {
    case CSV_STRING:
      return "string";
    case CSV_INT8:
      return "int8";
    case CSV_INT16:
      return "int16";
    case CSV_INT32:
      return "int32";
    case CSV_INT64:
      return "int64";
    case CSV_DOUBLE:
      return "double";
    default:
      return "null";
  }
};

CONSTEXPR DataType data_type(csv::string_view in, long double* const out = nullptr);
#endif

/** Given a byte size, return the largest number than can be stored in
 *  an integer of that size
 */
template<size_t Bytes>
CONSTEXPR long double get_int_max() {
  static_assert(Bytes == 1 || Bytes == 2 || Bytes == 4 || Bytes == 8,
                "Bytes must be a power of 2 below 8.");

  IF_CONSTEXPR (sizeof(signed char) == Bytes) {
    return (long double)std::numeric_limits<signed char>::max();
  }

  IF_CONSTEXPR (sizeof(short) == Bytes) {
    return (long double)std::numeric_limits<short>::max();
  }

  IF_CONSTEXPR (sizeof(int) == Bytes) {
    return (long double)std::numeric_limits<int>::max();
  }

  IF_CONSTEXPR (sizeof(long int) == Bytes) {
    return (long double)std::numeric_limits<long int>::max();
  }

  IF_CONSTEXPR (sizeof(long long int) == Bytes) {
    return (long double)std::numeric_limits<long long int>::max();
  }

  HEDLEY_UNREACHABLE();
}

/** Largest number that can be stored in a 1-bit integer */
CONSTEXPR_VALUE long double CSV_INT8_MAX = get_int_max<1>();

/** Largest number that can be stored in a 16-bit integer */
CONSTEXPR_VALUE long double CSV_INT16_MAX = get_int_max<2>();

/** Largest number that can be stored in a 32-bit integer */
CONSTEXPR_VALUE long double CSV_INT32_MAX = get_int_max<4>();

/** Largest number that can be stored in a 64-bit integer */
CONSTEXPR_VALUE long double CSV_INT64_MAX = get_int_max<8>();

/** Given a pointer to the start of what is start of
 *  the exponential part of a number written (possibly) in scientific notation
 *  parse the exponent
 */
HEDLEY_PRIVATE CONSTEXPR
DataType _process_potential_exponential(
    csv::string_view exponential_part,
    const long double& coeff,
    long double * const out) {
  long double exponent = 0;
  auto result = data_type(exponential_part, &exponent);

  if (result >= CSV_INT8 && result <= CSV_DOUBLE) {
    if (out) *out = coeff * pow10(exponent);
    return CSV_DOUBLE;
  }

  return CSV_STRING;
}

/** Given the absolute value of an integer, determine what numeric type
 *  it fits in
 */
HEDLEY_PRIVATE HEDLEY_PURE CONSTEXPR
DataType _determine_integral_type(const long double& number) noexcept {
  // We can assume number is always non-negative
  assert(number >= 0);

  if (number < internals::CSV_INT8_MAX)
    return CSV_INT8;
  else if (number < internals::CSV_INT16_MAX)
    return CSV_INT16;
  else if (number < internals::CSV_INT32_MAX)
    return CSV_INT32;
  else if (number < internals::CSV_INT64_MAX)
    return CSV_INT64;
  else // Conversion to long long will cause an overflow
    return CSV_DOUBLE;
}

/** Distinguishes numeric from other text values. Used by various
 *  type casting functions, like csv_parser::CSVReader::read_row()
 *
 *  #### Rules
 *   - Leading and trailing whitespace ("padding") ignored
 *   - A string of just whitespace is NULL
 *
 *  @param[in]  in  String value to be examined
 *  @param[out] out Pointer to long double where results of numeric parsing
 *                  get stored
 */
CONSTEXPR
DataType data_type(csv::string_view in, long double* const out) {
  // Empty string --> NULL
  if (in.size() == 0)
    return CSV_NULL;

  bool ws_allowed = true,
      neg_allowed = true,
      dot_allowed = true,
      digit_allowed = true,
      has_digit = false,
      prob_float = false;

  unsigned places_after_decimal = 0;
  long double integral_part = 0,
      decimal_part = 0;

  for (size_t i = 0, ilen = in.size(); i < ilen; i++) {
    const char& current = in[i];

    switch (current) {
      case ' ':
        if (!ws_allowed) {
          if (isdigit(in[i - 1])) {
            digit_allowed = false;
            ws_allowed = true;
          }
          else {
            // Ex: '510 123 4567'
            return CSV_STRING;
          }
        }
        break;
      case '-':
        if (!neg_allowed) {
          // Ex: '510-123-4567'
          return CSV_STRING;
        }

        neg_allowed = false;
        break;
      case '.':
        if (!dot_allowed) {
          return CSV_STRING;
        }

        dot_allowed = false;
        prob_float = true;
        break;
      case 'e':
      case 'E':
        // Process scientific notation
        if (prob_float) {
          size_t exponent_start_idx = i + 1;

          // Strip out plus sign
          if (in[i + 1] == '+') {
            exponent_start_idx++;
          }

          return _process_potential_exponential(
              in.substr(exponent_start_idx),
              neg_allowed ? integral_part + decimal_part : -(integral_part + decimal_part),
              out
          );
        }

        return CSV_STRING;
        break;
      default:
        short digit = current - '0';
        if (digit >= 0 && digit <= 9) {
          // Process digit
          has_digit = true;

          if (!digit_allowed)
            return CSV_STRING;
          else if (ws_allowed) // Ex: '510 456'
            ws_allowed = false;

          // Build current number
          if (prob_float)
            decimal_part += digit / pow10(++places_after_decimal);
          else
            integral_part = (integral_part * 10) + digit;
        }
        else {
          return CSV_STRING;
        }
    }
  }

  // No non-numeric/non-whitespace characters found
  if (has_digit) {
    long double number = integral_part + decimal_part;
    if (out) {
      *out = neg_allowed ? number : -number;
    }

    return prob_float ? CSV_DOUBLE : _determine_integral_type(number);
  }

  // Just whitespace
  return CSV_NULL;
}
}
}
/** @file
 *  Defines an object which can store CSV data in
 *  continuous regions of memory
 */

#include <memory>
#include <vector>
#include <unordered_map>


namespace csv {
namespace internals {
class RawRowBuffer;
struct ColumnPositions;
struct ColNames;
using BufferPtr = std::shared_ptr<RawRowBuffer>;
using ColNamesPtr = std::shared_ptr<ColNames>;
using SplitArray = std::vector<unsigned short>;

/** @struct ColNames
 *  A data structure for handling column name information.
 *
 *  These are created by CSVReader and passed (via smart pointer)
 *  to CSVRow objects it creates, thus
 *  allowing for indexing by column name.
 */
struct ColNames {
  ColNames(const std::vector<std::string>&);
  std::vector<std::string> col_names;
  std::unordered_map<std::string, size_t> col_pos;

  std::vector<std::string> get_col_names() const;
  size_t size() const;
};

/** Class for reducing number of new string and new vector
 *  and malloc calls
 *
 *  @par Motivation
 *  By storing CSV strings in a giant string (as opposed to an
 *  `std::vector` of smaller strings), we vastly reduce the number
 *  of calls to `malloc()`, thus speeding up the program.
 *  However, by doing so we will need a way to tell where different
 *  fields are located within this giant string.
 *  Hence, an array of indices is also maintained.
 *
 *  @warning
 *  `reset()` should be called somewhat often in the code. Since each
 *  `csv::CSVRow` contains an `std::shared_ptr` to a RawRowBuffer,
 *  the buffers do not get deleted until every CSVRow referencing it gets
 *  deleted. If RawRowBuffers get very large, then so will memory consumption.
 *  Currently, `reset()` is called by `csv::CSVReader::feed()` at the end of
 *  every sequence of bytes parsed.
 *
 */
class RawRowBuffer {
 public:
  RawRowBuffer() = default;

  /** Constructor mainly used for testing
   *  @param[in] _buffer    CSV text without delimiters or newlines
   *  @param[in] _splits    Positions in buffer where CSV fields begin
   *  @param[in] _col_names Pointer to a vector of column names
   */
  RawRowBuffer(const std::string& _buffer, const std::vector<unsigned short>& _splits,
               const std::shared_ptr<ColNames>& _col_names) :
      buffer(_buffer), split_buffer(_splits), col_names(_col_names) {};

  csv::string_view get_row();      /**< Return a string_view over the current_row */
  ColumnPositions get_splits();    /**< Return the field start positions for the current row */

  size_t size() const;             /**< Return size of current row */
  size_t splits_size() const;      /**< Return (num columns - 1) for current row */
  BufferPtr reset() const;         /**< Create a new RawRowBuffer with this buffer's unfinished work */

  std::string buffer;              /**< Buffer for storing text */
  SplitArray split_buffer = {};    /**< Array for storing indices (in buffer)
                                                  of where CSV fields start */
  ColNamesPtr col_names = nullptr; /**< Pointer to column names */

 private:
  size_t current_end = 0;          /**< Where we are currently in the text buffer */
  size_t current_split_idx = 0;    /**< Where we are currently in the split buffer */
};

struct ColumnPositions {
  ColumnPositions() : parent(nullptr) {};
  constexpr ColumnPositions(const RawRowBuffer& _parent,
                            size_t _start, unsigned short _size) : parent(&_parent), start(_start), n_cols(_size) {};

  const RawRowBuffer * parent; /**< RawRowBuffer to grab data from */
  size_t start;                /**< Where in split_buffer the array of column positions begins */
  unsigned short n_cols;       /**< Number of columns */

  /// Get the n-th column index
  unsigned short split_at(int n) const;
};
}
}
/** @file
 *  Defines CSV global constants
 */

#include <deque>


namespace csv {
namespace internals {
// Get operating system specific details
#if defined(_WIN32)
#include <Windows.h>
            #undef max
            #undef min

            inline int getpagesize() {
                _SYSTEM_INFO sys_info = {};
                GetSystemInfo(&sys_info);
                return sys_info.dwPageSize;
            }

            /** Size of a memory page in bytes */
            const int PAGE_SIZE = getpagesize();
#elif defined(__linux__)
#include <unistd.h>
const int PAGE_SIZE = getpagesize();
#else
const int PAGE_SIZE = 4096;
#endif

/** For functions that lazy load a large CSV, this determines how
 *  many bytes are read at a time
 */
const size_t ITERATION_CHUNK_SIZE = 50000000; // 50MB
}

/** Used for counting number of rows */
using RowCount = long long int;

class CSVRow;
using CSVCollection = std::deque<CSVRow>;
}

#include <string>
#include <type_traits>
#include <unordered_map>

namespace csv {
/** Returned by get_file_info() */
struct CSVFileInfo {
  std::string filename;               /**< Filename */
  std::vector<std::string> col_names; /**< CSV column names */
  char delim;                         /**< Delimiting character */
  RowCount n_rows;                    /**< Number of rows in a file */
  int n_cols;                         /**< Number of columns in a CSV */
};

/** @name Shorthand Parsing Functions
 *  @brief Convienience functions for parsing small strings
 */
///@{
CSVCollection operator ""_csv(const char*, size_t);
CSVCollection parse(csv::string_view in, CSVFormat format = CSVFormat());
///@}

/** @name Utility Functions */
///@{
std::unordered_map<std::string, DataType> csv_data_types(const std::string&);
CSVFileInfo get_file_info(const std::string& filename);
CSVFormat guess_format(csv::string_view filename,
                       const std::vector<char>& delims = { ',', '|', '\t', ';', '^', '~' });
std::vector<std::string> get_col_names(
    const std::string& filename,
    const CSVFormat format = CSVFormat::GUESS_CSV);
int get_col_pos(const std::string filename, const std::string col_name,
                const CSVFormat format = CSVFormat::GUESS_CSV);
///@}

namespace internals {
template<typename T>
inline bool is_equal(T a, T b, T epsilon = 0.001) {
  /** Returns true if two floating point values are about the same */
  static_assert(std::is_floating_point<T>::value, "T must be a floating point type.");
  return std::abs(a - b) < epsilon;
}
}
}
/** @file
 *  Defines the data type used for storing information about a CSV row
 */

#include <math.h>
#include <vector>
#include <string>
#include <iterator>
#include <unordered_map> // For ColNames
#include <memory> // For CSVField
#include <limits> // For CSVField


namespace csv {
namespace internals {
static const std::string ERROR_NAN = "Not a number.";
static const std::string ERROR_OVERFLOW = "Overflow error.";
static const std::string ERROR_FLOAT_TO_INT =
    "Attempted to convert a floating point value to an integral type.";
}

/**
* @class CSVField
* @brief Data type representing individual CSV values.
*        CSVFields can be obtained by using CSVRow::operator[]
*/
class CSVField {
 public:
  /** Constructs a CSVField from a string_view */
  constexpr explicit CSVField(csv::string_view _sv) : sv(_sv) { };

  operator std::string() const {
    return std::string("<CSVField> ") + std::string(this->sv);
  }

  /** Returns the value casted to the requested type, performing type checking before.
  *
  *  \par Valid options for T
  *   - std::string or csv::string_view
  *   - signed integral types (signed char, short, int, long int, long long int)
  *   - floating point types (float, double, long double)
  *   - unsigned integers are not supported at this time, but may be in a later release
  *
  *  \par Invalid conversions
  *   - Converting non-numeric values to any numeric type
  *   - Converting floating point values to integers
  *   - Converting a large integer to a smaller type that will not hold it
  *
  *  @note    This method is capable of parsing scientific E-notation.
  *           See [this page](md_docs_source_scientific_notation.html)
  *           for more details.
  *
  *  @throws  std::runtime_error Thrown if an invalid conversion is performed.
  *
  *  @warning Currently, conversions to floating point types are not
  *           checked for loss of precision
  *
  *  @warning Any string_views returned are only guaranteed to be valid
  *           if the parent CSVRow is still alive. If you are concerned
  *           about object lifetimes, then grab a std::string or a
  *           numeric value.
  *
  */
  template<typename T=std::string> T get() {
    static_assert(!std::is_unsigned<T>::value, "Conversions to unsigned types are not supported.");

    IF_CONSTEXPR (std::is_arithmetic<T>::value) {
      if (this->type() <= CSV_STRING) {
        throw std::runtime_error(internals::ERROR_NAN);
      }
    }

    IF_CONSTEXPR(std::is_integral<T>::value) {
      if (this->is_float()) {
        throw std::runtime_error(internals::ERROR_FLOAT_TO_INT);
      }
    }

    // Allow fallthrough from previous if branch
    IF_CONSTEXPR(!std::is_floating_point<T>::value) {
      if (internals::type_num<T>() < this->_type) {
        throw std::runtime_error(internals::ERROR_OVERFLOW);
      }
    }

    return static_cast<T>(this->value);
  }

  /** Compares the contents of this field to a numeric value. If this
   *  field does not contain a numeric value, then all comparisons return
   *  false.
   *
   *  @note    Floating point values are considered equal if they are within
   *           `0.000001` of each other.
   *
   *  @warning Multiple numeric comparisons involving the same field can
   *           be done more efficiently by calling the CSVField::get<>() method.
   *
   *  @sa      csv::CSVField::operator==(const char * other)
   *  @sa      csv::CSVField::operator==(csv::string_view other)
   */
  template<typename T>
  bool operator==(T other) const
  {
    static_assert(std::is_arithmetic<T>::value,
                  "T should be a numeric value.");

    if (this->_type != UNKNOWN) {
      if (this->_type == CSV_STRING) {
        return false;
      }

      return internals::is_equal(value, static_cast<long double>(other), 0.000001L);
    }

    long double out = 0;
    if (internals::data_type(this->sv, &out) == CSV_STRING) {
      return false;
    }

    return internals::is_equal(out, static_cast<long double>(other), 0.000001L);
  }

  /** Returns true if field is an empty string or string of whitespace characters */
  CONSTEXPR bool is_null() { return type() == CSV_NULL; }

  /** Returns true if field is a non-numeric, non-empty string */
  CONSTEXPR bool is_str() { return type() == CSV_STRING; }

  /** Returns true if field is an integer or float */
  CONSTEXPR bool is_num() { return type() >= CSV_INT8; }

  /** Returns true if field is an integer */
  CONSTEXPR bool is_int() {
    return (type() >= CSV_INT8) && (type() <= CSV_INT64);
  }

  /** Returns true if field is a floating point value */
  CONSTEXPR bool is_float() { return type() == CSV_DOUBLE; };

  /** Return the type of the underlying CSV data */
  CONSTEXPR DataType type() {
    this->get_value();
    return _type;
  }

 private:
  long double value = 0;    /**< Cached numeric value */
  csv::string_view sv = ""; /**< A pointer to this field's text */
  DataType _type = UNKNOWN; /**< Cached data type value */
  CONSTEXPR void get_value() {
    /* Check to see if value has been cached previously, if not
     * evaluate it
     */
    if (_type < 0) {
      this->_type = internals::data_type(this->sv, &this->value);
    }
  }
};

/** Data structure for representing CSV rows */
class CSVRow {
 public:
  CSVRow() = default;

  /** Construct a CSVRow from a RawRowBuffer. Should be called by CSVReader::write_record. */
  CSVRow(const internals::BufferPtr& _str) : buffer(_str)
  {
    this->row_str = _str->get_row();

    auto splits = _str->get_splits();
    this->start = splits.start;
    this->n_cols = splits.n_cols;
  };

  /** Constructor for testing */
  CSVRow(const std::string& str, const std::vector<unsigned short>& splits,
         const std::shared_ptr<internals::ColNames>& col_names)
      : CSVRow(internals::BufferPtr(new internals::RawRowBuffer(str, splits, col_names))) {};

  /** Indicates whether row is empty or not */
  CONSTEXPR bool empty() const { return this->row_str.empty(); }

  /** Return the number of fields in this row */
  CONSTEXPR size_t size() const { return this->n_cols; }

  /** @name Value Retrieval */
  ///@{
  CSVField operator[](size_t n) const;
  CSVField operator[](const std::string&) const;
  csv::string_view get_string_view(size_t n) const;

  /** Convert this CSVRow into a vector of strings.
   *  **Note**: This is a less efficient method of
   *  accessing data than using the [] operator.
   */
  operator std::vector<std::string>() const;
  ///@}

  /** A random access iterator over the contents of a CSV row.
   *  Each iterator points to a CSVField.
   */
  class iterator {
   public:
#ifndef DOXYGEN_SHOULD_SKIP_THIS
    using value_type = CSVField;
    using difference_type = int;

    // Using CSVField * as pointer type causes segfaults in MSVC debug builds
    // but using shared_ptr as pointer type won't compile in g++
#ifdef _MSC_BUILD
    using pointer = std::shared_ptr<CSVField> ;
#else
    using pointer = CSVField * ;
#endif

    using reference = CSVField & ;
    using iterator_category = std::random_access_iterator_tag;
#endif

    iterator(const CSVRow*, int i);

    reference operator*() const;
    pointer operator->() const;

    iterator operator++(int);
    iterator& operator++();
    iterator operator--(int);
    iterator& operator--();
    iterator operator+(difference_type n) const;
    iterator operator-(difference_type n) const;

    bool operator==(const iterator&) const;
    bool operator!=(const iterator& other) const { return !operator==(other); }

#ifndef NDEBUG
    friend CSVRow;
#endif

   private:
    const CSVRow * daddy = nullptr;            // Pointer to parent
    std::shared_ptr<CSVField> field = nullptr; // Current field pointed at
    int i = 0;                                 // Index of current field
  };

  /** @brief A reverse iterator over the contents of a CSVRow. */
  using reverse_iterator = std::reverse_iterator<iterator>;

  /** @name Iterators
   *  @brief Each iterator points to a CSVField object.
   */
  ///@{
  iterator begin() const;
  iterator end() const;
  reverse_iterator rbegin() const;
  reverse_iterator rend() const;
  ///@}

 private:
  /** Get the index in CSVRow's text buffer where the n-th field begins */
  unsigned short split_at(size_t n) const;

  internals::BufferPtr buffer = nullptr; /**< Memory buffer containing data for this row. */
  csv::string_view row_str = "";         /**< Text data for this row */
  size_t start;                          /**< Where in split buffer this row begins */
  unsigned short n_cols;                 /**< Numbers of columns this row has */
};

#pragma region CSVField::get Specializations
/** Retrieve this field's original string */
template<>
inline std::string CSVField::get<std::string>() {
  return std::string(this->sv);
}

/** Retrieve a view over this field's string
 *
 *  @warning This string_view is only guaranteed to be valid as long as this
 *           CSVRow is still alive.
 */
template<>
CONSTEXPR csv::string_view CSVField::get<csv::string_view>() {
  return this->sv;
}

/** Retrieve this field's value as a long double */
template<>
CONSTEXPR long double CSVField::get<long double>() {
  if (!is_num())
    throw std::runtime_error(internals::ERROR_NAN);

  return this->value;
}
#pragma endregion CSVField::get Specializations

/** Compares the contents of this field to a string */
template<>
inline bool CSVField::operator==(const char * other) const
{
  return this->sv == other;
}

/** Compares the contents of this field to a string */
template<>
inline bool CSVField::operator==(csv::string_view other) const
{
  return this->sv == other;
}
}

inline std::ostream& operator << (std::ostream& os, csv::CSVField const& value) {
  os << std::string(value);
  return os;
}
/** @file
 *  @brief Defines functionality needed for basic CSV parsing
 */

#include <array>
#include <deque>
#include <iterator>
#include <memory>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <string>
#include <vector>


/** The all encompassing namespace */
namespace csv {
/** Integer indicating a requested column wasn't found. */
constexpr int CSV_NOT_FOUND = -1;

/** Stuff that is generally not of interest to end-users */
namespace internals {
std::string type_name(const DataType& dtype);
std::string format_row(const std::vector<std::string>& row, csv::string_view delim = ", ");
}

/** @class CSVReader
 *  @brief Main class for parsing CSVs from files and in-memory sources
 *
 *  All rows are compared to the column names for length consistency
 *  - By default, rows that are too short or too long are dropped
 *  - Custom behavior can be defined by overriding bad_row_handler in a subclass
 */
class CSVReader {
 public:
  /**
   * An input iterator capable of handling large files.
   * @note Created by CSVReader::begin() and CSVReader::end().
   *
   * @par Iterating over a file
   * @snippet tests/test_csv_iterator.cpp CSVReader Iterator 1
   *
   * @par Using with `<algorithm>` library
   * @snippet tests/test_csv_iterator.cpp CSVReader Iterator 2
   */
  class iterator {
   public:
#ifndef DOXYGEN_SHOULD_SKIP_THIS
    using value_type = CSVRow;
    using difference_type = std::ptrdiff_t;
    using pointer = CSVRow * ;
    using reference = CSVRow & ;
    using iterator_category = std::input_iterator_tag;
#endif

    iterator() = default;
    iterator(CSVReader* reader) : daddy(reader) {};
    iterator(CSVReader*, CSVRow&&);

    reference operator*();
    pointer operator->();
    iterator& operator++();   /**< Pre-increment iterator */
    iterator operator++(int); /**< Post-increment ierator */
    iterator& operator--();

    bool operator==(const iterator&) const;
    bool operator!=(const iterator& other) const { return !operator==(other); }

   private:
    CSVReader * daddy = nullptr;  // Pointer to parent
    CSVRow row;                   // Current row
    RowCount i = 0;               // Index of current row
  };

  /** @name Constructors
   *  Constructors for iterating over large files and parsing in-memory sources.
   */
  ///@{
  CSVReader(csv::string_view filename, CSVFormat format = CSVFormat::GUESS_CSV);
  CSVReader(CSVFormat format = CSVFormat());
  ///@}

  CSVReader(const CSVReader&) = delete; // No copy constructor
  CSVReader(CSVReader&&) = default;     // Move constructor
  CSVReader& operator=(const CSVReader&) = delete; // No copy assignment
  CSVReader& operator=(CSVReader&& other) = default;
  ~CSVReader() { this->close(); }

  /** @name Reading In-Memory Strings
   *  You can piece together incomplete CSV fragments by calling feed() on them
   *  before finally calling end_feed().
   *
   *  Alternatively, you can also use the parse() shorthand function for
   *  smaller strings.
   */
  ///@{
  void feed(csv::string_view in);
  void end_feed();
  ///@}

  /** @name Retrieving CSV Rows */
  ///@{
  bool read_row(CSVRow &row);
  iterator begin();
  HEDLEY_CONST iterator end() const;
  ///@}

  /** @name CSV Metadata */
  ///@{
  CSVFormat get_format() const;
  std::vector<std::string> get_col_names() const;
  int index_of(csv::string_view col_name) const;
  ///@}

  /** @name CSV Metadata: Attributes */
  ///@{
  RowCount row_num = 0;        /**< How many lines have been parsed so far */
  RowCount correct_rows = 0;   /**< How many correct rows (minus header)
                                      *   have been parsed so far
                                      */
  bool utf8_bom = false;       /**< Set to true if UTF-8 BOM was detected */
  ///@}

  void close();

  friend CSVCollection parse(csv::string_view, CSVFormat);
 protected:
  /**
   * \defgroup csv_internal CSV Parser Internals
   * @brief Internals of CSVReader. Only maintainers and those looking to
   *        extend the parser should read this.
   * @{
   */

  /**  @typedef ParseFlags
   *   An enum used for describing the significance of each character
   *   with respect to CSV parsing
   */
  enum ParseFlags {
    NOT_SPECIAL, /**< Characters with no special meaning */
    QUOTE,       /**< Characters which may signify a quote escape */
    DELIMITER,   /**< Characters which may signify a new field */
    NEWLINE      /**< Characters which may signify a new row */
  };

  /** A string buffer and its size. Consumed by read_csv_worker(). */
  using WorkItem = std::pair<std::unique_ptr<char[]>, size_t>;

  /** Create a vector v where each index i corresponds to the
   *  ASCII number for a character and, v[i + 128] labels it according to
   *  the CSVReader::ParseFlags enum
   */
  HEDLEY_CONST CONSTEXPR
  std::array<CSVReader::ParseFlags, 256> make_parse_flags() const;

  /** Create a vector v where each index i corresponds to the
   *  ASCII number for a character c and, v[i + 128] is true if
   *  c is a whitespace character
   */
  HEDLEY_CONST CONSTEXPR
  std::array<bool, 256> make_ws_flags(const char * delims, size_t n_chars) const;

  /** Open a file for reading. Implementation is compiler specific. */
  void fopen(csv::string_view filename);

  /** Sets this reader's column names and associated data */
  void set_col_names(const std::vector<std::string>&);

  /** Returns true if we have reached end of file */
  bool eof() { return !(this->infile); };

  /** Buffer for current row being parsed */
  internals::BufferPtr record_buffer = internals::BufferPtr(new internals::RawRowBuffer());

  /** Queue of parsed CSV rows */
  std::deque<CSVRow> records;

  /** @name CSV Parsing Callbacks
   *  The heart of the CSV parser.
   *  These methods are called by feed().
   */
  ///@{
  void write_record();

  /** Handles possible Unicode byte order mark */
  CONSTEXPR void handle_unicode_bom(csv::string_view& in);
  virtual void bad_row_handler(std::vector<std::string>);
  ///@}

  /** @name CSV Settings **/
  ///@{
  char delimiter;         /**< Delimiter character */
  char quote_char;        /**< Quote character */
  int header_row;         /**< Line number of the header row (zero-indexed) */
  bool strict = false;    /**< Strictness of parser */

  /** An array where the (i + 128)th slot gives the ParseFlags for ASCII character i */
  std::array<ParseFlags, 256> parse_flags;

  /** An array where the (i + 128)th slot determines whether ASCII character i should
   *  be trimmed
   */
  std::array<bool, 256> ws_flags;
  ///@}

  /** @name Parser State */
  ///@{
  /** Pointer to a object containing column information */
  internals::ColNamesPtr col_names = std::make_shared<internals::ColNames>(
      std::vector<std::string>({}));

  /** Whether or not an attempt to find Unicode BOM has been made */
  bool unicode_bom_scan = false;

  /** Whether or not we have parsed the header row */
  bool header_was_parsed = false;

  /** The number of columns in this CSV */
  size_t n_cols = 0;
  ///@}

  /** @name Multi-Threaded File Reading Functions */
  ///@{
  void feed(WorkItem&&); /**< @brief Helper for read_csv_worker() */
  void read_csv(const size_t& bytes = internals::ITERATION_CHUNK_SIZE);
  void read_csv_worker();
  ///@}

  /** @name Multi-Threaded File Reading: Flags and State */
  ///@{
  std::FILE* HEDLEY_RESTRICT
      infile = nullptr;                /**< Current file handle.
                                                  Destroyed by ~CSVReader(). */
  std::deque<WorkItem> feed_buffer;    /**< Message queue for worker */
  std::mutex feed_lock;                /**< Allow only one worker to write */
  std::condition_variable feed_cond;   /**< Wake up worker */
  ///@}

  /**@}*/ // End of parser internals
};

namespace internals {
/** Class for guessing the delimiter & header row number of CSV files */
class CSVGuesser {

  /** Private subclass of csv::CSVReader which performs statistics
   *  on row lengths
   */
  struct Guesser : public CSVReader {
    using CSVReader::CSVReader;
    void bad_row_handler(std::vector<std::string> record) override;
    friend CSVGuesser;

    // Frequency counter of row length
    std::unordered_map<size_t, size_t> row_tally = { { 0, 0 } };

    // Map row lengths to row num where they first occurred
    std::unordered_map<size_t, size_t> row_when = { { 0, 0 } };
  };

 public:
  CSVGuesser(csv::string_view _filename, const std::vector<char>& _delims) :
      filename(_filename), delims(_delims) {};
  CSVFormat guess_delim();
  bool first_guess();
  void second_guess();

 private:
  std::string filename;      /**< File to read */
  std::string head;          /**< First x bytes of file */
  std::vector<char> delims;  /**< Candidate delimiters */

  char delim;                /**< Chosen delimiter (set by guess_delim()) */
  int header_row = 0;        /**< Chosen header row (set by guess_delim()) */

  void get_csv_head();       /**< Retrieve the first x bytes of a file */
};
}
}
/** @file
 *  Calculates statistics from CSV files
 */

#include <unordered_map>
#include <vector>

namespace csv {
/** @class CSVStat
 *  @brief Class for calculating statistics from CSV files and in-memory sources
 *
 *  **Example**
 *  \include programs/csv_stats.cpp
 *
 */
class CSVStat : public CSVReader {
 public:
  using FreqCount = std::unordered_map<std::string, RowCount>;
  using TypeCount = std::unordered_map<DataType, RowCount>;

  void end_feed();
  std::vector<long double> get_mean() const;
  std::vector<long double> get_variance() const;
  std::vector<long double> get_mins() const;
  std::vector<long double> get_maxes() const;
  std::vector<FreqCount> get_counts() const;
  std::vector<TypeCount> get_dtypes() const;

  CSVStat(std::string filename, CSVFormat format = CSVFormat::GUESS_CSV);
  CSVStat(CSVFormat format = CSVFormat()) : CSVReader(format) {};
 private:
  // An array of rolling averages
  // Each index corresponds to the rolling mean for the column at said index
  std::vector<long double> rolling_means;
  std::vector<long double> rolling_vars;
  std::vector<long double> mins;
  std::vector<long double> maxes;
  std::vector<FreqCount> counts;
  std::vector<TypeCount> dtypes;
  std::vector<long double> n;

  // Statistic calculators
  void variance(const long double&, const size_t&);
  void count(CSVField&, const size_t&);
  void min_max(const long double&, const size_t&);
  void dtype(CSVField&, const size_t&);

  void calc();
  void calc_worker(const size_t&);
};
}

/** @file
 *  Defines an object used to store CSV format settings
 */


namespace csv {
CSVFormat create_default_csv_strict() {
  CSVFormat format;
  format.delimiter(',')
      .quote('"')
      .header_row(0)
      .detect_bom(true)
      .strict_parsing(true);

  return format;
}

CSVFormat create_guess_csv() {
  CSVFormat format;
  format.delimiter({ ',', '|', '\t', ';', '^' })
      .quote('"')
      .header_row(0)
      .detect_bom(true);

  return format;
}

const CSVFormat CSVFormat::RFC4180_STRICT = create_default_csv_strict();
const CSVFormat CSVFormat::GUESS_CSV = create_guess_csv();

CSVFormat& CSVFormat::delimiter(char delim) {
  this->possible_delimiters = { delim };
  return *this;
}

CSVFormat& CSVFormat::delimiter(const std::vector<char> & delim) {
  this->possible_delimiters = delim;
  return *this;
}

CSVFormat& CSVFormat::quote(char quote) {
  this->quote_char = quote;
  return *this;
}

CSVFormat& CSVFormat::trim(const std::vector<char> & chars) {
  this->trim_chars = chars;
  return *this;
}

CSVFormat& CSVFormat::column_names(const std::vector<std::string>& names) {
  this->col_names = names;
  this->header = -1;
  return *this;
}

CSVFormat& CSVFormat::header_row(int row) {
  this->header = row;
  this->col_names = {};
  return *this;
}

CSVFormat& CSVFormat::strict_parsing(bool throw_error) {
  this->strict = throw_error;
  return *this;
}

CSVFormat& CSVFormat::detect_bom(bool detect) {
  this->unicode_detect = detect;
  return *this;
}
}
/** @file
 *  @brief Defines functionality needed for basic CSV parsing
 */

#include <algorithm>
#include <cstdio>   // For read_csv()
#include <cstring>  // For read_csv()
#include <fstream>
#include <sstream>


namespace csv {
namespace internals {
std::string format_row(const std::vector<std::string>& row, csv::string_view delim) {
  /** Print a CSV row */
  std::stringstream ret;
  for (size_t i = 0; i < row.size(); i++) {
    ret << row[i];
    if (i + 1 < row.size()) ret << delim;
    else ret << std::endl;
  }

  return ret.str();
}

//
// CSVGuesser
//
void CSVGuesser::Guesser::bad_row_handler(std::vector<std::string> record) {
  /** Helps CSVGuesser tally up the size of rows encountered while parsing */
  if (row_tally.find(record.size()) != row_tally.end()) row_tally[record.size()]++;
  else {
    row_tally[record.size()] = 1;
    row_when[record.size()] = this->row_num + 1;
  }
}

CSVFormat CSVGuesser::guess_delim() {
  /** Guess the delimiter of a CSV by scanning the first 100 lines by
  *  First assuming that the header is on the first row
  *  If the first guess returns too few rows, then we move to the second
  *  guess method
  */
  CSVFormat format;
  if (!first_guess()) second_guess();

  return format.delimiter(this->delim).header_row(this->header_row);
}

bool CSVGuesser::first_guess() {
  /** Guess the delimiter of a delimiter separated values file
   *  by scanning the first 100 lines
   *
   *  - "Winner" is based on which delimiter has the most number
   *    of correctly parsed rows + largest number of columns
   *  -  **Note:** Assumes that whatever the dialect, all records
   *     are newline separated
   *
   *  Returns True if guess was a good one and second guess isn't needed
   */

  CSVFormat format;
  char current_delim{ ',' };
  RowCount max_rows = 0,
      temp_rows = 0;
  size_t max_cols = 0;

  // Read first 500KB of the CSV file
  this->get_csv_head();

  for (char cand_delim: this->delims) {
    format.delimiter(cand_delim);
    CSVReader guesser(format);
    guesser.feed(this->head);
    guesser.end_feed();

    // WORKAROUND on Unix systems because certain newlines
    // get double counted
    // temp_rows = guesser.correct_rows;
    temp_rows = std::min(guesser.correct_rows, (RowCount)100);
    if ((guesser.row_num >= max_rows) &&
        (guesser.get_col_names().size() > max_cols)) {
      max_rows = temp_rows;
      max_cols = guesser.get_col_names().size();
      current_delim = cand_delim;
    }
  }

  this->delim = current_delim;

  // If there are only a few rows/columns, trying guessing again
  return (max_rows > 10 && max_cols > 2);
}

void CSVGuesser::second_guess() {
  /** For each delimiter, find out which row length was most common.
   *  The delimiter with the longest mode row length wins.
   *  Then, the line number of the header row is the first row with
   *  the mode row length.
   */

  CSVFormat format;
  size_t max_rlen = 0,
      header = 0;

  for (char cand_delim: this->delims) {
    format.delimiter(cand_delim);
    Guesser guess(format);
    guess.feed(this->head);
    guess.end_feed();

    // Most common row length
    auto max = std::max_element(guess.row_tally.begin(), guess.row_tally.end(),
                                [](const std::pair<size_t, size_t>& x,
                                   const std::pair<size_t, size_t>& y) {
                                  return x.second < y.second; });

    // Idea: If CSV has leading comments, actual rows don't start
    // until later and all actual rows get rejected because the CSV
    // parser mistakenly uses the .size() of the comment rows to
    // judge whether or not they are valid.
    //
    // The first part of the if clause means we only change the header
    // row if (number of rejected rows) > (number of actual rows)
    if (max->second > guess.records.size() &&
        (max->first > max_rlen)) {
      max_rlen = max->first;
      header = guess.row_when[max_rlen];
    }
  }

  this->header_row = static_cast<int>(header);
}

/** Read the first 500KB of a CSV file */
void CSVGuesser::get_csv_head() {
  const size_t bytes = 500000;
  std::ifstream infile(this->filename);
  if (!infile.is_open()) {
    throw std::runtime_error("Cannot open file " + this->filename);
  }

  std::unique_ptr<char[]> buffer(new char[bytes + 1]);
  char * head_buffer = buffer.get();

  for (size_t i = 0; i < bytes + 1; i++) {
    head_buffer[i] = '\0';
  }

  infile.read(head_buffer, bytes);
  this->head = head_buffer;
}
}

/** Guess the delimiter used by a delimiter-separated values file */
CSVFormat guess_format(csv::string_view filename, const std::vector<char>& delims) {
  internals::CSVGuesser guesser(filename, delims);
  return guesser.guess_delim();
}

HEDLEY_CONST CONSTEXPR
std::array<CSVReader::ParseFlags, 256> CSVReader::make_parse_flags() const {
  std::array<ParseFlags, 256> ret = {};
  for (int i = -128; i < 128; i++) {
    const int arr_idx = i + 128;
    char ch = char(i);

    if (ch == this->delimiter)
      ret[arr_idx] = DELIMITER;
    else if (ch == this->quote_char)
      ret[arr_idx] = QUOTE;
    else if (ch == '\r' || ch == '\n')
      ret[arr_idx] = NEWLINE;
    else
      ret[arr_idx] = NOT_SPECIAL;
  }

  return ret;
}

HEDLEY_CONST CONSTEXPR
std::array<bool, 256> CSVReader::make_ws_flags(const char * delims, size_t n_chars) const {
  std::array<bool, 256> ret = {};
  for (int i = -128; i < 128; i++) {
    const int arr_idx = i + 128;
    char ch = char(i);
    ret[arr_idx] = false;

    for (size_t j = 0; j < n_chars; j++) {
      if (delims[j] == ch) {
        ret[arr_idx] = true;
      }
    }
  }

  return ret;
}

void CSVReader::bad_row_handler(std::vector<std::string> record) {
  /** Handler for rejected rows (too short or too long). This does nothing
   *  unless strict parsing was set, in which case it throws an eror.
   *  Subclasses of CSVReader may easily override this to provide
   *  custom behavior.
   */
  if (this->strict) {
    std::string problem;
    if (record.size() > this->col_names->size()) problem = "too long";
    else problem = "too short";

    throw std::runtime_error("Line " + problem + " around line " +
        std::to_string(correct_rows) + " near\n" +
        internals::format_row(record)
    );
  }
};

/** Allows parsing in-memory sources (by calling feed() and end_feed()). */
CSVReader::CSVReader(CSVFormat format) :
    delimiter(format.get_delim()), quote_char(format.quote_char),
    header_row(format.header), strict(format.strict),
    unicode_bom_scan(!format.unicode_detect) {
  if (!format.col_names.empty()) {
    this->set_col_names(format.col_names);
  }

  parse_flags = this->make_parse_flags();
  ws_flags = this->make_ws_flags(format.trim_chars.data(), format.trim_chars.size());
};

/** Allows reading a CSV file in chunks, using overlapped
 *  threads for simulatenously reading from disk and parsing.
 *  Rows should be retrieved with read_row() or by using
 *  CSVReader::iterator.
 *
 *  **Details:** Reads the first 500kB of a CSV file to infer file information
 *              such as column names and delimiting character.
 *
 *  @param[in] filename  Path to CSV file
 *  @param[in] format    Format of the CSV file
 *
 *  \snippet tests/test_read_csv.cpp CSVField Example
 *
 */
CSVReader::CSVReader(csv::string_view filename, CSVFormat format) {
  if (format.guess_delim())
    format = guess_format(filename, format.possible_delimiters);

  if (!format.col_names.empty()) {
    this->set_col_names(format.col_names);
  }
  else {
    header_row = format.header;
  }

  delimiter = format.get_delim();
  quote_char = format.quote_char;
  strict = format.strict;
  parse_flags = this->make_parse_flags();
  ws_flags = this->make_ws_flags(format.trim_chars.data(), format.trim_chars.size());

  // Read first 500KB of CSV
  this->fopen(filename);
  this->read_csv(500000);
}

/** Return the format of the original raw CSV */
CSVFormat CSVReader::get_format() const {
  CSVFormat format;
  format.delimiter(this->delimiter)
      .quote(this->quote_char)
      .header_row(this->header_row)
      .column_names(this->col_names->col_names);

  return format;
}

/** Return the CSV's column names as a vector of strings. */
std::vector<std::string> CSVReader::get_col_names() const {
  return this->col_names->get_col_names();
}

/** Return the index of the column name if found or
 *         csv::CSV_NOT_FOUND otherwise.
 */
int CSVReader::index_of(csv::string_view col_name) const {
  auto _col_names = this->get_col_names();
  for (size_t i = 0; i < _col_names.size(); i++)
    if (_col_names[i] == col_name) return (int)i;

  return CSV_NOT_FOUND;
}

void CSVReader::feed(WorkItem&& buff) {
  this->feed( csv::string_view(buff.first.get(), buff.second) );
}

void CSVReader::feed(csv::string_view in) {
  /** Parse a CSV-formatted string.
   *
   *  @par Usage
   *  Incomplete CSV fragments can be joined together by calling feed() on them sequentially.
   *
   *  @note
   *  `end_feed()` should be called after the last string.
   */

  this->handle_unicode_bom(in);
  bool quote_escape = false;  // Are we currently in a quote escaped field?

  // Optimizations
  auto * HEDLEY_RESTRICT _parse_flags = this->parse_flags.data();
  auto * HEDLEY_RESTRICT _ws_flags = this->ws_flags.data();
  auto& row_buffer = *(this->record_buffer.get());
  auto& text_buffer = row_buffer.buffer;
  auto& split_buffer = row_buffer.split_buffer;
  text_buffer.reserve(in.size());
  split_buffer.reserve(in.size() / 10);

  const size_t in_size = in.size();
  for (size_t i = 0; i < in_size; i++) {
    switch (_parse_flags[in[i] + 128]) {
      case DELIMITER:
        if (!quote_escape) {
          split_buffer.push_back((unsigned short)row_buffer.size());
          break;
        }

        HEDLEY_FALL_THROUGH;
      case NEWLINE:
        if (!quote_escape) {
          // End of record -> Write record
          if (i + 1 < in_size && in[i + 1] == '\n') // Catches CRLF (or LFLF)
            ++i;
          this->write_record();
          break;
        }

        // Treat as regular character
        text_buffer += in[i];
        break;
      case NOT_SPECIAL: {
        size_t start, end;

        // Trim off leading whitespace
        while (i < in_size && _ws_flags[in[i] + 128]) {
          i++;
        }

        start = i;

        // Optimization: Since NOT_SPECIAL characters tend to occur in contiguous
        // sequences, use the loop below to avoid having to go through the outer
        // switch statement as much as possible
        while (i + 1 < in_size && _parse_flags[in[i + 1] + 128] == NOT_SPECIAL) {
          i++;
        }

        // Trim off trailing whitespace
        end = i;
        while (_ws_flags[in[end] + 128]) {
          end--;
        }

        // Finally append text
#ifdef CSV_HAS_CXX17
        text_buffer += in.substr(start, end - start + 1);
#else
        for (; start < end + 1; start++) {
                        text_buffer += in[start];
                    }
#endif

        break;
      }
      default: // Quote
        if (!quote_escape) {
          // Don't deref past beginning
          if (i && _parse_flags[in[i - 1] + 128] >= DELIMITER) {
            // Case: Previous character was delimiter or newline
            quote_escape = true;
          }

          break;
        }

        auto next_ch = _parse_flags[in[i + 1] + 128];
        if (next_ch >= DELIMITER) {
          // Case: Delim or newline => end of field
          quote_escape = false;
          break;
        }

        // Case: Escaped quote
        text_buffer += in[i];

        if (next_ch == QUOTE)
          ++i;  // Case: Two consecutive quotes
        else if (this->strict)
          throw std::runtime_error("Unescaped single quote around line " +
              std::to_string(this->correct_rows) + " near:\n" +
              std::string(in.substr(i, 100)));

        break;
    }
  }

  this->record_buffer = row_buffer.reset();
}

void CSVReader::end_feed() {
  /** Indicate that there is no more data to receive,
   *  and handle the last row
   */
  this->write_record();
}

CONSTEXPR void CSVReader::handle_unicode_bom(csv::string_view& in) {
  if (!this->unicode_bom_scan) {
    if (in[0] == 0xEF && in[1] == 0xBB && in[2] == 0xEF) {
      in.remove_prefix(3); // Remove BOM from input string
      this->utf8_bom = true;
    }

    this->unicode_bom_scan = true;
  }
}

void CSVReader::write_record() {
  /** Push the current row into a queue if it is the right length.
   *  Drop it otherwise.
   */

  if (header_was_parsed) {
    // Make sure record is of the right length
    const size_t row_size = this->record_buffer->splits_size();
    if (row_size + 1 == this->n_cols) {
      this->correct_rows++;
      this->records.push_back(CSVRow(this->record_buffer));
    }
    else {
      /* 1) Zero-length record, probably caused by extraneous newlines
       * 2) Too short or too long
       */
      this->row_num--;
      if (row_size > 0)
        bad_row_handler(std::vector<std::string>(CSVRow(
            this->record_buffer)));
    }
  }
  else if (this->row_num == this->header_row) {
    this->set_col_names(std::vector<std::string>(CSVRow(this->record_buffer)));
  } // else: Ignore rows before header row

  this->row_num++;
}

void CSVReader::read_csv_worker() {
  /** Worker thread for read_csv() which parses CSV rows (while the main
   *         thread pulls data from disk)
   */
  while (true) {
    std::unique_lock<std::mutex> lock{ this->feed_lock }; // Get lock
    this->feed_cond.wait(lock,                            // Wait
                         [this] { return !(this->feed_buffer.empty()); });

    // Wake-up
    auto in = std::move(this->feed_buffer.front());
    this->feed_buffer.pop_front();

    // Nullptr --> Die
    if (!in.first) break;

    lock.unlock();      // Release lock
    this->feed(std::move(in));
  }
}

void CSVReader::fopen(csv::string_view filename) {
  if (!this->infile) {
#ifdef _MSC_BUILD
    // Silence compiler warnings in Microsoft Visual C++
            size_t err = fopen_s(&(this->infile), filename.data(), "rb");
            if (err)
                throw std::runtime_error("Cannot open file " + std::string(filename));
#else
    this->infile = std::fopen(filename.data(), "rb");
    if (!this->infile)
      throw std::runtime_error("Cannot open file " + std::string(filename));
#endif
  }
}

/**
 *  @param[in] names Column names
 */
void CSVReader::set_col_names(const std::vector<std::string>& names)
{
  this->col_names = std::make_shared<internals::ColNames>(names);
  this->record_buffer->col_names = this->col_names;
  this->header_was_parsed = true;
  this->n_cols = names.size();
}

/**
 * Parse a CSV file using multiple threads
 *
 * @pre CSVReader::infile points to a valid file handle, i.e. CSVReader::fopen was called
 *
 * @param[in] bytes Number of bytes to read.
 * @see CSVReader::read_row()
 */
void CSVReader::read_csv(const size_t& bytes) {
  const size_t BUFFER_UPPER_LIMIT = std::min(bytes, (size_t)1000000);
  std::unique_ptr<char[]> buffer(new char[BUFFER_UPPER_LIMIT]);
  auto * HEDLEY_RESTRICT line_buffer = buffer.get();
  line_buffer[0] = '\0';

  std::thread worker(&CSVReader::read_csv_worker, this);

  for (size_t processed = 0; processed < bytes; ) {
    char * HEDLEY_RESTRICT result = std::fgets(line_buffer, internals::PAGE_SIZE, this->infile);
    if (result == NULL) break;
    line_buffer += std::strlen(line_buffer);
    size_t current_strlen = line_buffer - buffer.get();

    if (current_strlen >= 0.9 * BUFFER_UPPER_LIMIT) {
      processed += (line_buffer - buffer.get());
      std::unique_lock<std::mutex> lock{ this->feed_lock };

      this->feed_buffer.push_back(std::make_pair<>(std::move(buffer), current_strlen));

      buffer = std::unique_ptr<char[]>(new char[BUFFER_UPPER_LIMIT]); // New pointer
      line_buffer = buffer.get();
      line_buffer[0] = '\0';

      this->feed_cond.notify_one();
    }
  }

  // Feed remaining bits
  std::unique_lock<std::mutex> lock{ this->feed_lock };
  this->feed_buffer.push_back(std::make_pair<>(std::move(buffer), line_buffer - buffer.get()));
  this->feed_buffer.push_back(std::make_pair<>(nullptr, 0)); // Termination signal
  this->feed_cond.notify_one();
  lock.unlock();
  worker.join();

  if (std::feof(this->infile)) {
    this->end_feed();
    this->close();
  }
}

/** Close the open file handle.
 *
 *  @note Automatically called by ~CSVReader().
 */
void CSVReader::close() {
  if (this->infile) {
    std::fclose(this->infile);
    this->infile = nullptr;
  }
}

/**
 * Retrieve rows as CSVRow objects, returning true if more rows are available.
 *
 * **Performance Notes**:
 *  - The number of rows read in at a time is determined by csv::ITERATION_CHUNK_SIZE
 *  - For performance details, read the documentation for CSVRow and CSVField.
 *
 * @param[out] row The variable where the parsed row will be stored
 * @see CSVRow, CSVField
 *
 * **Example:**
 * \snippet tests/test_read_csv.cpp CSVField Example
 *
 */
bool CSVReader::read_row(CSVRow &row) {
  if (this->records.empty()) {
    if (!this->eof()) {
      // TODO/Suggestion: Make this call non-blocking,
      // i.e. move to it another thread
      this->read_csv(internals::ITERATION_CHUNK_SIZE);
    }
    else return false; // Stop reading
  }

  row = std::move(this->records.front());
  this->records.pop_front();

  return true;
}
}
/** @file
 *  @brief Defines an input iterator for csv::CSVReader
 */


namespace csv {
/** Return an iterator to the first row in the reader */
CSVReader::iterator CSVReader::begin() {
  CSVReader::iterator ret(this, std::move(this->records.front()));
  this->records.pop_front();
  return ret;
}

/** A placeholder for the imaginary past the end row in a CSV.
 *  Attempting to deference this will lead to bad things.
 */
HEDLEY_CONST CSVReader::iterator CSVReader::end() const {
  return CSVReader::iterator();
}

/////////////////////////
// CSVReader::iterator //
/////////////////////////

CSVReader::iterator::iterator(CSVReader* _daddy, CSVRow&& _row) :
    daddy(_daddy) {
  row = std::move(_row);
}

/** @brief Access the CSVRow held by the iterator */
CSVReader::iterator::reference CSVReader::iterator::operator*() {
  return this->row;
}

/** @brief Return a pointer to the CSVRow the iterator has stopped at */
CSVReader::iterator::pointer CSVReader::iterator::operator->() {
  return &(this->row);
}

/** @brief Advance the iterator by one row. If this CSVReader has an
 *  associated file, then the iterator will lazily pull more data from
 *  that file until EOF.
 */
CSVReader::iterator& CSVReader::iterator::operator++() {
  if (!daddy->read_row(this->row)) {
    this->daddy = nullptr; // this == end()
  }

  return *this;
}

/** @brief Post-increment iterator */
CSVReader::iterator CSVReader::iterator::operator++(int) {
  auto temp = *this;
  if (!daddy->read_row(this->row)) {
    this->daddy = nullptr; // this == end()
  }

  return temp;
}

/** @brief Returns true if iterators were constructed from the same CSVReader
 *         and point to the same row
 */
bool CSVReader::iterator::operator==(const CSVReader::iterator& other) const {
  return (this->daddy == other.daddy) && (this->i == other.i);
}
}
/** @file
 *  Defines the data type used for storing information about a CSV row
 */

#include <cassert>
#include <functional>

namespace csv {
/** Return a string view of the nth field
 *
 *  @complexity
 *  Constant
 *
 *  @throws
 *  std::runtime_error If n is out of bounds
 */
csv::string_view CSVRow::get_string_view(size_t n) const {
  csv::string_view ret(this->row_str);
  size_t beg = 0,
      end = 0,
      r_size = this->size();

  if (n >= r_size)
    throw std::runtime_error("Index out of bounds.");

  if (r_size > 1) {
    if (n == 0) {
      end = this->split_at(0);
    }
    else if (r_size == 2) {
      beg = this->split_at(0);
    }
    else {
      beg = this->split_at(n - 1);
      if (n != r_size - 1) end = this->split_at(n);
    }
  }

  // Performance optimization
  if (end == 0) {
    end = row_str.size();
  }

  return ret.substr(
      beg,
      end - beg // Number of characters
  );
}

/** Return a CSVField object corrsponding to the nth value in the row.
 *
 *  @note This method performs bounds checking, and will throw an
 *        `std::runtime_error` if n is invalid.
 *
 *  @complexity
 *  Constant, by calling csv::CSVRow::get_csv::string_view()
 *
 */
CSVField CSVRow::operator[](size_t n) const {
  return CSVField(this->get_string_view(n));
}

/** Retrieve a value by its associated column name. If the column
 *  specified can't be round, a runtime error is thrown.
 *
 *  @complexity
 *  Constant. This calls the other CSVRow::operator[]() after
 *  converting column names into indices using a hash table.
 *
 *  @param[in] col_name The column to look for
 */
CSVField CSVRow::operator[](const std::string& col_name) const {
  auto & col_names = this->buffer->col_names;
  auto col_pos = col_names->col_pos.find(col_name);
  if (col_pos != col_names->col_pos.end())
    return this->operator[](col_pos->second);

  throw std::runtime_error("Can't find a column named " + col_name);
}

CSVRow::operator std::vector<std::string>() const {

  std::vector<std::string> ret;
  for (size_t i = 0; i < size(); i++)
    ret.push_back(std::string(this->get_string_view(i)));

  return ret;
}

#pragma region CSVRow Iterator
/** Return an iterator pointing to the first field. */
CSVRow::iterator CSVRow::begin() const {
  return CSVRow::iterator(this, 0);
}

/** Return an iterator pointing to just after the end of the CSVRow.
 *
 *  @warning Attempting to dereference the end iterator results
 *           in dereferencing a null pointer.
 */
CSVRow::iterator CSVRow::end() const {
  return CSVRow::iterator(this, (int)this->size());
}

CSVRow::reverse_iterator CSVRow::rbegin() const {
  return std::reverse_iterator<CSVRow::iterator>(this->end());
}

CSVRow::reverse_iterator CSVRow::rend() const {
  return std::reverse_iterator<CSVRow::iterator>(this->begin());
}

unsigned short CSVRow::split_at(size_t n) const
{
  return this->buffer->split_buffer[this->start + n];
}

HEDLEY_NON_NULL(1)
CSVRow::iterator::iterator(const CSVRow* _reader, int _i)
    : daddy(_reader), i(_i) {
  if (_i < (int)this->daddy->size())
    this->field = std::make_shared<CSVField>(
        this->daddy->operator[](_i));
  else
    this->field = nullptr;
}

CSVRow::iterator::reference CSVRow::iterator::operator*() const {
  return *(this->field.get());
}

CSVRow::iterator::pointer CSVRow::iterator::operator->() const {
  // Using CSVField * as pointer type causes segfaults in MSVC debug builds
#ifdef _MSC_BUILD
  return this->field;
#else
  return this->field.get();
#endif
}

CSVRow::iterator& CSVRow::iterator::operator++() {
  // Pre-increment operator
  this->i++;
  if (this->i < (int)this->daddy->size())
    this->field = std::make_shared<CSVField>(
        this->daddy->operator[](i));
  else // Reached the end of row
    this->field = nullptr;
  return *this;
}

CSVRow::iterator CSVRow::iterator::operator++(int) {
  // Post-increment operator
  auto temp = *this;
  this->operator++();
  return temp;
}

CSVRow::iterator& CSVRow::iterator::operator--() {
  // Pre-decrement operator
  this->i--;
  this->field = std::make_shared<CSVField>(
      this->daddy->operator[](this->i));
  return *this;
}

CSVRow::iterator CSVRow::iterator::operator--(int) {
  // Post-decrement operator
  auto temp = *this;
  this->operator--();
  return temp;
}

CSVRow::iterator CSVRow::iterator::operator+(difference_type n) const {
  // Allows for iterator arithmetic
  return CSVRow::iterator(this->daddy, i + (int)n);
}

CSVRow::iterator CSVRow::iterator::operator-(difference_type n) const {
  // Allows for iterator arithmetic
  return CSVRow::iterator::operator+(-n);
}

/** Two iterators are equal if they point to the same field */
bool CSVRow::iterator::operator==(const iterator& other) const {
  return this->i == other.i;
}
#pragma endregion CSVRow Iterator
}
/** @file
 *  Calculates statistics from CSV files
 */

#include <string>

namespace csv {
CSVStat::CSVStat(std::string filename, CSVFormat format) :
    CSVReader(filename, format) {
  /** Lazily calculate statistics for a potentially large file. Once this constructor
   *  is called, CSVStat will process the entire file iteratively. Once finished,
   *  methods like get_mean(), get_counts(), etc... can be used to retrieve statistics.
   */
  while (!this->eof()) {
    this->read_csv(internals::ITERATION_CHUNK_SIZE);
    this->calc();
  }

  if (!this->records.empty())
    this->calc();
}

void CSVStat::end_feed() {
  CSVReader::end_feed();
  this->calc();
}

/** @brief Return current means */
std::vector<long double> CSVStat::get_mean() const {
  std::vector<long double> ret;
  for (size_t i = 0; i < this->col_names->size(); i++) {
    ret.push_back(this->rolling_means[i]);
  }
  return ret;
}

/** @brief Return current variances */
std::vector<long double> CSVStat::get_variance() const {
  std::vector<long double> ret;
  for (size_t i = 0; i < this->col_names->size(); i++) {
    ret.push_back(this->rolling_vars[i]/(this->n[i] - 1));
  }
  return ret;
}

/** @brief Return current mins */
std::vector<long double> CSVStat::get_mins() const {
  std::vector<long double> ret;
  for (size_t i = 0; i < this->col_names->size(); i++) {
    ret.push_back(this->mins[i]);
  }
  return ret;
}

/** @brief Return current maxes */
std::vector<long double> CSVStat::get_maxes() const {
  std::vector<long double> ret;
  for (size_t i = 0; i < this->col_names->size(); i++) {
    ret.push_back(this->maxes[i]);
  }
  return ret;
}

/** @brief Get counts for each column */
std::vector<CSVStat::FreqCount> CSVStat::get_counts() const {
  std::vector<FreqCount> ret;
  for (size_t i = 0; i < this->col_names->size(); i++) {
    ret.push_back(this->counts[i]);
  }
  return ret;
}

/** @brief Get data type counts for each column */
std::vector<CSVStat::TypeCount> CSVStat::get_dtypes() const {
  std::vector<TypeCount> ret;
  for (size_t i = 0; i < this->col_names->size(); i++) {
    ret.push_back(this->dtypes[i]);
  }
  return ret;
}

void CSVStat::calc() {
  /** Go through all records and calculate specified statistics */
  for (size_t i = 0; i < this->col_names->size(); i++) {
    dtypes.push_back({});
    counts.push_back({});
    rolling_means.push_back(0);
    rolling_vars.push_back(0);
    mins.push_back(NAN);
    maxes.push_back(NAN);
    n.push_back(0);
  }

  std::vector<std::thread> pool;

  // Start threads
  for (size_t i = 0; i < this->col_names->size(); i++)
    pool.push_back(std::thread(&CSVStat::calc_worker, this, i));

  // Block until done
  for (auto& th: pool)
    th.join();

  this->records.clear();
}

void CSVStat::calc_worker(const size_t &i) {
  /** Worker thread for CSVStat::calc() which calculates statistics for one column.
   *
   *  @param[in] i Column index
   */

  auto current_record = this->records.begin();
  for (size_t processed = 0; current_record != this->records.end(); processed++) {
    auto current_field = (*current_record)[i];

    // Optimization: Don't count() if there's too many distinct values in the first 1000 rows
    if (processed < 1000 || this->counts[i].size() <= 500)
      this->count(current_field, i);

    this->dtype(current_field, i);

    // Numeric Stuff
    if (current_field.is_num()) {
      long double x_n = current_field.get<long double>();

      // This actually calculates mean AND variance
      this->variance(x_n, i);
      this->min_max(x_n, i);
    }

    ++current_record;
  }
}

void CSVStat::dtype(CSVField& data, const size_t &i) {
  /** Given a record update the type counter
   *  @param[in]  record Data observation
   *  @param[out] i      The column index that should be updated
   */

  auto type = data.type();
  if (this->dtypes[i].find(type) !=
      this->dtypes[i].end()) {
    // Increment count
    this->dtypes[i][type]++;
  } else {
    // Initialize count
    this->dtypes[i].insert(std::make_pair(type, 1));
  }
}

void CSVStat::count(CSVField& data, const size_t &i) {
  /** Given a record update the frequency counter
   *  @param[in]  record Data observation
   *  @param[out] i      The column index that should be updated
   */

  auto item = data.get<std::string>();

  if (this->counts[i].find(item) !=
      this->counts[i].end()) {
    // Increment count
    this->counts[i][item]++;
  } else {
    // Initialize count
    this->counts[i].insert(std::make_pair(item, 1));
  }
}

void CSVStat::min_max(const long double &x_n, const size_t &i) {
  /** Update current minimum and maximum
   *  @param[in]  x_n Data observation
   *  @param[out] i   The column index that should be updated
   */
  if (isnan(this->mins[i]))
    this->mins[i] = x_n;
  if (isnan(this->maxes[i]))
    this->maxes[i] = x_n;

  if (x_n < this->mins[i])
    this->mins[i] = x_n;
  else if (x_n > this->maxes[i])
    this->maxes[i] = x_n;
}

void CSVStat::variance(const long double &x_n, const size_t &i) {
  /** Given a record update rolling mean and variance for all columns
   *  using Welford's Algorithm
   *  @param[in]  x_n Data observation
   *  @param[out] i   The column index that should be updated
   */
  long double& current_rolling_mean = this->rolling_means[i];
  long double& current_rolling_var = this->rolling_vars[i];
  long double& current_n = this->n[i];
  long double delta;
  long double delta2;

  current_n++;

  if (current_n == 1) {
    current_rolling_mean = x_n;
  } else {
    delta = x_n - current_rolling_mean;
    current_rolling_mean += delta/current_n;
    delta2 = x_n - current_rolling_mean;
    current_rolling_var += delta*delta2;
  }
}

/** @brief Useful for uploading CSV files to SQL databases.
 *
 *  Return a data type for each column such that every value in a column can be
 *  converted to the corresponding data type without data loss.
 *  @param[in]  filename The CSV file
 *
 *  \return A mapping of column names to csv::DataType enums
 */
std::unordered_map<std::string, DataType> csv_data_types(const std::string& filename) {
  CSVStat stat(filename);
  std::unordered_map<std::string, DataType> csv_dtypes;

  auto col_names = stat.get_col_names();
  auto temp = stat.get_dtypes();

  for (size_t i = 0; i < stat.get_col_names().size(); i++) {
    auto& col = temp[i];
    auto& col_name = col_names[i];

    if (col[CSV_STRING])
      csv_dtypes[col_name] = CSV_STRING;
    else if (col[CSV_INT64])
      csv_dtypes[col_name] = CSV_INT64;
    else if (col[CSV_INT32])
      csv_dtypes[col_name] = CSV_INT32;
    else if (col[CSV_INT16])
      csv_dtypes[col_name] = CSV_INT16;
    else if (col[CSV_INT8])
      csv_dtypes[col_name] = CSV_INT8;
    else
      csv_dtypes[col_name] = CSV_DOUBLE;
  }

  return csv_dtypes;
}
}
#include <vector>


namespace csv {
/** Shorthand function for parsing an in-memory CSV string,
 *  a collection of CSVRow objects
 *
 *  @snippet tests/test_read_csv.cpp Parse Example
 */
CSVCollection parse(csv::string_view in, CSVFormat format) {
  CSVReader parser(format);
  parser.feed(in);
  parser.end_feed();
  return parser.records;
}

/** Parse a RFC 4180 CSV string, returning a collection
 *  of CSVRow objects
 *
 *  @par Example
 *  @snippet tests/test_read_csv.cpp Escaped Comma
 *
 */
CSVCollection operator ""_csv(const char* in, size_t n) {
  std::string temp(in, n);
  return parse(temp);
}

/** Return a CSV's column names
 *
 *  @param[in] filename  Path to CSV file
 *  @param[in] format    Format of the CSV file
 *
 */
std::vector<std::string> get_col_names(const std::string& filename, CSVFormat format) {
  CSVReader reader(filename, format);
  return reader.get_col_names();
}

/**
 *  @brief Find the position of a column in a CSV file or CSV_NOT_FOUND otherwise
 *
 *  @param[in] filename  Path to CSV file
 *  @param[in] col_name  Column whose position we should resolve
 *  @param[in] format    Format of the CSV file
 */
int get_col_pos(
    const std::string filename,
    const std::string col_name,
    const CSVFormat format) {
  CSVReader reader(filename, format);
  return reader.index_of(col_name);
}

/** @brief Get basic information about a CSV file
 *  @include programs/csv_info.cpp
 */
CSVFileInfo get_file_info(const std::string& filename) {
  CSVReader reader(filename);
  CSVFormat format = reader.get_format();
  for (auto& row : reader) {
#ifndef NDEBUG
    SUPPRESS_UNUSED_WARNING(row);
#endif
  }

  CSVFileInfo info = {
      filename,
      reader.get_col_names(),
      format.get_delim(),
      reader.correct_rows,
      (int)reader.get_col_names().size()
  };

  return info;
}
}
/** @file
 *  Defines an object which can store CSV data in
 *  continuous regions of memory
 */


namespace csv {
namespace internals {
//////////////
// ColNames //
//////////////

ColNames::ColNames(const std::vector<std::string>& _cnames)
    : col_names(_cnames) {
  for (size_t i = 0; i < _cnames.size(); i++) {
    this->col_pos[_cnames[i]] = i;
  }
}

std::vector<std::string> ColNames::get_col_names() const {
  return this->col_names;
}

size_t ColNames::size() const {
  return this->col_names.size();
}

csv::string_view RawRowBuffer::get_row() {
  csv::string_view ret(
      this->buffer.c_str() + this->current_end, // Beginning of string
      (this->buffer.size() - this->current_end) // Count
  );

  this->current_end = this->buffer.size();
  return ret;
}

ColumnPositions RawRowBuffer::get_splits()
{
  const size_t head_idx = this->current_split_idx,
      new_split_idx = this->split_buffer.size();

  this->current_split_idx = new_split_idx;
  return ColumnPositions(*this, head_idx, (unsigned short)(new_split_idx - head_idx + 1));
}

size_t RawRowBuffer::size() const {
  return this->buffer.size() - this->current_end;
}

size_t RawRowBuffer::splits_size() const {
  return this->split_buffer.size() - this->current_split_idx;
}

HEDLEY_WARN_UNUSED_RESULT
BufferPtr RawRowBuffer::reset() const {
  // Save current row in progress
  auto new_buff = BufferPtr(new RawRowBuffer());

  // Save text
  new_buff->buffer = this->buffer.substr(
      this->current_end,   // Position
      (this->buffer.size() - this->current_end) // Count
  );

  // Save split buffer in progress
  for (size_t i = this->current_split_idx; i < this->split_buffer.size(); i++) {
    new_buff->split_buffer.push_back(this->split_buffer[i]);
  }

  new_buff->col_names = this->col_names;

  // No need to remove unnecessary bits from this buffer
  // (memory savings would be marginal anyways)
  return new_buff;
}

unsigned short ColumnPositions::split_at(int n) const {
  return this->parent->split_buffer[this->start + n];
}
}
}
