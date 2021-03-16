// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <cassert>
#include <cstring>
#include <memory>
#include <sstream>

namespace dbgroup::index::bztree
{
/*--------------------------------------------------------------------------------------------------
 * public utility: divide `utility.h` when reconstruct directory structure
 *------------------------------------------------------------------------------------------------*/

/**
 * @brief Return codes for BzTree.
 *
 */
enum ReturnCode
{
  kSuccess = 0,
  kScanInProgress,
  kKeyNotExist,
  kKeyExist
};

uintptr_t
PayloadToUIntptr(const void *payload)
{
  return *reinterpret_cast<const uint64_t *>(payload);
}

/**
 * @brief Cast a memory address to a target pointer.
 *
 * @tparam T a target class
 * @param addr an original address
 * @return a pointer of \c T
 */
template <class T>
constexpr T
CastAddress(const void *addr)
{
  static_assert(std::is_pointer_v<T>);

  if constexpr (std::is_const_v<T>) {
    return static_cast<T>(addr);
  } else {
    return static_cast<T>(const_cast<void *>(addr));
  }
}

template <class To, class From>
constexpr To
BitCast(const From *obj)
{
  return static_cast<To>(static_cast<void *>(const_cast<From *>(obj)));
}

/**
 * @brief Compare binary keys as C_String. The end of every key must be '\\0'.
 *
 */
struct CompareAsCString {
  constexpr bool
  operator()(const void *a, const void *b) const noexcept
  {
    if (a == nullptr) {
      return false;
    } else if (b == nullptr) {
      return true;
    } else {
      return strcmp(static_cast<const char *>(a), static_cast<const char *>(b)) < 0;
    }
  }
};

/**
 * @brief Compare binary keys as `uint64_t key`. The length of every key must be 8 bytes.
 *
 */
struct CompareAsUInt64 {
  constexpr bool
  operator()(const void *a, const void *b) const noexcept
  {
    if (a == nullptr) {
      return false;
    } else if (b == nullptr) {
      return true;
    } else {
      return *static_cast<const uint64_t *>(a) < *static_cast<const uint64_t *>(b);
    }
  }
};

/**
 * @brief Compare binary keys as `int64_t key`. The length of every key must be 8 bytes.
 *
 */
struct CompareAsInt64 {
  constexpr bool
  operator()(const void *a, const void *b) const noexcept
  {
    if (a == nullptr) {
      return false;
    } else if (b == nullptr) {
      return true;
    } else {
      return *static_cast<const int64_t *>(a) < *static_cast<const int64_t *>(b);
    }
  }
};

/*--------------------------------------------------------------------------------------------------
 * Common constants and utility functions
 *------------------------------------------------------------------------------------------------*/

/// Assumes that one word is represented by 8 bytes
constexpr size_t kWordLength = 8;

/// Assumes that one word is represented by 8 bytes
constexpr size_t kCacheLineSize = 64;

/// Header length in bytes
constexpr size_t kHeaderLength = 2 * kWordLength;

/**
 * @brief
 *
 * @tparam Compare
 * @param obj_1
 * @param obj_2
 * @return true if a specified objects are equivalent according to `comp` comparator
 * @return false otherwise
 */
template <class Compare, class Key>
constexpr bool
IsEqual(const Key &obj_1, const Key &obj_2)
{
  return !(Compare{}(obj_1, obj_2) || Compare{}(obj_2, obj_1));
}

/**
 * @brief
 *
 * @tparam Compare
 * @param key
 * @param begin_key
 * @param begin_is_closed
 * @param end_key
 * @param end_is_closed
 * @return true if a specfied key is in an input interval
 * @return false
 */
template <class Compare, class Key>
constexpr bool
IsInRange(const Key &key,
          const Key *begin_key,
          const bool begin_is_closed,
          const Key *end_key,
          const bool end_is_closed)
{
  if (begin_key == nullptr && end_key == nullptr) {
    // no range condition
    return true;
  } else if (begin_key == nullptr) {
    // less than or equal to
    return Compare{}(key, *end_key) || (end_is_closed && IsEqual<Compare>(key, *end_key));
  } else if (end_key == nullptr) {
    // greater than or equal to
    return Compare{}(*begin_key, key) || (begin_is_closed && IsEqual<Compare>(key, *begin_key));
  } else {
    // between
    return (Compare{}(*begin_key, key) && Compare{}(key, *end_key))
           || (begin_is_closed && IsEqual<Compare>(key, *begin_key))
           || (end_is_closed && IsEqual<Compare>(key, *end_key));
  }
}

/**
 * @brief Shift a memory address by byte offsets.
 *
 * @tparam T
 * @param ptr original address
 * @param offset
 * @return byte* shifted address
 */
constexpr void *
ShiftAddress(const void *ptr, const size_t offset)
{
  return static_cast<void *>(static_cast<std::byte *>(const_cast<void *>(ptr)) + offset);
}

constexpr bool
HaveSameAddress(const void *a, const void *b)
{
  return a == b;
}
}  // namespace dbgroup::index::bztree
