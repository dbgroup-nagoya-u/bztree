// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <cassert>
#include <cstring>
#include <memory>
#include <sstream>

namespace bztree
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

template <class T>
std::byte *
CastToBytePtr(const T *obj)
{
  return static_cast<std::byte *>(static_cast<void *>(const_cast<T *>(obj)));
}

template <class T>
uint64_t
CastToUint64(const T *obj)
{
  return *static_cast<uint64_t *>(static_cast<void *>(const_cast<T *>(obj)));
}

template <class T>
uint64_t *
CastToUint64Ptr(const T *obj)
{
  return static_cast<uint64_t *>(static_cast<void *>(const_cast<T *>(obj)));
}

char *
CastToCString(const std::byte *obj)
{
  return static_cast<char *>(static_cast<void *>(const_cast<std::byte *>(obj)));
}

/**
 * @brief Compare binary keys as C_String. The end of every key must be '\\0'.
 *
 */
struct CompareAsCString {
  constexpr bool
  operator()(const std::byte *a, const std::byte *b) const noexcept
  {
    return strcmp(static_cast<const char *>(static_cast<const void *>(a)),
                  static_cast<const char *>(static_cast<const void *>(b)))
           < 0;
  }
};

/**
 * @brief Compare binary keys as `uint64_t key`. The length of every key must be 8 bytes.
 *
 */
struct CompareAsUInt64 {
  constexpr bool
  operator()(const std::byte *a, const std::byte *b) const noexcept
  {
    return *static_cast<const uint64_t *>(static_cast<const void *>(a))
           < *static_cast<const uint64_t *>(static_cast<const void *>(b));
  }
};

/**
 * @brief Compare binary keys as `int64_t key`. The length of every key must be 8 bytes.
 *
 */
struct CompareAsInt64 {
  constexpr bool
  operator()(const std::byte *a, const std::byte *b) const noexcept
  {
    return *static_cast<const int64_t *>(static_cast<const void *>(a))
           < *static_cast<const int64_t *>(static_cast<const void *>(b));
  }
};

/*--------------------------------------------------------------------------------------------------
 * Common constants and utility functions
 *------------------------------------------------------------------------------------------------*/

// this code assumes that one word is represented by 8 bytes.
constexpr size_t kWordLength = 8;

// pointer's byte length
constexpr size_t kPointerLength = kWordLength;

// header length in bytes
constexpr size_t kHeaderLength = 2 * kWordLength;

/**
 * @brief
 *
 * @tparam Compare
 * @param obj_1
 * @param obj_2
 * @param comp comparator
 * @return true if a specified objects are equivalent according to `comp` comparator
 * @return false otherwise
 */
template <class Key, template <typename> class Compare>
bool
IsEqual(const Key obj_1, const Key obj_2, Compare<Key> comp)
{
  return !(comp(obj_1, obj_2) || comp(obj_2, obj_1));
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
 * @param comp
 * @return true if a specfied key is in an input interval
 * @return false
 */
template <class Key, template <typename> class Compare>
bool
IsInRange(const Key key,
          const Key begin_key,
          const bool begin_is_closed,
          const Key end_key,
          const bool end_is_closed,
          Compare<Key> comp)
{
  if (begin_key != nullptr && end_key != nullptr) {
    return (comp(begin_key, key) && comp(key, end_key))
           || (begin_is_closed && IsEqual(key, begin_key, comp))
           || (end_is_closed && IsEqual(key, end_key, comp));
  } else if (begin_key == nullptr) {
    return comp(key, end_key) || (end_is_closed && IsEqual(key, end_key, comp));
  } else if (end_key == nullptr) {
    return comp(begin_key, key) || (begin_is_closed && IsEqual(key, begin_key, comp));
  } else {
    return true;
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
template <class T>
constexpr std::byte *
ShiftAddress(T *ptr, const size_t offset)
{
  return static_cast<std::byte *>(
      static_cast<void *>(static_cast<std::byte *>(static_cast<void *>(ptr)) + offset));
}

template <class T1, class T2>
constexpr bool
HaveSameAddress(const T1 *a, const T2 *b)
{
  return static_cast<const void *>(a) == static_cast<const void *>(b);
}

}  // namespace bztree
