// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <cassert>
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

/**
 * @brief Compare binary keys as C_String. The end of every key must be '\\0'.
 *
 */
struct CompareAsCString {
  constexpr bool
  operator()(const std::byte *a, const std::byte *b) const noexcept
  {
    return static_cast<const char *>(static_cast<const void *>(a))
           < static_cast<const char *>(static_cast<const void *>(b));
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

template <class Compare>
struct UniquePtrComparator {
  Compare comp;

  explicit UniquePtrComparator(Compare comp) : comp(comp) {}

  bool
  operator()(const std::unique_ptr<std::byte[]> &a,
             const std::unique_ptr<std::byte[]> &b) const noexcept
  {
    return comp(a.get(), b.get());
  }
};

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
template <class Compare>
bool
IsEqual(const std::byte *obj_1, const std::byte *obj_2, Compare comp)
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
template <class Compare>
bool
IsInRange(const std::byte *key,
          const std::byte *begin_key,
          const bool begin_is_closed,
          const std::byte *end_key,
          const bool end_is_closed,
          Compare comp)
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
template <typename T>
constexpr std::byte *
ShiftAddress(T *ptr, const size_t offset)
{
  return static_cast<std::byte *>(
      static_cast<void *>(static_cast<std::byte *>(static_cast<void *>(ptr)) + offset));
}

template <typename T1, typename T2>
bool
HaveSameAddress(const T1 *a, const T2 *b)
{
  return static_cast<const void *>(a) == static_cast<const void *>(b);
}

}  // namespace bztree
