/*
 * Copyright 2021 Database Group, Nagoya University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

constexpr auto mo_relax = std::memory_order_relaxed;

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

constexpr uintptr_t
PayloadToNodeAddr(const void *payload)
{
  return *static_cast<const uintptr_t *>(payload);
}

template <class Key>
constexpr Key
CastKey(const void *addr)
{
  if constexpr (std::is_pointer_v<Key>) {
    if constexpr (std::is_const_v<Key>) {
      return static_cast<Key>(addr);
    } else {
      return static_cast<Key>(const_cast<void *>(addr));
    }
  } else {
    return *static_cast<const Key *>(addr);
  }
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

/*--------------------------------------------------------------------------------------------------
 * Common constants and utility functions
 *------------------------------------------------------------------------------------------------*/

/// Assumes that one word is represented by 8 bytes
constexpr size_t kWordLength = 8;

/// Assumes that one word is represented by 8 bytes
constexpr size_t kCacheLineSize = 64;

/// Header length in bytes
constexpr size_t kHeaderLength = 2 * kWordLength;

#ifdef BZTREE_PAGE_SIZE
/// The page size of each node
constexpr size_t kPageSize = BZTREE_PAGE_SIZE;
#else
constexpr size_t kPageSize = 8192;
#endif

/// check whether the specified page size is valid
static_assert(kPageSize % kWordLength == 0);

/// a flag to indicate creating leaf nodes
constexpr bool kLeafFlag = true;

/// a flag to indicate creating internal nodes
constexpr bool kInternalFlag = false;

const std::unique_ptr<char> kNullCString = nullptr;

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
  return static_cast<std::byte *>(const_cast<void *>(ptr)) + offset;
}

constexpr bool
HaveSameAddress(const void *a, const void *b)
{
  return a == b;
}
}  // namespace dbgroup::index::bztree
