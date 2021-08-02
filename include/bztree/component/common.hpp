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

#include <cstring>
#include <memory>

#include "../utility.hpp"
#include "memory/utility.hpp"
#include "mwcas/mwcas_descriptor.hpp"

namespace dbgroup::index::bztree::component
{
/*--------------------------------------------------------------------------------------------------
 * public utility: divide `utility.h` when reconstruct directory structure
 *------------------------------------------------------------------------------------------------*/

constexpr auto mo_relax = std::memory_order_relaxed;

/**
 * @brief Return codes for functions in Base/Leaf/InternalNode.
 *
 */
enum NodeReturnCode
{
  kSuccess = 0,
  kKeyNotExist,
  kKeyExist,
  kScanInProgress,
  kFrozen,
  kNeedConsolidation
};

enum KeyExistence
{
  kExist = 0,
  kNotExist,
  kDeleted,
  kUncertain
};

template <class T>
constexpr T
Cast(const void *addr)
{
  static_assert(std::is_pointer_v<T>);

  return static_cast<T>(const_cast<void *>(addr));
}

/*--------------------------------------------------------------------------------------------------
 * Common constants and utility functions
 *------------------------------------------------------------------------------------------------*/

using ::dbgroup::atomic::mwcas::MwCASDescriptor;

using ::dbgroup::atomic::mwcas::ReadMwCASField;

using ::dbgroup::memory::CallocNew;

using ::dbgroup::memory::STLAlloc;

using ::dbgroup::memory::Deleter;

/// Header length in bytes
constexpr size_t kHeaderLength = 2 * kWordLength;

template <class Key, class Payload>
constexpr size_t
GetMaxRecordNum()
{
  auto record_min_length = kWordLength;
  if constexpr (std::is_same_v<Key, char *>) {
    record_min_length += 1;
  } else {
    record_min_length += sizeof(Key);
  }
  if constexpr (std::is_same_v<Payload, char *>) {
    record_min_length += 1;
  } else {
    record_min_length += sizeof(Payload);
  }
  return (kPageSize - kHeaderLength) / record_min_length;
}

template <class Payload>
constexpr bool
CanCASUpdate()
{
  return !std::is_same_v<Payload, char *> && sizeof(Payload) == kWordLength;
}

template <class Key>
constexpr void
AlignOffset(size_t &offset)
{
  if constexpr (std::is_same_v<Key, char *>) {
    const auto align_size = offset & (kWordLength - 1);
    if (align_size > 0) {
      offset -= align_size;
    }
  } else if constexpr (sizeof(Key) % kWordLength != 0) {
    const auto align_size = kWordLength - (sizeof(Key) % kWordLength);
    offset -= align_size;
  }
}

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
  return !Compare{}(obj_1, obj_2) && !Compare{}(obj_2, obj_1);
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
IsInRange(  //
    const Key &key,
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
    return Compare{}(key, *end_key) || (end_is_closed && !Compare{}(*end_key, key));
  } else if (end_key == nullptr) {
    // greater than or equal to
    return Compare{}(*begin_key, key) || (begin_is_closed && !Compare{}(key, *begin_key));
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

}  // namespace dbgroup::index::bztree::component
