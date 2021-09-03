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
/*##################################################################################################
 * Common aliases for simplicity
 *################################################################################################*/

using ::dbgroup::atomic::mwcas::MwCASDescriptor;
using ::dbgroup::atomic::mwcas::ReadMwCASField;
using ::dbgroup::memory::CallocNew;
using ::dbgroup::memory::Deleter;
using ::dbgroup::memory::STLAlloc;

/*##################################################################################################
 * Internal enum and classes
 *################################################################################################*/

/**
 * @brief Internal return codes to represent results of node modification.
 *
 */
enum NodeReturnCode
{
  kSuccess = 0,
  kKeyNotExist,
  kKeyExist,
  kFrozen,
  kNeedConsolidation
};

/**
 * @brief Internal return codes to represent results of uniqueness check.
 *
 */
enum KeyExistence
{
  kExist = 0,
  kNotExist,
  kDeleted,
  kUncertain
};

/*##################################################################################################
 * Internal constants
 *################################################################################################*/

/// alias of memory order for simplicity.
constexpr auto mo_relax = std::memory_order_relaxed;

/// Header length in bytes.
constexpr size_t kHeaderLength = 2 * kWordLength;

/*##################################################################################################
 * Internal utility functions
 *################################################################################################*/

/**
 * @brief Cast a given pointer to a specified pointer type.
 *
 * @tparam T a target pointer type.
 * @param addr a target pointer.
 * @return T: a casted pointer.
 */
template <class T>
constexpr T
Cast(const void *addr)
{
  static_assert(std::is_pointer_v<T>);

  return static_cast<T>(const_cast<void *>(addr));
}

/**
 * @brief Compute the maximum number of records in a node.
 *
 * @tparam Key a target key class.
 * @tparam Payload a target payload class.
 * @return size_t the expected maximum number of records.
 */
template <class Key, class Payload>
constexpr size_t
GetMaxRecordNum()
{
  auto record_min_length = kWordLength;
  if constexpr (IsVariableLengthData<Key>()) {
    record_min_length += 1;
  } else {
    record_min_length += sizeof(Key);
  }
  if constexpr (IsVariableLengthData<Payload>()) {
    record_min_length += 1;
  } else {
    record_min_length += sizeof(Payload);
  }
  return (kPageSize - kHeaderLength) / record_min_length;
}

/**
 * @brief Align a given offset to perform CAS operations.
 *
 * @tparam Key a target key class.
 * @param offset a target offset to be aligned.
 */
template <class Key>
constexpr void
AlignOffset(size_t &offset)
{
  if constexpr (IsVariableLengthData<Key>()) {
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
 * @tparam Compare a comparator class.
 * @tparam T a target class.
 * @param obj_1 an object to be compared.
 * @param obj_2 another object to be compared.
 * @retval true if given objects are equivalent.
 * @retval false if given objects are different.
 */
template <class Compare, class T>
constexpr bool
IsEqual(  //
    const T &obj_1,
    const T &obj_2)
{
  return !Compare{}(obj_1, obj_2) && !Compare{}(obj_2, obj_1);
}

/**
 * @tparam Compare a comparator class for target keys.
 * @tparam Key a target key class.
 * @param key a target key.
 * @param begin_key a begin key of a range condition.
 * @param begin_closed a flag to indicate whether the begin side of range is closed.
 * @param end_key an end key of a range condition.
 * @param end_closed a flag to indicate whether the end side of range is closed.
 * @retval true if a target key is in a range.
 * @retval false if a target key is outside of a range.
 */
template <class Compare, class Key>
constexpr bool
IsInRange(  //
    const Key &key,
    const Key *begin_key,
    const bool begin_closed,
    const Key *end_key,
    const bool end_closed)
{
  if (begin_key == nullptr && end_key == nullptr) {
    // no range condition
    return true;
  } else if (begin_key == nullptr) {
    // less than or equal to
    return Compare{}(key, *end_key) || (end_closed && !Compare{}(*end_key, key));
  } else if (end_key == nullptr) {
    // greater than or equal to
    return Compare{}(*begin_key, key) || (begin_closed && !Compare{}(key, *begin_key));
  } else {
    // between
    return !((Compare{}(key, *begin_key) || Compare{}(*end_key, key))
             || (!begin_closed && IsEqual<Compare>(key, *begin_key))
             || (!end_closed && IsEqual<Compare>(key, *end_key)));
  }
}

/**
 * @brief Shift a memory address by byte offsets.
 *
 * @param addr an original address.
 * @param offset an offset to shift.
 * @return void* a shifted address.
 */
constexpr void *
ShiftAddress(  //
    const void *addr,
    const size_t offset)
{
  return static_cast<std::byte *>(const_cast<void *>(addr)) + offset;
}

}  // namespace dbgroup::index::bztree::component
