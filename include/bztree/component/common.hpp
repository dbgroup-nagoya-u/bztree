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

#ifndef BZTREE_COMPONENT_COMMON_HPP
#define BZTREE_COMPONENT_COMMON_HPP

#include <cstring>
#include <memory>

#include "bztree/utility.hpp"
#include "mwcas/mwcas_descriptor.hpp"

namespace dbgroup::index::bztree::component
{
/*######################################################################################
 * Common aliases for simplicity
 *####################################################################################*/

using ::dbgroup::atomic::mwcas::MwCASDescriptor;

/*######################################################################################
 * Internal enum and classes
 *####################################################################################*/

/**
 * @brief Internal return codes to represent results of node modification.
 *
 */
enum NodeRC
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

/*######################################################################################
 * Internal utility functions
 *####################################################################################*/

/**
 * @tparam Payload a target payload class.
 * @retval true if a target payload can be updated by MwCAS.
 * @retval false if a target payload cannot be update by MwCAS.
 */
template <class Payload>
constexpr bool
CanCASUpdate()
{
  if constexpr (IsVariableLengthData<Payload>()) {
    return false;
  } else if constexpr (::dbgroup::atomic::mwcas::CanMwCAS<Payload>()) {
    return true;
  } else {
    return false;
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
 * @brief Shift a memory address by byte offsets.
 *
 * @param addr an original address.
 * @param offset an offset to shift.
 * @return void* a shifted address.
 */
constexpr void *
ShiftAddr(  //
    const void *addr,
    const int64_t offset)
{
  return static_cast<std::byte *>(const_cast<void *>(addr)) + offset;
}

/**
 * @brief Compute padded key/payload/total lengths for alignment.
 *
 * @tparam Key a class of keys.
 * @tparam Payload a class of payloads.
 * @param key_len the length of a target key.
 * @param pay_len the length of a target payload.
 * @return the tuple of key/payload/total lengths.
 */
template <class Key, class Payload>
constexpr auto
AlignRecord(  //
    size_t key_len,
    size_t pay_len)  //
    -> std::tuple<size_t, size_t, size_t>
{
  if constexpr (IsVariableLengthData<Key>() && IsVariableLengthData<Payload>()) {
    // record alignment is not required
    return {key_len, pay_len, key_len + pay_len};
  } else if constexpr (IsVariableLengthData<Key>()) {
    // dynamic alignment is required
    const size_t align_len = alignof(Payload) - key_len % alignof(Payload);
    if (align_len == alignof(Payload)) {
      // alignment is not required
      return {key_len, pay_len, key_len + pay_len};
    }
    return {key_len, pay_len, align_len + key_len + pay_len};
  } else if constexpr (IsVariableLengthData<Payload>()) {
    const size_t align_len = alignof(Key) - pay_len % alignof(Key);
    if (align_len != alignof(Key)) {
      // dynamic alignment is required
      key_len += align_len;
    }
    return {key_len, pay_len, key_len + pay_len};
  } else if constexpr (alignof(Key) < alignof(Payload)) {
    constexpr size_t kAlignLen = alignof(Payload) - sizeof(Key) % alignof(Payload);
    if constexpr (kAlignLen == alignof(Payload)) {
      // alignment is not required
      return {key_len, pay_len, key_len + pay_len};
    } else {
      // fixed-length alignment is required
      constexpr size_t kKeyLen = sizeof(Key) + kAlignLen;
      return {kKeyLen, pay_len, kKeyLen + pay_len};
    }
  } else if constexpr (alignof(Key) > alignof(Payload)) {
    constexpr size_t kAlignLen = alignof(Key) - sizeof(Payload) % alignof(Key);
    if constexpr (kAlignLen == alignof(Key)) {
      // alignment is not required
      return {key_len, pay_len, key_len + pay_len};
    } else {
      // fixed-length alignment is required
      constexpr size_t kPayLen = sizeof(Payload) + kAlignLen;
      return {key_len, kPayLen, key_len + kPayLen};
    }
  } else {
    // alignment is not required
    return {key_len, pay_len, key_len + pay_len};
  }
}

/**
 * @brief Compute an aligned offset for node initialization.
 *
 * @tparam Key a class of keys.
 * @tparam Payload a class of payloads.
 * @return an aligned offset.
 */
template <class Key, class Payload>
constexpr auto
GetInitialOffset()  //
    -> size_t
{
  if constexpr (IsVariableLengthData<Payload>() || IsVariableLengthData<Key>()) {
    return kPageSize;
  } else if constexpr (alignof(Key) <= alignof(Payload)) {
    return kPageSize;
  } else {
    constexpr size_t kAlignLen = alignof(Key) - sizeof(Payload) % alignof(Key);
    if constexpr (kAlignLen == alignof(Key)) {
      return kPageSize;
    } else {
      return kPageSize - kAlignLen;
    }
  }
}

}  // namespace dbgroup::index::bztree::component

#endif  // BZTREE_COMPONENT_COMMON_HPP
