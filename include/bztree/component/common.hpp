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

#ifdef BZTREE_HAS_SPINLOCK_HINT
#include <xmmintrin.h>
#define BZTREE_SPINLOCK_HINT _mm_pause();  // NOLINT
#else
#define BZTREE_SPINLOCK_HINT /* do nothing */
#endif

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
enum NodeRC {
  kSuccess = 0,
  kKeyNotExist = -4,
  kKeyExist,
  kFrozen,
  kNeedConsolidation
};

/**
 * @brief Internal return codes to represent results of uniqueness check.
 *
 */
enum KeyExistence {
  kExist = 0,
  kNotExist = -3,
  kDeleted,
  kUncertain
};

constexpr size_t kAlignMask = ~7UL;

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
Align([[maybe_unused]] size_t key_len)  //
    -> std::pair<size_t, size_t>
{
  if constexpr (!CanCASUpdate<Payload>()) {
    // record alignment is not required
    if constexpr (IsVariableLengthData<Key>()) {
      return {key_len, key_len + sizeof(Payload)};
    } else {
      return {sizeof(Key), sizeof(Key) + sizeof(Payload)};
    }
  } else if constexpr (IsVariableLengthData<Key>()) {
    // dynamic alignment is required
    const size_t align_len = alignof(Payload) - key_len % alignof(Payload);
    if (align_len == alignof(Payload)) {
      // alignment is not required
      return {key_len, key_len + sizeof(Payload)};
    }
    return {key_len, align_len + key_len + sizeof(Payload)};
  } else {
    constexpr size_t kAlignLen = alignof(Payload) - sizeof(Key) % alignof(Payload);
    if constexpr (kAlignLen == alignof(Payload)) {
      // alignment is not required
      return {sizeof(Key), sizeof(Key) + sizeof(Payload)};
    } else {
      // fixed-length alignment is required
      return {sizeof(Key), sizeof(Key) + sizeof(Payload) + kAlignLen};
    }
  }
}

/**
 * @tparam Key a class of keys.
 * @tparam Payload a class of payloads.
 * @retval true if offsets may need padding for alignments.
 * @retval false otherwise.
 */
template <class Key, class Payload>
constexpr auto
NeedOffsetAlignment()  //
    -> bool
{
  if constexpr (!CanCASUpdate<Payload>()) {
    // record alignment is not required
    return false;
  } else if constexpr (IsVariableLengthData<Key>()) {
    // dynamic alignment is required
    return true;
  } else {
    constexpr size_t kAlignLen = alignof(Payload) - sizeof(Key) % alignof(Payload);
    if constexpr (kAlignLen == alignof(Payload)) {
      // alignment is not required
      return false;
    } else {
      // fixed-length alignment is required
      return true;
    }
  }
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
Pad(size_t offset)  //
    -> size_t
{
  if constexpr (IsVariableLengthData<Key>()) {
    // dynamic alignment is required
    return offset & kAlignMask;
  } else {
    // fixed-length alignment is required
    constexpr size_t kAlignLen = alignof(Payload) - sizeof(Key) % alignof(Payload);
    return offset - kAlignLen;
  }
}

/**
 * @tparam Key a class of keys.
 * @return the maximum size of records in internal nodes.
 */
template <class Key>
[[nodiscard]] constexpr auto
GetMaxInternalRecordSize()  //
    -> size_t
{
  if constexpr (IsVariableLengthData<Key>()) {
    return Align<Key, void *>(kMaxVarDataSize).second;
  } else {
    return Align<Key, void *>(sizeof(Key)).second;
  }
}

}  // namespace dbgroup::index::bztree::component

#endif  // BZTREE_COMPONENT_COMMON_HPP
