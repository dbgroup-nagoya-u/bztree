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
#include <functional>
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

/// Assumes that one word is represented by 8 bytes
constexpr size_t kWordSize = sizeof(uintptr_t);

/// A bit mask for record alignments.
constexpr size_t kAlignMask = ~7UL;

/// Header length in bytes.
constexpr size_t kHeaderLen = 32;

/*######################################################################################
 * Internal utility classes
 *####################################################################################*/

/**
 * @brief A dummy class for defining destruction.
 *
 */
struct DestructByZeroFill {
  DestructByZeroFill() = default;
  DestructByZeroFill(const DestructByZeroFill &) = default;
  DestructByZeroFill(DestructByZeroFill &&) = default;
  DestructByZeroFill &operator=(const DestructByZeroFill &) = default;
  DestructByZeroFill &operator=(DestructByZeroFill &&) = default;

  ~DestructByZeroFill() { std::memset(reinterpret_cast<void *>(this), 0, kPageSize); }
};

/**
 * @brief A struct for representing GC targets.
 *
 */
struct PageTarget {
  // fill zeros as destruction
  using T = DestructByZeroFill;

  // reuse pages
  static constexpr bool kReusePages = true;

  // use the standard free function to release garbage
  static const inline std::function<void(void *)> deleter = [](void *ptr) { std::free(ptr); };
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
  if constexpr (IsVarLenData<Payload>()) {
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
 * @brief Compute the padded length of a record for MwCAS operations.
 *
 * @tparam Payload a class of payloads.
 * @param rec_len the length of a target record.
 * @return the padded length of a record .
 */
template <class Payload>
constexpr auto
Pad(size_t rec_len)  //
    -> size_t
{
  if constexpr (CanCASUpdate<Payload>()) {
    return (rec_len + ~kAlignMask) & kAlignMask;
  } else {
    return rec_len;
  }
}

/**
 * @brief Compute an aligned offset value for MwCAS operations.
 *
 * @tparam Payload a class of payloads.
 * @param offset a current offset value.
 * @return an aligned offset value.
 */
template <class Payload>
constexpr auto
Align(size_t offset)  //
    -> size_t
{
  if constexpr (CanCASUpdate<Payload>()) {
    return offset & kAlignMask;
  } else {
    return offset;
  }
}

}  // namespace dbgroup::index::bztree::component

#endif  // BZTREE_COMPONENT_COMMON_HPP
