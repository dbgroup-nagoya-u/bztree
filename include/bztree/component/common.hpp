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

#include "bztree/utility.hpp"
#include "mwcas/mwcas_descriptor.hpp"

namespace dbgroup::index::bztree::component
{
/*##################################################################################################
 * Common aliases for simplicity
 *################################################################################################*/

using ::dbgroup::atomic::mwcas::MwCASDescriptor;

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

constexpr size_t kMaxAlignment = 16;

/*##################################################################################################
 * Internal utility functions
 *################################################################################################*/

/**
 * @tparam Payload a target payload class.
 * @retval true if a target payload can be updated by MwCAS.
 * @retval false if a target payload cannot be update by MwCAS.
 */
template <class Payload>
constexpr bool
CanCASUpdate()
{
  if constexpr (IsVariableLengthData<Payload>())
    return false;
  else if constexpr (::dbgroup::atomic::mwcas::CanMwCAS<Payload>())
    return true;
  else
    return false;
}

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
 * @brief A wrapper of a deleter class for unique_ptr/shared_ptr.
 *
 * @tparam Payload a class to be deleted by this deleter.
 */
template <class Payload>
struct PayloadDeleter {
  constexpr PayloadDeleter() noexcept = default;

  template <class Up, typename = typename std::enable_if_t<std::is_convertible_v<Up *, Payload *>>>
  PayloadDeleter(const PayloadDeleter<Up> &) noexcept
  {
  }

  void
  operator()(Payload *ptr) const
  {
    static_assert(!std::is_void_v<Payload>, "can't delete pointer to incomplete type");
    static_assert(sizeof(Payload) > 0, "can't delete pointer to incomplete type");

    ::operator delete(ptr);
  }
};

}  // namespace dbgroup::index::bztree::component
