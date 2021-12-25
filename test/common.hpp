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

#include <functional>

#include "bztree/utility.hpp"

constexpr size_t kVarDataLength = 9;

/**
 * @brief An example class to represent CAS-updatable data.
 *
 */
struct MyClass {
  uint64_t data : 61;
  uint64_t control_bits : 3;

  constexpr MyClass() : data{}, control_bits{0} {}
  ~MyClass() = default;

  constexpr MyClass(const MyClass &) = default;
  constexpr MyClass &operator=(const MyClass &) = default;
  constexpr MyClass(MyClass &&) = default;
  constexpr MyClass &operator=(MyClass &&) = default;

  constexpr auto
  operator=(const uint64_t value)  //
      -> MyClass &
  {
    data = value;
    return *this;
  }

  // enable std::less to compare this class
  constexpr bool
  operator<(const MyClass &comp) const
  {
    return data < comp.data;
  }
};

namespace dbgroup::atomic::mwcas
{
/**
 * @brief Specialization to enable MwCAS to swap our sample class.
 *
 */
template <>
constexpr bool
CanMwCAS<MyClass>()
{
  return true;
}

}  // namespace dbgroup::atomic::mwcas

namespace dbgroup::index::bztree
{
/**
 * @brief Use CString as variable-length data in tests.
 *
 */
template <>
constexpr bool
IsVariableLengthData<char *>()
{
  return true;
}

template <class T>
void
PrepareTestData(  //
    T *data_array,
    const size_t data_num,
    [[maybe_unused]] const size_t data_length)
{
  if constexpr (IsVariableLengthData<T>()) {
    // variable-length data
    for (size_t i = 0; i < data_num; ++i) {
      auto *data = reinterpret_cast<char *>(::operator new(data_length));
      snprintf(data, data_length, "%08lu", i);  // NOLINT
      data_array[i] = reinterpret_cast<T>(data);
    }
  } else if constexpr (std::is_same_v<T, uint64_t *>) {
    // pointer data
    for (size_t i = 0; i < data_num; ++i) {
      auto *data = reinterpret_cast<uint64_t *>(::operator new(data_length));
      *data = i;
      data_array[i] = data;
    }
  } else {
    // static-length data
    for (size_t i = 0; i < data_num; ++i) {
      data_array[i] = i;
    }
  }
}

template <class T>
void
ReleaseTestData(  //
    [[maybe_unused]] T *data_array,
    const size_t data_num)
{
  if constexpr (std::is_pointer_v<T>) {
    for (size_t i = 0; i < data_num; ++i) {
      ::operator delete(data_array[i]);
    }
  }
}

}  // namespace dbgroup::index::bztree

/*######################################################################################
 * Type definitions for templated tests
 *####################################################################################*/

struct UInt8 {
  using Data = uint64_t;
  using Comp = std::less<uint64_t>;
};

struct Int8 {
  using Data = int64_t;
  using Comp = std::less<int64_t>;
};

struct UInt4 {
  using Data = uint32_t;
  using Comp = std::less<uint32_t>;
};

struct Ptr {
  struct PtrComp {
    constexpr bool
    operator()(const uint64_t *a, const uint64_t *b) const noexcept
    {
      return *a < *b;
    }
  };

  using Data = uint64_t *;
  using Comp = PtrComp;
};

struct Var {
  using Data = char *;
  using Comp = dbgroup::index::bztree::CompareAsCString;
};

struct Original {
  using Data = MyClass;
  using Comp = std::less<MyClass>;
};
