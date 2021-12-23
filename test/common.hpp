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

  constexpr void
  operator=(const uint64_t value)
  {
    data = value;
  }

  // enable std::less to compare this class
  constexpr bool
  operator<(const MyClass &comp) const
  {
    return data < comp.data;
  }
};

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

/**
 * @brief An example specialization to enable CAS-based update.
 *
 */
template <>
constexpr bool
CanCASUpdate<MyClass>()
{
  return true;
}

/**
 * @brief An example specialization of BulkloadEntry.
 *
 */
template <>
class BulkloadEntry<char *, char *>
{
 private:
  char *key_{};
  char *payload_{};

 public:
  constexpr BulkloadEntry(  //
      const char *key,
      const char *payload)
      : key_{const_cast<char *>(key)}, payload_{const_cast<char *>(payload)}
  {
  }

  ~BulkloadEntry() = default;

  constexpr auto
  GetKey() const  //
      -> char *const &
  {
    return key_;
  }

  constexpr auto
  GetPayload() const  //
      -> char *const &
  {
    return payload_;
  }

  constexpr auto
  GetKeyLength() const  //
      -> size_t
  {
    return 7;
  }

  constexpr auto
  GetPayloadLength() const  //
      -> size_t
  {
    return 7;
  }
};

template <class Payload>
class BulkloadEntry<char *, Payload>
{
 private:
  char *key_{};
  Payload payload_{};

 public:
  constexpr BulkloadEntry(  //
      const char *key,
      const Payload payload)
      : key_{const_cast<char *>(key)}, payload_{payload}
  {
  }

  ~BulkloadEntry() = default;

  constexpr auto
  GetKey() const  //
      -> char *const &
  {
    return key_;
  }

  constexpr auto
  GetPayload() const  //
      -> const Payload &
  {
    return payload_;
  }

  constexpr auto
  GetKeyLength() const  //
      -> size_t
  {
    return 7;
  }

  constexpr auto
  GetPayloadLength() const  //
      -> size_t
  {
    return sizeof(Payload);
  }
};

template <class Key>
class BulkloadEntry<Key, char *>
{
 private:
  Key key_{};
  char *payload_{};

 public:
  constexpr BulkloadEntry(  //
      const Key key,
      const char *payload)
      : key_{key}, payload_{const_cast<char *>(payload)}
  {
  }

  ~BulkloadEntry() = default;

  constexpr auto
  GetKey() const  //
      -> const Key &
  {
    return key_;
  }

  constexpr auto
  GetPayload() const  //
      -> char *const &
  {
    return payload_;
  }

  constexpr auto
  GetKeyLength() const  //
      -> size_t
  {
    return sizeof(Key);
  }

  constexpr auto
  GetPayloadLength() const  //
      -> size_t
  {
    return 7;
  }
};

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
      auto data = reinterpret_cast<char *>(::operator new(data_length));
      snprintf(data, data_length, "%06lu", i);
      data_array[i] = reinterpret_cast<T>(data);
    }
  } else if constexpr (std::is_same_v<T, uint64_t *>) {
    // pointer data
    for (size_t i = 0; i < data_num; ++i) {
      auto data = reinterpret_cast<uint64_t *>(::operator new(data_length));
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

using UInt32Comp = std::less<uint32_t>;
using UInt64Comp = std::less<uint64_t>;
using Int64Comp = std::less<int64_t>;
using CStrComp = dbgroup::index::bztree::CompareAsCString;
using PtrComp = std::less<uint64_t *>;
using MyClassComp = std::less<MyClass>;
