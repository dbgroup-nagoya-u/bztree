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

  constexpr void
  operator=(const uint64_t value)
  {
    data = value;
  }
};

/**
 * @brief An example comparator.
 *
 */
struct MyClassComp {
  constexpr bool
  operator()(const MyClass &a, const MyClass &b) const noexcept
  {
    return std::less<uint64_t>{}(a.data, b.data);
  }
};

namespace dbgroup::index::bztree
{
/**
 * @brief An example specialization to enable CAS-based update.
 *
 */
template <>
struct CASUpdatable<MyClass> {
  constexpr bool
  CanUseCAS() const noexcept
  {
    return true;
  }
};
}  // namespace dbgroup::index::bztree
