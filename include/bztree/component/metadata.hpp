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

#include "common.hpp"

namespace dbgroup::index::bztree::component
{
/**
 * @brief Record metadata accessor:
 *
 */
class alignas(kWordLength) Metadata
{
 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  uint64_t offset_ : 27;
  uint64_t visible_ : 1;
  uint64_t in_progress_ : 1;
  uint64_t key_length_ : 16;
  uint64_t total_length_ : 16;
  uint64_t control_region_ : 3;

 public:
  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  constexpr Metadata()
      : offset_{0},
        visible_{0},
        in_progress_{0},
        key_length_{0},
        total_length_{0},
        control_region_{0}
  {
  }

  constexpr Metadata(  //
      const size_t offset,
      const size_t key_length,
      const size_t total_length,
      const bool is_in_progress = false)
      : offset_{offset},
        visible_{!is_in_progress},
        in_progress_{is_in_progress},
        key_length_{key_length},
        total_length_{total_length},
        control_region_{0}
  {
  }

  ~Metadata() = default;

  constexpr Metadata(const Metadata &) = default;
  constexpr Metadata &operator=(const Metadata &) = default;
  constexpr Metadata(Metadata &&) = default;
  constexpr Metadata &operator=(Metadata &&) = default;

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  constexpr bool
  IsVisible() const
  {
    return visible_;
  }

  constexpr bool
  IsInProgress() const
  {
    return in_progress_;
  }

  constexpr bool
  IsDeleted() const
  {
    return !IsVisible() && !IsInProgress();
  }

  constexpr bool
  IsFailedRecord(const size_t index_epoch) const
  {
    return IsInProgress() && (GetOffset() != index_epoch);
  }

  constexpr size_t
  GetOffset() const
  {
    return offset_;
  }

  constexpr size_t
  GetKeyLength() const
  {
    return key_length_;
  }

  constexpr size_t
  GetTotalLength() const
  {
    return total_length_;
  }

  constexpr size_t
  GetPayloadLength() const
  {
    return GetTotalLength() - GetKeyLength();
  }

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  constexpr Metadata
  UpdateOffset(const size_t offset) const
  {
    auto updated_meta = *this;
    updated_meta.offset_ = offset;
    return updated_meta;
  }

  constexpr Metadata
  MakeVisible(const size_t offset) const
  {
    auto new_meta = *this;
    new_meta.visible_ = 1;
    new_meta.in_progress_ = 0;
    new_meta.offset_ = offset;
    return new_meta;
  }

  constexpr Metadata
  MakeInvisible(const size_t offset) const
  {
    auto new_meta = *this;
    new_meta.in_progress_ = 0;
    new_meta.offset_ = offset;
    return new_meta;
  }

  constexpr Metadata
  Delete() const
  {
    auto new_meta = *this;
    new_meta.visible_ = 0;
    return new_meta;
  }
};

}  // namespace dbgroup::index::bztree::component
