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
 * @brief Status word accessor:
 *
 */
class alignas(kWordLength) StatusWord
{
 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  uint64_t record_count_ : 16;
  uint64_t block_size_ : 22;
  uint64_t deleted_size_ : 22;
  uint64_t frozen_ : 1;
  uint64_t control_region_ : 3;

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  constexpr StatusWord()
      : record_count_{0}, block_size_{0}, deleted_size_{0}, frozen_{0}, control_region_{0}
  {
  }

  constexpr StatusWord(  //
      const size_t record_count,
      const size_t block_size)
      : record_count_{record_count},
        block_size_{block_size},
        deleted_size_{0},
        frozen_{0},
        control_region_{0}
  {
  }

  ~StatusWord() = default;

  constexpr StatusWord(const StatusWord &) = default;
  constexpr StatusWord &operator=(const StatusWord &) = default;
  constexpr StatusWord(StatusWord &&) = default;
  constexpr StatusWord &operator=(StatusWord &&) = default;

  constexpr bool
  operator==(const StatusWord &comp) const
  {
    return record_count_ == comp.record_count_     //
           && block_size_ == comp.block_size_      //
           && deleted_size_ == comp.deleted_size_  //
           && frozen_ == comp.frozen_;
  }

  constexpr bool
  operator!=(const StatusWord &comp) const
  {
    return !(*this == comp);
  }

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  constexpr bool
  IsFrozen() const
  {
    return frozen_;
  }

  constexpr size_t
  GetRecordCount() const
  {
    return record_count_;
  }

  constexpr size_t
  GetBlockSize() const
  {
    return block_size_;
  }

  constexpr size_t
  GetDeletedSize() const
  {
    return deleted_size_;
  }

  constexpr size_t
  GetOccupiedSize() const
  {
    return kHeaderLength + (kWordLength * record_count_) + block_size_;
  }

  constexpr size_t
  GetLiveDataSize() const
  {
    return (kWordLength * record_count_) + block_size_ - deleted_size_;
  }

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  constexpr StatusWord
  Freeze() const
  {
    auto frozen_status = *this;
    frozen_status.frozen_ = 1;
    return frozen_status;
  }

  constexpr StatusWord
  Add(  //
      const size_t record_count,
      const size_t block_size) const
  {
    auto new_status = *this;
    new_status.record_count_ += record_count;
    new_status.block_size_ += block_size;
    return new_status;
  }

  constexpr StatusWord
  Delete(const size_t deleted_size) const
  {
    auto new_status = *this;
    new_status.deleted_size_ += deleted_size;
    return new_status;
  }
};

static_assert(sizeof(StatusWord) == kWordLength);

}  // namespace dbgroup::index::bztree::component
