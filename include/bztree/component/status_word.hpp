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
 * @brief A class to represent status words of BzTree.
 *
 */
class StatusWord
{
 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  /// the total number of records in a node.
  uint64_t record_count_ : 16;

  /// the total byte length of records in a node.
  uint64_t block_size_ : 22;

  /// the total byte length of deleted metadata/records in a node.
  uint64_t deleted_size_ : 22;

  /// a flag to indicate whether a node is frozen (i.e., immutable).
  uint64_t frozen_ : 1;

  /// control bits to perform PMwCAS.
  uint64_t control_region_ : 3;

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  /**
   * @brief Construct a new status word with zero padding.
   *
   */
  constexpr StatusWord()
      : record_count_{0}, block_size_{0}, deleted_size_{0}, frozen_{0}, control_region_{0}
  {
  }

  /**
   * @brief Construct a new status word with specified arguments.
   *
   */
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

  /**
   * @brief Destroy the status word object.
   *
   */
  ~StatusWord() = default;

  constexpr StatusWord(const StatusWord &) = default;
  constexpr StatusWord &operator=(const StatusWord &) = default;
  constexpr StatusWord(StatusWord &&) = default;
  constexpr StatusWord &operator=(StatusWord &&) = default;

  /**
   * @brief An operator to check equality.
   */
  constexpr bool
  operator==(const StatusWord &comp) const
  {
    return record_count_ == comp.record_count_     //
           && block_size_ == comp.block_size_      //
           && deleted_size_ == comp.deleted_size_  //
           && frozen_ == comp.frozen_;
  }

  /**
   * @brief An operator to check inequality.
   */
  constexpr bool
  operator!=(const StatusWord &comp) const
  {
    return record_count_ != comp.record_count_     //
           || block_size_ != comp.block_size_      //
           || deleted_size_ != comp.deleted_size_  //
           || frozen_ != comp.frozen_;
  }

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  /**
   * @retval true if a node is frozen (i.e., immutable).
   * @retval false if a node is not frozen.
   */
  constexpr bool
  IsFrozen() const
  {
    return frozen_;
  }

  /**
   * @return size_t: the total number of records in a node.
   */
  constexpr size_t
  GetRecordCount() const
  {
    return record_count_;
  }

  /**
   * @return size_t: the total byte length of records in a node.
   */
  constexpr size_t
  GetBlockSize() const
  {
    return block_size_;
  }

  /**
   * @return size_t: the total byte length of deleted metadata/records in a node.
   */
  constexpr size_t
  GetDeletedSize() const
  {
    return deleted_size_;
  }

  /**
   * @return size_t: the total byte length of a header, metadata, and records in a node.
   */
  constexpr size_t
  GetOccupiedSize() const
  {
    return kHeaderLength + (kWordLength * record_count_) + block_size_;
  }

  /**
   * @return size_t: the total byte length of live metadata/records in a node.
   */
  constexpr size_t
  GetLiveDataSize() const
  {
    return (kWordLength * record_count_) + block_size_ - deleted_size_;
  }

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  /**
   * @return StatusWord: a frozen status word.
   */
  constexpr StatusWord
  Freeze() const
  {
    auto frozen_status = *this;
    frozen_status.frozen_ = 1;
    return frozen_status;
  }

  /**
   * @param record_count the number of added records.
   * @param block_size the byte length of added records.
   * @return StatusWord: a new status word with added records.
   */
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

  /**
   * @param deleted_size: the byte length of deleted metadata and records.
   * @return StatusWord: a new status word with deleted records.
   */
  constexpr StatusWord
  Delete(const size_t deleted_size) const
  {
    auto new_status = *this;
    new_status.deleted_size_ += deleted_size;
    return new_status;
  }
};

// a status word must be represented by one word.
static_assert(sizeof(StatusWord) == kWordLength);

// a status word must be trivially copyable.
static_assert(std::is_trivially_copyable_v<StatusWord>);

}  // namespace dbgroup::index::bztree::component
