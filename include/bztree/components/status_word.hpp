// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include "common.hpp"

namespace bztree
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

  ~StatusWord() = default;

  StatusWord(const StatusWord &) = default;
  StatusWord &operator=(const StatusWord &) = default;
  StatusWord(StatusWord &&) = default;
  StatusWord &operator=(StatusWord &&) = default;

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
  AddRecordInfo(  //
      const size_t record_count,
      const size_t block_size,
      const size_t deleted_size) const
  {
    auto new_status = *this;
    new_status.record_count_ += record_count;
    new_status.block_size_ += block_size;
    new_status.deleted_size_ += deleted_size;
    return new_status;
  }
};

static_assert(sizeof(StatusWord) == kWordLength);

}  // namespace bztree
