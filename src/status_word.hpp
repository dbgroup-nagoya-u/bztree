// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <string>

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

  uint64_t control_ : 3 = 0;
  bool frozen_ : 1 = false;
  uint64_t record_count_ : 16 = 0;
  uint64_t block_size_ : 22 = 0;
  uint64_t deleted_size_ : 22 = 0;

 public:
  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  constexpr size_t
  GetControlBit() const
  {
    return control_;
  }

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

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  constexpr StatusWord
  Freeze() const
  {
    auto frozen_status = *this;
    frozen_status.frozen_ = true;
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

  std::string
  ToString() const
  {
    std::stringstream ss;
    ss << "StatusWord: 0x" << std::hex << this << "{" << std::endl
       << "  control: 0x" << GetControlBit() << "," << std::dec << std::endl
       << "  frozen: 0x" << IsFrozen() << "," << std::endl
       << "  block size: " << GetBlockSize() << "," << std::endl
       << "  delete size: " << GetDeletedSize() << "," << std::endl
       << "  record count: " << GetRecordCount() << std::endl
       << "}";
    return ss.str();
  }
};

constexpr auto kInitStatusWord = StatusWord{};

}  // namespace bztree
