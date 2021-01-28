// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <string>

#include "common.hpp"

namespace bztree
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

  uint64_t control_ : 3 = 0;
  bool visible_ : 1 = false;
  bool in_progress_ : 1 = false;
  uint64_t offset_ : 27 = 0;
  uint64_t key_length_ : 16 = 0;
  uint64_t total_length_ : 16 = 0;

 public:
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
  IsCorrupted(const size_t index_epoch) const
  {
    return IsInProgress() && (GetOffset() != index_epoch);
  }

  constexpr uint64_t
  GetControlBit() const
  {
    return control_;
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
  InitForInsert(const size_t index_epoch) const
  {
    auto inserting_meta = *this;
    inserting_meta.in_progress_ = true;
    inserting_meta.offset_ = index_epoch;
    return inserting_meta;
  }

  constexpr Metadata
  UpdateOffset(const size_t offset) const
  {
    auto updated_meta = *this;
    updated_meta.offset_ = offset;
    return updated_meta;
  }

  constexpr Metadata
  SetRecordInfo(  //
      const size_t offset,
      const size_t key_length,
      const size_t total_length) const
  {
    auto new_meta = *this;
    new_meta.visible_ = true;
    new_meta.in_progress_ = false;
    new_meta.offset_ = offset;
    new_meta.key_length_ = key_length;
    new_meta.total_length_ = total_length;
    return new_meta;
  }

  constexpr Metadata
  DeleteRecordInfo() const
  {
    auto new_meta = *this;
    new_meta.visible_ = false;
    new_meta.in_progress_ = false;
    return new_meta;
  }

  std::string
  ToString() const
  {
    std::stringstream ss;
    ss << "0x" << std::hex << this << "{" << std::endl
       << "  control: 0x" << GetControlBit() << "," << std::endl
       << "  visible: 0x" << IsVisible() << "," << std::endl
       << "  inserting: 0x" << IsInProgress() << "," << std::dec << std::endl
       << "  offset/epoch: " << GetOffset() << "," << std::endl
       << "  key length: " << GetKeyLength() << "," << std::endl
       << "  total length: " << GetTotalLength() << std::endl
       << "}";
    return ss.str();
  }
};

constexpr Metadata kInitMetadata = Metadata{};

}  // namespace bztree
