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
   * Internal enums and constants
   *##############################################################################################*/

  static constexpr size_t kControlBitOffset = 61;
  static constexpr size_t kVisibleBitOffset = 60;
  static constexpr size_t kInProgressBitOffset = 59;
  static constexpr size_t kOffsetBitOffset = 32;
  static constexpr size_t kKeyLengthBitOffset = 16;
  static constexpr size_t kTotalLengthBitOffset = 0;

  // bitmask 64-62 [1110 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000]
  static constexpr uint64_t kControlMask = 0x7UL << kControlBitOffset;
  // bitmask 61    [0001 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000]
  static constexpr uint64_t kVisibleMask = 0x1UL << kVisibleBitOffset;
  // bitmask 60    [0000 1000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000]
  static constexpr uint64_t kInProgressMask = 0x1UL << kInProgressBitOffset;
  // bitmask 59-33 [0000 0111 1111 1111 1111 1111 1111 1111 0000 0000 0000 0000 0000 0000 0000 0000]
  static constexpr uint64_t kOffsetMask = 0x7FFFFFFUL << kOffsetBitOffset;
  // bitmask 32-17 [0000 0000 0000 0000 0000 0000 0000 0000 1111 1111 1111 1111 0000 0000 0000 0000]
  static constexpr uint64_t kKeyLengthMask = 0xFFFFUL << kKeyLengthBitOffset;
  // bitmask 16-1  [0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 1111 1111 1111 1111]
  static constexpr uint64_t kTotalLengthMask = 0xFFFFUL << kTotalLengthBitOffset;

  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  uint64_t control_ : 3 = 0;
  bool visible_ : 1 = false;
  bool in_progress_ : 1 = false;
  uint64_t offset_ : 27 = 0;
  uint64_t key_length_ : 16 = 0;
  uint64_t total_length_ : 16 = 0;

  /*################################################################################################
   * Internal getters/setters
   *##############################################################################################*/

  static bool
  IsInProgress(const uint64_t meta)
  {
    return (meta & kInProgressMask) > 0;
  }

  static uint64_t
  GetControlBits(const uint64_t meta)
  {
    return (meta & kControlMask) >> kControlBitOffset;
  }

  static size_t
  GetEpoch(const uint64_t meta)
  {
    return GetOffset(meta);
  }

  static uint64_t
  ToVisible(const uint64_t meta)
  {
    return (meta | kVisibleMask) & ~kInProgressMask;
  }

  static uint64_t
  ToInvisible(const uint64_t meta)
  {
    return meta & ~kVisibleMask;
  }

  static uint64_t
  SetKeyLength(  //
      const uint64_t meta,
      const size_t key_length)
  {
    assert((meta & kKeyLengthMask) == 0);           // original metadata has no key length
    assert((key_length & ~kTotalLengthMask) == 0);  // an input key length must not overflow

    return meta | (key_length << kKeyLengthBitOffset);
  }

  static uint64_t
  SetTotalLength(  //
      const uint64_t meta,
      const size_t total_length)
  {
    assert((meta & kTotalLengthMask) == 0);           // original metadata has no total length
    assert((total_length & ~kTotalLengthMask) == 0);  // an input total length must not overflow

    return meta | (total_length << kTotalLengthBitOffset);
  }

 public:
  /*################################################################################################
   * Public enums and constants
   *##############################################################################################*/

  // metadata length in bytes
  static constexpr size_t kMetadataByteLength = kWordLength;

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
  DeletePayload() const
  {
    auto new_meta = *this;
    new_meta.visible_ = false;
    new_meta.in_progress_ = false;
    return new_meta;
  }

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  static bool
  IsVisible(const uint64_t meta)
  {
    return (meta & kVisibleMask) > 0;
  }

  static bool
  IsDeleted(const uint64_t meta)
  {
    return !IsVisible(meta) && !IsInProgress(meta);
  }

  static bool
  IsNotCorrupted(const uint64_t meta, const size_t index_epoch)
  {
    return IsInProgress(meta) && (GetEpoch(meta) == index_epoch);
  }

  static size_t
  GetOffset(const uint64_t meta)
  {
    return (meta & kOffsetMask) >> kOffsetBitOffset;
  }

  static size_t
  GetKeyLength(const uint64_t meta)
  {
    return (meta & kKeyLengthMask) >> kKeyLengthBitOffset;
  }

  static size_t
  GetTotalLength(const uint64_t meta)
  {
    return (meta & kTotalLengthMask) >> kTotalLengthBitOffset;
  }

  static size_t
  GetPayloadLength(const uint64_t meta)
  {
    return GetTotalLength(meta) - GetKeyLength(meta);
  }

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  static uint64_t
  UpdateOffset(  //
      const uint64_t meta,
      const size_t offset)
  {
    return (meta & (~kOffsetMask)) | ((offset << kOffsetBitOffset) & kOffsetMask);
  }

  static uint64_t
  SetRecordInfo(  //
      const uint64_t meta,
      const size_t offset,
      const size_t key_length,
      const size_t total_length)
  {
    return ToVisible(
        SetTotalLength(SetKeyLength(UpdateOffset(meta, offset), key_length), total_length));
  }

  static uint64_t
  DeletePayload(const uint64_t meta)
  {
    return ToInvisible((meta & ~kInProgressMask));
  }

  std::string
  Dump(const uint64_t meta)
  {
    std::stringstream ss;
    ss << "0x" << std::hex << meta << "{" << std::endl
       << "  control: 0x" << GetControlBits(meta) << "," << std::endl
       << "  visible: 0x" << IsVisible(meta) << "," << std::endl
       << "  inserting: 0x" << IsInProgress(meta) << "," << std::dec << std::endl
       << "  offset/epoch: " << GetOffset(meta) << "," << std::endl
       << "  key length: " << GetKeyLength(meta) << "," << std::endl
       << "  total length: " << GetTotalLength(meta) << std::endl
       << "}";
    return ss.str();
  }
};

constexpr Metadata kInitMetadata = Metadata{};

}  // namespace bztree
