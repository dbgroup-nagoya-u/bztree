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
struct StatusWord {
 private:
  /*################################################################################################
   * Internal enums and constants
   *##############################################################################################*/

  static constexpr size_t kControlBitOffset = 61;
  static constexpr size_t kFrozenBitOffset = 60;
  static constexpr size_t kRecordCountBitOffset = 44;
  static constexpr size_t kBlockSizeBitOffset = 22;
  static constexpr size_t kDeletedSizeBitOffset = 0;

  // bitmask 64-62
  static constexpr uint64_t kControlMask = 0x7UL << kControlBitOffset;
  // bitmask 61
  static constexpr uint64_t kFrozenMask = 0x1UL << kFrozenBitOffset;
  // bitmask 60-45
  static constexpr uint64_t kRecordCountMask = 0xFFFFUL << kRecordCountBitOffset;
  // bitmask 44-23
  static constexpr uint64_t kBlockSizeMask = 0x3FFFFFUL << kBlockSizeBitOffset;
  // bitmask 22-1
  static constexpr uint64_t kDeleteSizeMask = 0x3FFFFFUL << kDeletedSizeBitOffset;

  /*################################################################################################
   * Internal getters/setters
   *##############################################################################################*/

  static uint64_t
  GetControlBits(const uint64_t status)
  {
    return (status & kControlMask) >> kControlBitOffset;
  }

 public:
  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  static bool
  IsFrozen(const uint64_t status)
  {
    return (status & kFrozenMask) > 0;
  }

  static size_t
  GetRecordCount(const uint64_t status)
  {
    return (status & kRecordCountMask) >> kRecordCountBitOffset;
  }

  static size_t
  GetBlockSize(const uint64_t status)
  {
    return (status & kBlockSizeMask) >> kBlockSizeBitOffset;
  }

  static size_t
  GetDeletedSize(const uint64_t status)
  {
    return (status & kDeleteSizeMask) >> kDeletedSizeBitOffset;
  }

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  static uint64_t
  Freeze(const uint64_t status)
  {
    return status | kFrozenMask;
  }

  static uint64_t
  AddRecordInfo(  //
      const uint64_t status,
      const size_t record_count,
      const size_t block_size,
      const size_t deleted_size)
  {
    // each incremented values must not overflow
    assert((GetRecordCount(status) + (record_count << kRecordCountBitOffset))
           < (1UL << kFrozenBitOffset));
    assert((GetBlockSize(status) + (block_size << kBlockSizeBitOffset))
           < (1UL << kRecordCountBitOffset));
    assert((GetDeletedSize(status) + (deleted_size << kDeletedSizeBitOffset))
           < (1UL << kBlockSizeBitOffset));

    return status + (record_count << kRecordCountBitOffset) + (block_size << kBlockSizeBitOffset)
           + (deleted_size << kDeletedSizeBitOffset);
  }

  static std::string
  Dump(const uint64_t status)
  {
    std::stringstream ss;
    ss << "StatusWord: 0x" << std::hex << status << "{" << std::endl
       << "  control: 0x" << GetControlBits(status) << "," << std::dec << std::endl
       << "  frozen: 0x" << IsFrozen(status) << "," << std::endl
       << "  block size: " << GetBlockSize(status) << "," << std::endl
       << "  delete size: " << GetDeletedSize(status) << "," << std::endl
       << "  record count: " << GetRecordCount(status) << std::endl
       << "}";
    return ss.str();
  }
};

}  // namespace bztree
