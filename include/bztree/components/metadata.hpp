// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

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

  constexpr explicit Metadata(const bool is_in_progress)
      : offset_{0},
        visible_{0},
        in_progress_{is_in_progress},
        key_length_{0},
        total_length_{0},
        control_region_{0}
  {
  }

  ~Metadata() = default;

  Metadata(const Metadata &) = default;
  Metadata &operator=(const Metadata &) = default;
  Metadata(Metadata &&) = default;
  Metadata &operator=(Metadata &&) = default;

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

  static constexpr Metadata
  GetInsertingMeta(const size_t index_epoch)
  {
    auto inserting_meta = Metadata{true};
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
    new_meta.visible_ = 1;
    new_meta.in_progress_ = 0;
    new_meta.offset_ = offset;
    new_meta.key_length_ = key_length;
    new_meta.total_length_ = total_length;
    return new_meta;
  }

  constexpr Metadata
  DeleteRecordInfo() const
  {
    auto new_meta = *this;
    new_meta.visible_ = 0;
    new_meta.in_progress_ = 0;
    return new_meta;
  }
};

}  // namespace bztree
