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
 * @brief A class to represent record metadata.
 *
 */
class Metadata
{
 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  /// an offset to a corresponding record.
  uint64_t offset_ : 27;

  /// a flag to indicate whether a record is visible.
  uint64_t visible_ : 1;

  /// a flag to indicate whether a record is in progress.
  uint64_t in_progress_ : 1;

  /// the length of a key in a corresponding record.
  uint64_t key_length_ : 16;

  /// the total length of a corresponding record.
  uint64_t total_length_ : 16;

  /// control bits for PMwCAS.
  uint64_t control_region_ : 3;

 public:
  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  /**
   * @brief Construct a new metadata object with zero padding.
   *
   */
  constexpr Metadata()
      : offset_{0},
        visible_{0},
        in_progress_{0},
        key_length_{0},
        total_length_{0},
        control_region_{0}
  {
  }

  /**
   * @brief Construct a new metadata object with given arguments.
   *
   */
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

  /**
   * @brief Destroy the metadata object.
   *
   */
  ~Metadata() = default;

  constexpr Metadata(const Metadata &) = default;
  constexpr Metadata &operator=(const Metadata &) = default;
  constexpr Metadata(Metadata &&) = default;
  constexpr Metadata &operator=(Metadata &&) = default;

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  /**
   * @retval true if a corresponding record is visible.
   * @retval false if a corresponding record is invisible.
   */
  constexpr bool
  IsVisible() const
  {
    return visible_;
  }

  /**
   * @retval true if a corresponding record is in progress.
   * @retval false if a corresponding record is a definite state.
   */
  constexpr bool
  IsInProgress() const
  {
    return in_progress_;
  }

  /**
   * @retval true if a corresponding record is deleted.
   * @retval false if a corresponding record is live.
   */
  constexpr bool
  IsDeleted() const
  {
    return !IsVisible() && !IsInProgress();
  }

  /**
   * @retval true if a corresponding record is broken because of failure.
   * @retval false if a corresponding record is valid.
   */
  constexpr bool
  IsFailedRecord(const size_t index_epoch) const
  {
    return IsInProgress() && (GetOffset() != index_epoch);
  }

  /**
   * @return size_t: an offset to a corresponding record.
   */
  constexpr size_t
  GetOffset() const
  {
    return offset_;
  }

  /**
   * @return size_t: the length of a key in a corresponding record.
   */
  constexpr size_t
  GetKeyLength() const
  {
    return key_length_;
  }

  /**
   * @return size_t: the total length of a corresponding record.
   */
  constexpr size_t
  GetTotalLength() const
  {
    return total_length_;
  }

  /**
   * @return size_t: the length of a payload in a corresponding record.
   */
  constexpr size_t
  GetPayloadLength() const
  {
    return GetTotalLength() - GetKeyLength();
  }

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  /**
   * @param offset a new offset to be set.
   * @return Metadata: a new metadata object.
   */
  constexpr Metadata
  UpdateOffset(const size_t offset) const
  {
    auto updated_meta = *this;
    updated_meta.offset_ = offset;
    return updated_meta;
  }

  /**
   * @brief Make metadata visible with updating its offset.
   *
   * @param offset a new offset to be set.
   * @return Metadata: a new metadata object.
   */
  constexpr Metadata
  MakeVisible(const size_t offset) const
  {
    auto new_meta = *this;
    new_meta.visible_ = 1;
    new_meta.in_progress_ = 0;
    new_meta.offset_ = offset;
    return new_meta;
  }

  /**
   * @brief Make metadata invisible with updating its offset.
   *
   * @param offset a new offset to be set.
   * @return Metadata: a new metadata object.
   */
  constexpr Metadata
  MakeInvisible(const size_t offset) const
  {
    auto new_meta = *this;
    new_meta.in_progress_ = 0;
    new_meta.offset_ = offset;
    return new_meta;
  }

  /**
   * @brief Make metadata invisible.
   *
   * @return Metadata: a new metadata object.
   */
  constexpr Metadata
  Delete() const
  {
    auto new_meta = *this;
    new_meta.visible_ = 0;
    return new_meta;
  }
};

}  // namespace dbgroup::index::bztree::component

namespace dbgroup::atomic::mwcas
{
/**
 * @brief Specialization for enabling MwCAS.
 *
 */
template <>
constexpr bool
CanMwCAS<::dbgroup::index::bztree::component::Metadata>()
{
  return true;
}
}  // namespace dbgroup::atomic::mwcas
