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

#ifndef BZTREE_COMPONENT_METADATA_HPP
#define BZTREE_COMPONENT_METADATA_HPP

#include "common.hpp"

namespace dbgroup::index::bztree::component
{
/**
 * @brief A class to represent record metadata.
 *
 */
class Metadata
{
 public:
  /*################################################################################################
   * Public constructors and assignment operators
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
      const size_t total_length)
      : offset_{offset},
        visible_{1},
        in_progress_{0},
        key_length_{key_length},
        total_length_{total_length},
        control_region_{0}
  {
  }

  /**
   * @brief Construct a new in-progress metadata object with given arguments.
   *
   */
  constexpr Metadata(  //
      const size_t key_length,
      const size_t total_length)
      : offset_{0},
        visible_{1},
        in_progress_{1},
        key_length_{key_length},
        total_length_{total_length},
        control_region_{0}
  {
  }

  constexpr Metadata(const Metadata &) = default;
  constexpr Metadata &operator=(const Metadata &) = default;
  constexpr Metadata(Metadata &&) = default;
  constexpr Metadata &operator=(Metadata &&) = default;

  /*################################################################################################
   * Public destructors
   *##############################################################################################*/

  /**
   * @brief Destroy the metadata object.
   *
   */
  ~Metadata() = default;

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  /**
   * @retval true if a corresponding record is visible.
   * @retval false if a corresponding record is invisible.
   */
  constexpr auto
  IsVisible() const  //
      -> bool
  {
    return visible_;
  }

  /**
   * @retval true if a corresponding record is in progress.
   * @retval false if a corresponding record is a definite state.
   */
  constexpr auto
  IsInProgress() const  //
      -> bool
  {
    return in_progress_;
  }

  /**
   * @return an offset to a corresponding record.
   */
  constexpr auto
  GetOffset() const  //
      -> size_t
  {
    return offset_;
  }

  /**
   * @return the length of a key in a corresponding record.
   */
  constexpr auto
  GetKeyLength() const  //
      -> size_t
  {
    return key_length_;
  }

  /**
   * @return the total length of a corresponding record.
   */
  constexpr auto
  GetTotalLength() const  //
      -> size_t
  {
    return total_length_;
  }

  /**
   * @return the length of a payload in a corresponding record.
   */
  constexpr auto
  GetPayloadLength() const  //
      -> size_t
  {
    return GetTotalLength() - GetKeyLength();
  }

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  /**
   * @param offset a new offset to be set.
   * @return a new metadata object.
   */
  constexpr auto
  UpdateOffset(const size_t offset) const  //
      -> Metadata
  {
    auto updated_meta = *this;
    updated_meta.offset_ = offset;
    return updated_meta;
  }

  /**
   * @brief Make metadata visible with committing it.
   *
   * @param offset a new offset to be set.
   * @return a new metadata object.
   */
  constexpr auto
  Commit(const size_t offset) const  //
      -> Metadata
  {
    auto new_meta = *this;
    new_meta.in_progress_ = 0;
    new_meta.offset_ = offset;
    return new_meta;
  }

  /**
   * @brief Make metadata invisible with committing it.
   *
   * @param offset a new offset to be set.
   * @return a new metadata object.
   */
  constexpr auto
  Delete(const size_t offset) const  //
      -> Metadata
  {
    auto new_meta = *this;
    new_meta.visible_ = 0;
    new_meta.in_progress_ = 0;
    new_meta.offset_ = offset;
    return new_meta;
  }

  /**
   * @brief Make metadata invisible.
   *
   * @return a new metadata object.
   */
  constexpr auto
  Delete() const  //
      -> Metadata
  {
    auto new_meta = *this;
    new_meta.visible_ = 0;
    return new_meta;
  }

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
};

}  // namespace dbgroup::index::bztree::component

namespace dbgroup::atomic::mwcas
{
/**
 * @brief Specialization for enabling MwCAS.
 *
 */
template <>
constexpr auto
CanMwCAS<::dbgroup::index::bztree::component::Metadata>()  //
    -> bool
{
  return true;
}
}  // namespace dbgroup::atomic::mwcas

#endif  // BZTREE_COMPONENT_METADATA_HPP
