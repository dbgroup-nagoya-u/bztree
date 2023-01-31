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

#ifndef BZTREE_COMPONENT_STATUS_WORD_HPP
#define BZTREE_COMPONENT_STATUS_WORD_HPP

#include "common.hpp"

namespace dbgroup::index::bztree::component
{
/**
 * @brief A class to represent status words of BzTree.
 *
 */
class StatusWord
{
 public:
  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  /**
   * @brief Create a zero-filled status word.
   *
   */
  constexpr StatusWord()
      : record_count_{0},
        block_size_{0},
        deleted_size_{0},
        frozen_{0},
        removed_{0},
        smo_parent_{0},
        smo_child_{0},
        control_region_{0}
  {
  }

  /**
   * @brief Create a new status word with specified arguments.
   *
   */
  constexpr StatusWord(  //
      const size_t record_count,
      const size_t block_size)
      : record_count_{record_count},
        block_size_{block_size},
        deleted_size_{0},
        frozen_{0},
        removed_{0},
        smo_parent_{0},
        smo_child_{0},
        control_region_{0}
  {
  }

  constexpr StatusWord(const StatusWord &) = default;
  constexpr StatusWord &operator=(const StatusWord &) = default;
  constexpr StatusWord(StatusWord &&) = default;
  constexpr StatusWord &operator=(StatusWord &&) = default;

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

  /**
   * @brief Destroy the status word object.
   *
   */
  ~StatusWord() = default;

  /*####################################################################################
   * Public operators
   *##################################################################################*/

  /**
   * @brief An operator to check equality.
   */
  constexpr auto
  operator==(const StatusWord &comp) const  //
      -> bool
  {
    return record_count_ == comp.record_count_     //
           && block_size_ == comp.block_size_      //
           && deleted_size_ == comp.deleted_size_  //
           && frozen_ == comp.frozen_              //
           && removed_ == comp.removed_            //
           && smo_parent_ == comp.smo_parent_      //
           && smo_child_ == comp.smo_child_;
  }

  /**
   * @brief An operator to check inequality.
   */
  constexpr auto
  operator!=(const StatusWord &comp) const  //
      -> bool
  {
    return record_count_ != comp.record_count_     //
           || block_size_ != comp.block_size_      //
           || deleted_size_ != comp.deleted_size_  //
           || frozen_ != comp.frozen_              //
           || removed_ != comp.removed_            //
           || smo_parent_ == comp.smo_parent_      //
           || smo_child_ == comp.smo_child_;
  }

  /*####################################################################################
   * Public getters/setters
   *##################################################################################*/

  /**
   * @retval true if a node is frozen (i.e., immutable).
   * @retval false otherwise.
   */
  [[nodiscard]] constexpr auto
  IsFrozen() const  //
      -> bool
  {
    return frozen_;
  }

  /**
   * @retval true if a node is merged right node.
   * @retval false otherwise.
   */
  [[nodiscard]] constexpr auto
  IsRemoved() const  //
      -> bool
  {
    return removed_;
  }

  /**
   * @retval true if a node is a parent of the smo node.
   * @retval false otherwise.
   */
  [[nodiscard]] constexpr auto
  IsSmoParent() const  //
      -> bool
  {
    return smo_parent_;
  }

  /**
   * @retval true if a node is a child of the smo node.
   * @retval false otherwise.
   */
  [[nodiscard]] constexpr auto
  IsSmoChild() const  //
      -> bool
  {
    return smo_child_;
  }

  /**
   * @return the total number of records in a node.
   */
  [[nodiscard]] constexpr auto
  GetRecordCount() const  //
      -> size_t
  {
    return record_count_;
  }

  /**
   * @return the total byte length of records in a node.
   */
  [[nodiscard]] constexpr auto
  GetBlockSize() const  //
      -> size_t
  {
    return block_size_;
  }

  /*####################################################################################
   * Public utility functions
   *##################################################################################*/

  /**
   * @param sorted_count the number of sorted records in this node.
   * @retval true if this node should be consolidated.
   * @retval false otherwise.
   */
  [[nodiscard]] constexpr auto
  NeedConsolidation(const size_t sorted_count) const  //
      -> bool
  {
    const auto total_size = (kWordSize * record_count_) + block_size_;

    return record_count_ - sorted_count > kMaxDeltaRecNum || total_size > kPageSize - kHeaderLen  //
           || deleted_size_ > kMaxDeletedSpaceSize;
  }

  /**
   * @retval true if this node does not have sufficient free space.
   * @retval false otherwise.
   */
  template <class Key, class Payload>
  [[nodiscard]] constexpr auto
  NeedSplit() const  //
      -> bool
  {
    constexpr auto kKeyLen = (IsVarLenData<Key>()) ? kMaxVarDataSize : sizeof(Key);
    constexpr auto kRecLen = kWordSize + kKeyLen + sizeof(Payload);
    constexpr auto kMinBlockSize = (kRecLen > kMinFreeSpaceSize) ? kRecLen : kMinFreeSpaceSize;
    constexpr auto kMaxUsedSize = kPageSize - (kHeaderLen + kMinBlockSize);

    const auto this_size = (kWordSize * record_count_) + block_size_ - deleted_size_;
    return this_size > kMaxUsedSize;
  }

  /**
   * @retval true if this node does not have sufficient free space.
   * @retval false otherwise.
   */
  template <class Key>
  [[nodiscard]] constexpr auto
  NeedInternalSplit() const  //
      -> bool
  {
    constexpr size_t kMaxRecLen = (IsVarLenData<Key>() ? kMaxVarDataSize : sizeof(Key)) + kWordSize;
    constexpr size_t kPaddedLen = Pad<void *>(kMaxRecLen);
    constexpr size_t kMaxUsedSize = kPageSize - (kHeaderLen + kWordSize + kPaddedLen);
    const auto this_size = (kWordSize * record_count_) + block_size_;

    return this_size > kMaxUsedSize;
  }

  /**
   * @retval true if this node does not have sufficient free space.
   * @retval false otherwise.
   */
  [[nodiscard]] constexpr auto
  NeedMerge() const  //
      -> bool
  {
    const auto this_size = (kWordSize * record_count_) + block_size_;

    return this_size < kMinNodeSize - kHeaderLen;
  }

  /**
   * @param stat the status word of a sibling node.
   * @retval true if this node can merge with a given sibling node.
   * @retval false otherwise.
   */
  [[nodiscard]] constexpr auto
  CanMergeWith(const StatusWord stat) const  //
      -> bool
  {
    const auto this_size = (kWordSize * record_count_) + block_size_ - deleted_size_;
    const auto sib_size = (kWordSize * stat.record_count_) + stat.block_size_ - stat.deleted_size_;

    return this_size + sib_size < kMaxMergedSize - kHeaderLen;
  }

  /**
   * @return a frozen status word.
   */
  [[nodiscard]] constexpr auto
  Freeze(const bool is_removed,
         const bool is_smo_parent) const  //
      -> StatusWord
  {
    auto frozen_status = *this;
    frozen_status.frozen_ = 1;
    if (is_removed) frozen_status.removed_ = 1;
    if (is_smo_parent) frozen_status.smo_parent_ = 1;
    return frozen_status;
  }

  /**
   * @return a frozen status word.
   */
  [[nodiscard]] constexpr auto
  Unfreeze() const  //
      -> StatusWord
  {
    auto unfrozen_status = *this;
    unfrozen_status.frozen_ = 0;
    // unfrozen_status.removed_ = 0; //後々，手を加える必要あり？？？？？？？？？？？？？？
    //  unfrozen_status.smo_parent_ = 0;
    return unfrozen_status;
  }

  /**
   * @param rec_size the byte length of an added record.
   * @return a new status word with added records.
   */
  [[nodiscard]] constexpr auto
  Add(const size_t rec_size) const  //
      -> StatusWord
  {
    auto new_status = *this;
    ++new_status.record_count_;
    new_status.block_size_ += rec_size;
    return new_status;
  }

  /**
   * @param deleted_size: the byte length of deleted metadata and records.
   * @return a new status word with deleted records.
   */
  [[nodiscard]] constexpr auto
  Delete(const size_t deleted_size) const  //
      -> StatusWord
  {
    auto new_status = *this;
    new_status.deleted_size_ += deleted_size;
    return new_status;
  }

 private:
  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// the total number of records in a node.
  uint64_t record_count_ : 16;

  /// the total byte length of records in a node.
  uint64_t block_size_ : 22;

  /// the total byte length of deleted metadata/records in a node.
  uint64_t deleted_size_ : 19;  //-3

  /// a flag to indicate whether a node is frozen (i.e., immutable).
  uint64_t frozen_ : 1;

  uint64_t removed_ : 1;

  uint64_t smo_parent_ : 1;

  uint64_t smo_child_ : 1;

  /// control bits to perform PMwCAS.
  uint64_t control_region_ : 3;  // NOLINT
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
CanMwCAS<::dbgroup::index::bztree::component::StatusWord>()
{
  return true;
}
}  // namespace dbgroup::atomic::mwcas

#endif  // BZTREE_COMPONENT_STATUS_WORD_HPP
