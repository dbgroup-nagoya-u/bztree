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

#include <functional>
#include <list>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "base_node.hpp"
#include "record.hpp"

namespace dbgroup::index::bztree
{
template <class Key, class Payload, class Compare = std::less<Key>>
class LeafNode
{
  using BaseNode_t = BaseNode<Key, Payload, Compare>;
  using KeyExistence = typename BaseNode_t::KeyExistence;
  using NodeReturnCode = typename BaseNode_t::NodeReturnCode;
  using Record_t = Record<Key, Payload>;

 private:
  /*################################################################################################
   * Internal structs to cpmare key & metadata pairs
   *##############################################################################################*/

  struct PairComp {
    PairComp() {}

    constexpr bool
    operator()(  //
        const std::pair<Key, Metadata> &a,
        const std::pair<Key, Metadata> &b) const noexcept
    {
      return Compare{}(a.first, b.first);
    }
  };

  struct PairEqual {
    PairEqual() {}

    constexpr bool
    operator()(  //
        const std::pair<Key, Metadata> &a,
        const std::pair<Key, Metadata> &b) const noexcept
    {
      return IsEqual<Compare>(a.first, b.first);
    }
  };

  /*################################################################################################
   * Internal utility functions
   *##############################################################################################*/

  static constexpr KeyExistence
  SearchUnsortedMetaToWrite(  //
      const BaseNode_t *node,
      const Key &key,
      const int64_t begin_index,
      const int64_t sorted_count,
      const size_t index_epoch)
  {
    // perform a linear search in revese order
    for (int64_t index = begin_index; index >= sorted_count; --index) {
      const auto meta = node->GetMetadataProtected(index);
      if (meta.IsInProgress()) {
        if (meta.GetOffset() == index_epoch) {
          return KeyExistence::kUncertain;
        }
        continue;  // failed record
      }

      const auto target_key = CastKey<Key>(node->GetKeyAddr(meta));
      if (IsEqual<Compare>(key, target_key)) {
        if (meta.IsVisible()) {
          return KeyExistence::kExist;
        }
        return KeyExistence::kDeleted;
      }
    }
    return KeyExistence::kNotExist;
  }

  static constexpr KeyExistence
  CheckUniqueness(  //
      const BaseNode_t *node,
      const Key &key,
      const int64_t record_count,
      const size_t index_epoch)
  {
    const auto existence =
        SearchUnsortedMetaToWrite(node, key, record_count - 1, node->GetSortedCount(), index_epoch);
    if (existence == KeyExistence::kNotExist) {
      // there is no key in unsorted metadata, so search a sorted region
      return node->SearchSortedMetadata(key, true).first;
    } else {
      return existence;
    }
  }

  static constexpr std::pair<KeyExistence, size_t>
  SearchUnsortedMetaToRead(  //
      const BaseNode_t *node,
      const Key &key,
      const int64_t end_index,
      const int64_t record_count)
  {
    for (int64_t index = record_count - 1; index >= end_index; --index) {
      const auto meta = node->GetMetadataProtected(index);
      if (meta.IsInProgress()) {
        continue;
      }

      const auto target_key = CastKey<Key>(node->GetKeyAddr(meta));
      if (IsEqual<Compare>(key, target_key)) {
        if (meta.IsVisible()) {
          return {KeyExistence::kExist, index};
        }
        return {KeyExistence::kDeleted, index};
      }
    }
    return {KeyExistence::kNotExist, 0};
  }

  static constexpr std::pair<KeyExistence, size_t>
  SearchMetadataToRead(  //
      const BaseNode_t *node,
      const Key &key,
      const size_t record_count)
  {
    const auto [existence, index] =
        SearchUnsortedMetaToRead(node, key, node->GetSortedCount(), record_count);
    if (existence == KeyExistence::kExist || existence == KeyExistence::kDeleted) {
      return {existence, index};
    } else {
      return node->SearchSortedMetadata(key, true);
    }
  }

  static constexpr void
  CopyRecordsViaMetadata(  //
      BaseNode_t *copied_node,
      const BaseNode_t *original_node,
      const typename std::vector<std::pair<Key, Metadata>>::const_iterator begin_iter,
      const typename std::vector<std::pair<Key, Metadata>>::const_iterator end_iter)
  {
    const auto node_size = original_node->GetNodeSize();

    auto record_count = copied_node->GetSortedCount();
    auto offset = node_size - copied_node->GetStatusWord().GetBlockSize();
    for (auto iter = begin_iter; iter != end_iter; ++record_count, ++iter) {
      // copy a record
      const auto [key, meta] = *iter;
      offset = copied_node->CopyRecord(original_node, meta, offset);
      // copy metadata
      const auto new_meta = meta.UpdateOffset(offset);
      copied_node->SetMetadata(record_count, new_meta);
    }
    copied_node->SetStatus(StatusWord{}.AddRecordInfo(record_count, node_size - offset, 0));
    copied_node->SetSortedCount(record_count);
  }

 public:
  /*################################################################################################
   * Read operations
   *##############################################################################################*/

  /**
   * @brief
   *
   * @tparam Compare
   * @param key
   * @param key_length
   * @return std::pair<NodeReturnCode, std::unique_ptr<std::byte[]>>
   */
  static constexpr std::pair<NodeReturnCode, std::unique_ptr<Record_t>>
  Read(  //
      const BaseNode_t *node,
      const Key &key)
  {
    const auto status = node->GetStatusWordProtected();
    const auto [existence, index] = SearchMetadataToRead(node, key, status.GetRecordCount());
    if (existence == KeyExistence::kNotExist || existence == KeyExistence::kDeleted) {
      return {NodeReturnCode::kKeyNotExist, nullptr};
    } else {
      const auto meta = node->GetMetadataProtected(index);
      return {NodeReturnCode::kSuccess, node->GetRecord(meta)};
    }
  }

  /**
   * @brief
   *
   * @tparam Compare
   * @param begin_key
   * @param begin_is_closed
   * @param end_key
   * @param end_is_closed
   * @return std::pair<NodeReturnCode,
   *         std::vector<std::pair<std::unique_ptr<std::byte[]>, std::unique_ptr<std::byte[]>>>>
   */
  static constexpr std::pair<NodeReturnCode, std::vector<std::unique_ptr<Record_t>>>
  Scan(  //
      const BaseNode_t *node,
      const Key *begin_key,
      const bool begin_is_closed,
      const Key *end_key,
      const bool end_is_closed)
  {
    const auto status = node->GetStatusWordProtected();
    const int64_t record_count = status.GetRecordCount();
    const int64_t sorted_count = node->GetSortedCount();

    // gather valid (live or deleted) records
    std::vector<std::pair<Key, Metadata>> meta_arr;
    meta_arr.reserve(record_count);

    // search unsorted metadata in reverse order
    for (int64_t index = record_count - 1; index >= sorted_count; --index) {
      const auto meta = node->GetMetadataProtected(index);
      const auto key = CastKey<Key>(node->GetKeyAddr(meta));
      if (IsInRange<Compare>(key, begin_key, begin_is_closed, end_key, end_is_closed)
          && (meta.IsVisible() || meta.IsDeleted())) {
        meta_arr.emplace_back(key, meta);
      }
    }

    // search sorted metadata
    const auto begin_index =
        (begin_key == nullptr) ? 0 : node->SearchSortedMetadata(*begin_key, begin_is_closed).second;
    for (int64_t index = begin_index; index < sorted_count; ++index) {
      const auto meta = node->GetMetadataProtected(index);
      const auto key = CastKey<Key>(node->GetKeyAddr(meta));
      if (IsInRange<Compare>(key, begin_key, begin_is_closed, end_key, end_is_closed)) {
        meta_arr.emplace_back(key, meta);
        if (end_key != nullptr && end_is_closed && IsEqual<Compare>(key, *end_key)) {
          break;  // a current key is end of range condition
        }
      } else {
        break;  // a current key is out of range condition
      }
    }

    // make unique with keeping the order of writes
    std::stable_sort(meta_arr.begin(), meta_arr.end(), PairComp{});
    auto end_iter = std::unique(meta_arr.begin(), meta_arr.end(), PairEqual{});
    meta_arr.erase(end_iter, meta_arr.end());

    // copy live records for return
    std::vector<std::unique_ptr<Record_t>> scan_results;
    scan_results.reserve(meta_arr.size());
    for (auto &&[key, meta] : meta_arr) {
      if (meta.IsVisible()) {
        scan_results.emplace_back(node->GetRecord(meta));
      }
    }
    return {NodeReturnCode::kSuccess, std::move(scan_results)};
  }

  /*################################################################################################
   * Write operations
   *##############################################################################################*/

  /**
   * @brief
   *
   * @tparam Compare
   * @param key
   * @param key_length
   * @param payload
   * @param payload_length
   * @param descriptor_pool
   * @return NodeReturnCode
   */
  static constexpr std::pair<NodeReturnCode, StatusWord>
  Write(  //
      BaseNode_t *node,
      const Key &key,
      const size_t key_length,
      const Payload &payload,
      const size_t payload_length,
      const size_t index_epoch = 1)
  {
    // variables and constants shared in Phase 1 & 2
    StatusWord current_status;
    size_t record_count;
    const auto total_length = key_length + payload_length;
    const auto inserting_meta = Metadata::GetInsertingMeta(index_epoch);

    /*----------------------------------------------------------------------------------------------
     * Phase 1: reserve free space to write a record
     *--------------------------------------------------------------------------------------------*/
    bool mwcas_success;
    do {
      current_status = node->GetStatusWordProtected();
      if (current_status.IsFrozen()) {
        return {NodeReturnCode::kFrozen, StatusWord{}};
      }

      if (current_status.GetOccupiedSize() + kWordLength + total_length > node->GetNodeSize()) {
        return {NodeReturnCode::kNoSpace, StatusWord{}};
      }

      // prepare for MwCAS
      record_count = current_status.GetRecordCount();
      const auto new_status = current_status.AddRecordInfo(1, total_length, 0);

      // perform MwCAS to reserve space
      auto desc = MwCASDescriptor{};
      node->SetStatusForMwCAS(desc, current_status, new_status);
      node->SetMetadataForMwCAS(desc, record_count, Metadata{}, inserting_meta);
      mwcas_success = desc.MwCAS();
    } while (!mwcas_success);

    /*----------------------------------------------------------------------------------------------
     * Phase 2: write a record and check conflicts
     *--------------------------------------------------------------------------------------------*/

    // insert a record
    auto offset = node->GetNodeSize() - current_status.GetBlockSize();
    offset = node->SetRecord(key, key_length, payload, payload_length, offset);

    // prepare record metadata for MwCAS
    const auto inserted_meta = inserting_meta.SetRecordInfo(offset, key_length, total_length);

    // check conflicts (concurrent SMOs)
    do {
      current_status = node->GetStatusWordProtected();
      if (current_status.IsFrozen()) {
        return {NodeReturnCode::kFrozen, StatusWord{}};
      }

      // perform MwCAS to complete a write
      auto desc = MwCASDescriptor{};
      node->SetStatusForMwCAS(desc, current_status, current_status);
      node->SetMetadataForMwCAS(desc, record_count, inserting_meta, inserted_meta);
      mwcas_success = desc.MwCAS();
    } while (!mwcas_success);

    return {NodeReturnCode::kSuccess, current_status};
  }

  /**
   * @brief
   *
   * @tparam Compare
   * @param key
   * @param key_length
   * @param payload
   * @param payload_length
   * @param descriptor_pool
   * @return NodeReturnCode
   */
  static constexpr std::pair<NodeReturnCode, StatusWord>
  Insert(  //
      BaseNode_t *node,
      const Key &key,
      const size_t key_length,
      const Payload &payload,
      const size_t payload_length,
      const size_t index_epoch = 1)
  {
    // variables and constants shared in Phase 1 & 2
    StatusWord current_status, new_status;
    size_t record_count;
    const auto total_length = key_length + payload_length;
    const auto inserting_meta = Metadata::GetInsertingMeta(index_epoch);

    // local flags for insertion
    auto uniqueness = KeyExistence::kNotExist;

    /*----------------------------------------------------------------------------------------------
     * Phase 1: reserve free space to insert a record
     *--------------------------------------------------------------------------------------------*/
    bool mwcas_success;
    do {
      current_status = node->GetStatusWordProtected();
      if (current_status.IsFrozen()) {
        return {NodeReturnCode::kFrozen, StatusWord{}};
      }

      record_count = current_status.GetRecordCount();
      if (uniqueness != KeyExistence::kUncertain) {
        uniqueness = CheckUniqueness(node, key, record_count, index_epoch);
        if (uniqueness == KeyExistence::kExist) {
          return {NodeReturnCode::kKeyExist, current_status};
        }
      }

      if (current_status.GetOccupiedSize() + kWordLength + total_length > node->GetNodeSize()) {
        return {NodeReturnCode::kNoSpace, StatusWord{}};
      }

      // prepare new status for MwCAS
      new_status = current_status.AddRecordInfo(1, total_length, 0);

      // perform MwCAS to reserve space
      auto desc = MwCASDescriptor{};
      node->SetStatusForMwCAS(desc, current_status, new_status);
      node->SetMetadataForMwCAS(desc, record_count, Metadata{}, inserting_meta);
      mwcas_success = desc.MwCAS();

      if (!mwcas_success) {
        uniqueness = KeyExistence::kUncertain;
      }
    } while (!mwcas_success);

    /*----------------------------------------------------------------------------------------------
     * Phase 2: insert a record and check conflicts
     *--------------------------------------------------------------------------------------------*/

    // insert a record
    auto offset = node->GetNodeSize() - current_status.GetBlockSize();
    offset = node->SetRecord(key, key_length, payload, payload_length, offset);

    // prepare record metadata for MwCAS
    const auto inserted_meta = inserting_meta.SetRecordInfo(offset, key_length, total_length);

    // recheck uniqueness
    while (uniqueness == KeyExistence::kUncertain) {
      uniqueness = CheckUniqueness(node, key, record_count, index_epoch);
      if (uniqueness == KeyExistence::kExist) {
        // delete an inserted record
        node->SetMetadataByCAS(record_count, inserting_meta.UpdateOffset(0));
        return {NodeReturnCode::kKeyExist, new_status};
      }
    }

    // check concurrent SMOs
    do {
      current_status = node->GetStatusWordProtected();
      if (current_status.IsFrozen()) {
        // delete an inserted record
        node->SetMetadataByCAS(record_count, inserting_meta.UpdateOffset(0));
        return {NodeReturnCode::kFrozen, StatusWord{}};
      }

      // perform MwCAS to complete an insert
      auto desc = MwCASDescriptor{};
      node->SetStatusForMwCAS(desc, current_status, current_status);
      node->SetMetadataForMwCAS(desc, record_count, inserting_meta, inserted_meta);
      mwcas_success = desc.MwCAS();
    } while (!mwcas_success);

    return {NodeReturnCode::kSuccess, current_status};
  }

  /**
   * @brief
   *
   * @tparam Compare
   * @param key
   * @param key_length
   * @param payload
   * @param payload_length
   * @param descriptor_pool
   * @return NodeReturnCode
   */
  static constexpr std::pair<NodeReturnCode, StatusWord>
  Update(  //
      BaseNode_t *node,
      const Key &key,
      const size_t key_length,
      const Payload &payload,
      const size_t payload_length,
      const size_t index_epoch = 1)
  {
    // variables and constants shared in Phase 1 & 2
    StatusWord current_status;
    size_t record_count;
    const auto total_length = key_length + payload_length;
    const auto inserting_meta = Metadata::GetInsertingMeta(index_epoch);

    /*----------------------------------------------------------------------------------------------
     * Phase 1: reserve free space to insert a record
     *--------------------------------------------------------------------------------------------*/
    bool mwcas_success;
    do {
      current_status = node->GetStatusWordProtected();
      if (current_status.IsFrozen()) {
        return {NodeReturnCode::kFrozen, StatusWord{}};
      }

      record_count = current_status.GetRecordCount();
      const auto [existence, updated_index] = SearchMetadataToRead(node, key, record_count);
      if (existence == KeyExistence::kNotExist || existence == KeyExistence::kDeleted) {
        return {NodeReturnCode::kKeyNotExist, current_status};
      }

      if (current_status.GetOccupiedSize() + kWordLength + total_length > node->GetNodeSize()) {
        return {NodeReturnCode::kNoSpace, StatusWord{}};
      }

      // prepare new status for MwCAS
      const auto updated_meta = node->GetMetadataProtected(updated_index);
      const auto deleted_size = kWordLength + updated_meta.GetTotalLength();
      const auto new_status = current_status.AddRecordInfo(1, total_length, deleted_size);

      // perform MwCAS to reserve space
      auto desc = MwCASDescriptor{};
      node->SetStatusForMwCAS(desc, current_status, new_status);
      node->SetMetadataForMwCAS(desc, record_count, Metadata{}, inserting_meta);
      mwcas_success = desc.MwCAS();
    } while (!mwcas_success);

    /*----------------------------------------------------------------------------------------------
     * Phase 2: insert a record and check conflicts
     *--------------------------------------------------------------------------------------------*/

    // insert a record
    auto offset = node->GetNodeSize() - current_status.GetBlockSize();
    offset = node->SetRecord(key, key_length, payload, payload_length, offset);

    // prepare record metadata for MwCAS
    const auto inserted_meta = inserting_meta.SetRecordInfo(offset, key_length, total_length);

    // check conflicts (concurrent SMOs)
    do {
      current_status = node->GetStatusWordProtected();
      if (current_status.IsFrozen()) {
        return {NodeReturnCode::kFrozen, StatusWord{}};
      }

      // perform MwCAS to complete an update
      auto desc = MwCASDescriptor{};
      node->SetStatusForMwCAS(desc, current_status, current_status);
      node->SetMetadataForMwCAS(desc, record_count, inserting_meta, inserted_meta);
      mwcas_success = desc.MwCAS();
    } while (!mwcas_success);

    return {NodeReturnCode::kSuccess, current_status};
  }

  /**
   * @brief
   *
   * @tparam Compare
   * @param key
   * @param key_length
   * @param descriptor_pool
   * @return NodeReturnCode
   */
  static constexpr std::pair<NodeReturnCode, StatusWord>
  Delete(  //
      BaseNode_t *node,
      const Key &key,
      const size_t key_length)
  {
    // variables and constants
    StatusWord new_status;

    bool mwcas_success;
    do {
      const auto current_status = node->GetStatusWordProtected();
      if (current_status.IsFrozen()) {
        return {NodeReturnCode::kFrozen, StatusWord{}};
      }

      const auto record_count = current_status.GetRecordCount();
      const auto [existence, index] = SearchMetadataToRead(node, key, record_count);
      if (existence == KeyExistence::kNotExist || existence == KeyExistence::kDeleted) {
        return {NodeReturnCode::kKeyNotExist, current_status};
      }

      // delete payload infomation from metadata
      const auto current_meta = node->GetMetadataProtected(index);
      const auto deleted_meta = current_meta.DeleteRecordInfo();

      // prepare new status
      const auto deleted_block_size = kWordLength + key_length + current_meta.GetPayloadLength();
      new_status = current_status.AddRecordInfo(0, 0, deleted_block_size);

      // perform MwCAS to reserve space
      auto desc = MwCASDescriptor{};
      node->SetStatusForMwCAS(desc, current_status, new_status);
      node->SetMetadataForMwCAS(desc, index, current_meta, deleted_meta);
      mwcas_success = desc.MwCAS();
    } while (!mwcas_success);

    return {NodeReturnCode::kSuccess, new_status};
  }

  /*################################################################################################
   * Public structure modification operations
   *##############################################################################################*/

  static constexpr BaseNode_t *
  Consolidate(  //
      const BaseNode_t *node,
      const std::vector<std::pair<Key, Metadata>> &live_meta)
  {
    // create a new node and copy records
    auto new_node = BaseNode_t::CreateEmptyNode(node->GetNodeSize(), true);
    CopyRecordsViaMetadata(new_node, node, live_meta.begin(), live_meta.end());

    return new_node;
  }

  static constexpr std::pair<BaseNode_t *, BaseNode_t *>
  Split(  //
      const BaseNode_t *node,
      const std::vector<std::pair<Key, Metadata>> &sorted_meta,
      const size_t left_record_count)
  {
    const auto node_size = node->GetNodeSize();
    const auto split_iter = sorted_meta.begin() + left_record_count;

    // create a split left node
    auto left_node = BaseNode_t::CreateEmptyNode(node_size, true);
    CopyRecordsViaMetadata(left_node, node, sorted_meta.begin(), split_iter);

    // create a split right node
    auto right_node = BaseNode_t::CreateEmptyNode(node_size, true);
    CopyRecordsViaMetadata(right_node, node, split_iter, sorted_meta.end());

    return {left_node, right_node};
  }

  static constexpr BaseNode_t *
  Merge(  //
      const BaseNode_t *target_node,
      const std::vector<std::pair<Key, Metadata>> &target_meta,
      const BaseNode_t *sibling_node,
      const std::vector<std::pair<Key, Metadata>> &sibling_meta,
      const bool sibling_is_left)
  {
    // create a merged node
    auto merged_node = BaseNode_t::CreateEmptyNode(target_node->GetNodeSize(), true);
    if (sibling_is_left) {
      CopyRecordsViaMetadata(merged_node, sibling_node, sibling_meta.begin(), sibling_meta.end());
      CopyRecordsViaMetadata(merged_node, target_node, target_meta.begin(), target_meta.end());
    } else {
      CopyRecordsViaMetadata(merged_node, target_node, target_meta.begin(), target_meta.end());
      CopyRecordsViaMetadata(merged_node, sibling_node, sibling_meta.begin(), sibling_meta.end());
    }

    return merged_node;
  }

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  static constexpr std::vector<std::pair<Key, Metadata>>
  GatherSortedLiveMetadata(const BaseNode_t *node)
  {
    const auto record_count = node->GetStatusWord().GetRecordCount();
    const int64_t sorted_count = node->GetSortedCount();

    // gather valid (live or deleted) records
    std::vector<std::pair<Key, Metadata>> meta_arr;
    meta_arr.reserve(record_count);

    // search unsorted metadata in reverse order
    for (int64_t index = record_count - 1; index >= sorted_count; --index) {
      const auto meta = node->GetMetadataProtected(index);
      if (meta.IsVisible() || meta.IsDeleted()) {
        const auto key = CastKey<Key>(node->GetKeyAddr(meta));
        meta_arr.emplace_back(key, meta);
      } else {
        // there is a key, but it is in inserting or corrupted.
        // NOTE: we can ignore inserting records because concurrent writes are aborted due to SMOs.
      }
    }

    // search sorted metadata
    for (int64_t index = 0; index < sorted_count; ++index) {
      const auto meta = node->GetMetadataProtected(index);
      const auto key = CastKey<Key>(node->GetKeyAddr(meta));
      meta_arr.emplace_back(key, meta);
    }

    // make unique with keeping the order of writes
    std::stable_sort(meta_arr.begin(), meta_arr.end(), PairComp{});
    auto end_iter = std::unique(meta_arr.begin(), meta_arr.end(), PairEqual{});

    // gather live records
    end_iter = std::remove_if(meta_arr.begin(), end_iter,
                              [](auto &obj) { return obj.second.IsDeleted(); });
    meta_arr.erase(end_iter, meta_arr.end());

    return meta_arr;
  }
};

}  // namespace dbgroup::index::bztree
