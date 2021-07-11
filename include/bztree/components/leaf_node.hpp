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

  static constexpr std::pair<KeyExistence, size_t>
  SearchUnsortedMetaToWrite(  //
      const BaseNode_t *node,
      const Key key,
      const int64_t begin_index,
      const int64_t sorted_count,
      const size_t index_epoch)
  {
    // perform a linear search in revese order
    for (int64_t index = begin_index; index >= sorted_count; --index) {
      const auto meta = node->GetMetadataProtected(index);
      if (meta.IsInProgress()) {
        if (meta.GetOffset() == index_epoch) {
          return {KeyExistence::kUncertain, index};
        }
        continue;  // failed record
      }

      const auto target_key = Cast<Key>(node->GetKeyAddr(meta));
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
  CheckUniqueness(  //
      const BaseNode_t *node,
      const Key key,
      const int64_t record_count,
      const size_t index_epoch)
  {
    const auto [existence, index] =
        SearchUnsortedMetaToWrite(node, key, record_count - 1, node->GetSortedCount(), index_epoch);
    if (existence == KeyExistence::kNotExist) {
      // there is no key in unsorted metadata, so search a sorted region
      return node->SearchSortedMetadata(key, true);
    } else {
      return {existence, index};
    }
  }

  static constexpr std::pair<KeyExistence, size_t>
  SearchUnsortedMetaToRead(  //
      const BaseNode_t *node,
      const Key key,
      const int64_t end_index,
      const int64_t record_count)
  {
    for (int64_t index = record_count - 1; index >= end_index; --index) {
      const auto meta = node->GetMetadataProtected(index);
      if (meta.IsInProgress()) {
        continue;
      }

      const auto target_key = Cast<Key>(node->GetKeyAddr(meta));
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
      const Key key,
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

  static constexpr size_t
  GetAlignedSize(const size_t block_size)
  {
    if constexpr (!std::is_same_v<Payload, char *> && sizeof(Payload) == kWordLength) {
      if constexpr (std::is_same_v<Key, char *>) {
        const auto align_size = block_size & (kWordLength - 1);
        if (align_size > 0) {
          return block_size + (kWordLength - align_size);
        }
      } else if constexpr (sizeof(Key) % kWordLength != 0) {
        constexpr auto kAlignedSize = sizeof(Key) - (sizeof(Key) % kWordLength);
        return block_size + kAlignedSize;
      }
    }
    return block_size;
  }

  static constexpr auto
  GetPayload(  //
      const BaseNode_t *node,
      const Metadata meta)
  {
    if constexpr (std::is_same_v<Payload, char *>) {
      const auto payload_length = meta.GetPayloadLength();
      auto payload = malloc(payload_length);
      memcpy(payload, node->GetPayloadAddr(meta), payload_length);
      return std::unique_ptr<char>(static_cast<char *>(payload));
    } else {
      return Cast<Payload>(node->GetPayloadAddr(meta));
    }
  }

  static constexpr auto
  GetRecord(  //
      const BaseNode_t *node,
      const Metadata meta)
  {
    const auto record_addr = node->GetKeyAddr(meta);
    if constexpr (std::is_same_v<Key, char *> && std::is_same_v<Payload, char *>) {
      const auto key_length = meta.GetKeyLength();
      const auto payload_length = meta.GetPayloadLength();
      return VarRecord::Create(record_addr, key_length, payload_length);
    } else if constexpr (std::is_same_v<Key, char *>) {
      const auto key_length = meta.GetKeyLength();
      return VarKeyRecord<Payload>::Create(record_addr, key_length);
    } else if constexpr (std::is_same_v<Payload, char *>) {
      const auto payload_length = meta.GetPayloadLength();
      return VarPayloadRecord<Key>::Create(record_addr, payload_length);
    } else {
      return Record<Key, Payload>{record_addr};
    }
  }

  static constexpr auto
  CreateScanResults(  //
      const BaseNode_t *node,
      const std::vector<std::pair<Key, Metadata>> &meta_arr)
  {
    if constexpr (std::is_same_v<Key, char *> && std::is_same_v<Payload, char *>) {
      std::vector<std::unique_ptr<VarRecord>> scan_results;
      scan_results.reserve(meta_arr.size());
      for (auto &&[key, meta] : meta_arr) {
        if (meta.IsVisible()) {
          scan_results.emplace_back(GetRecord(node, meta));
        }
      }
      return scan_results;
    } else if constexpr (std::is_same_v<Key, char *>) {
      std::vector<std::unique_ptr<VarKeyRecord<Payload>>> scan_results;
      scan_results.reserve(meta_arr.size());
      for (auto &&[key, meta] : meta_arr) {
        if (meta.IsVisible()) {
          scan_results.emplace_back(GetRecord(node, meta));
        }
      }
      return scan_results;
    } else if constexpr (std::is_same_v<Payload, char *>) {
      std::vector<std::unique_ptr<VarPayloadRecord<Key>>> scan_results;
      scan_results.reserve(meta_arr.size());
      for (auto &&[key, meta] : meta_arr) {
        if (meta.IsVisible()) {
          scan_results.emplace_back(GetRecord(node, meta));
        }
      }
      return scan_results;
    } else {
      std::vector<Record<Key, Payload>> scan_results;
      scan_results.reserve(meta_arr.size());
      for (auto &&[key, meta] : meta_arr) {
        if (meta.IsVisible()) {
          scan_results.emplace_back(GetRecord(node, meta));
        }
      }
      return scan_results;
    }
  }

  static constexpr void
  CopyRecordsViaMetadata(  //
      BaseNode_t *copied_node,
      const BaseNode_t *original_node,
      const typename std::vector<std::pair<Key, Metadata>>::const_iterator begin_iter,
      const typename std::vector<std::pair<Key, Metadata>>::const_iterator end_iter)
  {
    auto record_count = copied_node->GetSortedCount();
    auto offset = kPageSize - copied_node->GetStatusWord().GetBlockSize();
    for (auto iter = begin_iter; iter != end_iter; ++record_count, ++iter) {
      // copy a record
      const auto [key, meta] = *iter;
      offset = CopyRecord(copied_node, offset, original_node, meta);
      // copy metadata
      const auto new_meta = meta.UpdateOffset(offset);
      copied_node->SetMetadata(record_count, new_meta);
    }
    const auto aligned_block_size = GetAlignedSize(kPageSize - offset);
    copied_node->SetStatus(StatusWord{}.AddRecordInfo(record_count, aligned_block_size, 0));
    copied_node->SetSortedCount(record_count);
  }

  static constexpr size_t
  CopyRecord(  //
      BaseNode_t *copied_node,
      size_t offset,
      const BaseNode_t *original_node,
      const Metadata meta)
  {
    if constexpr (!std::is_same_v<Payload, char *> && sizeof(Payload) == kWordLength) {
      // align memory address
      if constexpr (std::is_same_v<Key, char *>) {
        offset -= (offset & (kWordLength - 1));
      } else if constexpr (sizeof(Key) % kWordLength != 0) {
        constexpr auto kAlignedSize = sizeof(Key) - (sizeof(Key) % kWordLength);
        offset -= kAlignedSize;
      }
    }

    const auto total_length = meta.GetTotalLength();

    offset -= total_length;
    const auto dest = ShiftAddress(copied_node, offset);
    const auto src = original_node->GetKeyAddr(meta);
    memcpy(dest, src, total_length);

    return offset;
  }

  static constexpr bool
  HasSpace(  //
      const StatusWord status,
      const size_t block_size)
  {
    return status.GetOccupiedSize() + block_size <= kPageSize - kWordLength;
  }

 public:
  /*################################################################################################
   * Read operations
   *##############################################################################################*/

  static constexpr auto
  Read(  //
      const BaseNode_t *node,
      const Key key)
  {
    const auto status = node->GetStatusWordProtected();
    const auto [existence, index] = SearchMetadataToRead(node, key, status.GetRecordCount());
    if (existence == KeyExistence::kNotExist || existence == KeyExistence::kDeleted) {
      if constexpr (std::is_same_v<Payload, char *>) {
        return std::make_pair(NodeReturnCode::kKeyNotExist,
                              static_cast<std::unique_ptr<char>>(nullptr));
      } else {
        return std::make_pair(NodeReturnCode::kKeyNotExist, Payload{});
      }
    }
    const auto meta = node->GetMetadataProtected(index);
    return std::make_pair(NodeReturnCode::kSuccess, GetPayload(node, meta));
  }

  static constexpr auto
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
      const auto key = Cast<Key>(node->GetKeyAddr(meta));
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
      const auto key = Cast<Key>(node->GetKeyAddr(meta));
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

    return std::make_pair(NodeReturnCode::kSuccess, CreateScanResults(node, meta_arr));
  }

  /*################################################################################################
   * Write operations
   *##############################################################################################*/

  static constexpr std::pair<NodeReturnCode, StatusWord>
  Write(  //
      BaseNode_t *node,
      const Key key,
      const size_t key_length,
      const Payload payload,
      const size_t payload_length,
      const size_t index_epoch = 1)
  {
    // variables and constants shared in Phase 1 & 2
    const auto total_length = key_length + payload_length;
    const auto block_size = GetAlignedSize(total_length);
    const auto in_progress_meta = Metadata::GetInsertingMeta(index_epoch);
    StatusWord current_status;
    size_t record_count;

    /*----------------------------------------------------------------------------------------------
     * Phase 1: reserve free space to write a record
     *--------------------------------------------------------------------------------------------*/
    while (true) {
      current_status = node->GetStatusWordProtected();
      if (current_status.IsFrozen()) return {NodeReturnCode::kFrozen, StatusWord{}};
      if (!HasSpace(current_status, block_size)) return {NodeReturnCode::kNoSpace, StatusWord{}};

      // prepare for MwCAS
      record_count = current_status.GetRecordCount();
      const auto new_status = current_status.AddRecordInfo(1, block_size, 0);

      // perform MwCAS to reserve space
      auto desc = MwCASDescriptor{};
      node->SetStatusForMwCAS(desc, current_status, new_status);
      node->SetMetadataForMwCAS(desc, record_count, Metadata{}, in_progress_meta);
      if (desc.MwCAS()) break;
    }

    /*----------------------------------------------------------------------------------------------
     * Phase 2: write a record and check conflicts
     *--------------------------------------------------------------------------------------------*/

    // insert a record
    auto offset = kPageSize - current_status.GetBlockSize();
    offset = node->SetPayload(offset, payload, payload_length);
    offset = node->SetKey(offset, key, key_length);

    // prepare record metadata for MwCAS
    const auto inserted_meta = in_progress_meta.SetRecordInfo(offset, key_length, total_length);

    // check conflicts (concurrent SMOs)
    while (true) {
      current_status = node->GetStatusWordProtected();
      if (current_status.IsFrozen()) {
        return {NodeReturnCode::kFrozen, StatusWord{}};
      }

      // perform MwCAS to complete a write
      auto desc = MwCASDescriptor{};
      node->SetStatusForMwCAS(desc, current_status, current_status);
      node->SetMetadataForMwCAS(desc, record_count, in_progress_meta, inserted_meta);
      if (desc.MwCAS()) break;
    }

    return {NodeReturnCode::kSuccess, current_status};
  }

  static constexpr std::pair<NodeReturnCode, StatusWord>
  Insert(  //
      BaseNode_t *node,
      const Key key,
      const size_t key_length,
      const Payload payload,
      const size_t payload_length,
      const size_t index_epoch = 1)
  {
    // variables and constants shared in Phase 1 & 2
    const auto total_length = key_length + payload_length;
    const auto block_size = GetAlignedSize(total_length);
    const auto in_progress_meta = Metadata::GetInsertingMeta(index_epoch);
    StatusWord current_status;
    size_t record_count;

    // local flags for insertion
    auto uniqueness = KeyExistence::kNotExist;

    /*----------------------------------------------------------------------------------------------
     * Phase 1: reserve free space to insert a record
     *--------------------------------------------------------------------------------------------*/
    while (true) {
      current_status = node->GetStatusWordProtected();
      if (current_status.IsFrozen()) return {NodeReturnCode::kFrozen, StatusWord{}};
      if (!HasSpace(current_status, block_size)) return {NodeReturnCode::kNoSpace, StatusWord{}};

      // check uniqueness
      record_count = current_status.GetRecordCount();
      if (uniqueness != KeyExistence::kUncertain) {
        uniqueness = CheckUniqueness(node, key, record_count, index_epoch).first;
        if (uniqueness == KeyExistence::kExist) {
          return {NodeReturnCode::kKeyExist, current_status};
        }
      }

      // prepare new status for MwCAS
      const auto new_status = current_status.AddRecordInfo(1, block_size, 0);

      // perform MwCAS to reserve space
      auto desc = MwCASDescriptor{};
      node->SetStatusForMwCAS(desc, current_status, new_status);
      node->SetMetadataForMwCAS(desc, record_count, Metadata{}, in_progress_meta);
      if (desc.MwCAS()) break;

      // set retry flag (described in Section 4.2.1 Inserts: Concurrency issues)
      uniqueness = KeyExistence::kUncertain;
    }

    /*----------------------------------------------------------------------------------------------
     * Phase 2: insert a record and check conflicts
     *--------------------------------------------------------------------------------------------*/

    // insert a record
    auto offset = kPageSize - current_status.GetBlockSize();
    offset = node->SetPayload(offset, payload, payload_length);
    offset = node->SetKey(offset, key, key_length);

    // prepare record metadata for MwCAS
    const auto inserted_meta = in_progress_meta.SetRecordInfo(offset, key_length, total_length);

    while (true) {
      // check concurrent SMOs
      current_status = node->GetStatusWordProtected();
      if (current_status.IsFrozen()) {
        // delete an inserted record
        node->SetMetadataByCAS(record_count, in_progress_meta.UpdateOffset(0));
        return {NodeReturnCode::kFrozen, StatusWord{}};
      }

      // recheck uniqueness if required
      if (uniqueness == KeyExistence::kUncertain) {
        uniqueness = CheckUniqueness(node, key, record_count, index_epoch).first;
        if (uniqueness == KeyExistence::kExist) {
          // delete an inserted record
          node->SetMetadataByCAS(record_count, in_progress_meta.UpdateOffset(0));
          return {NodeReturnCode::kKeyExist, current_status};
        } else if (uniqueness == KeyExistence::kUncertain) {
          // retry if there are still uncertain records
          continue;
        }
      }

      // perform MwCAS to complete an insert
      auto desc = MwCASDescriptor{};
      node->SetStatusForMwCAS(desc, current_status, current_status);
      node->SetMetadataForMwCAS(desc, record_count, in_progress_meta, inserted_meta);
      if (desc.MwCAS()) break;
    }

    return {NodeReturnCode::kSuccess, current_status};
  }

  static constexpr std::pair<NodeReturnCode, StatusWord>
  Update(  //
      BaseNode_t *node,
      const Key key,
      const size_t key_length,
      const Payload payload,
      const size_t payload_length,
      const size_t index_epoch = 1)
  {
    // variables and constants shared in Phase 1 & 2
    const auto total_length = key_length + payload_length;
    const auto block_size = GetAlignedSize(total_length);
    const auto in_progress_meta = Metadata::GetInsertingMeta(index_epoch);
    StatusWord current_status;
    size_t record_count, target_index = 0;
    auto uniqueness = KeyExistence::kNotExist;

    /*----------------------------------------------------------------------------------------------
     * Phase 1: reserve free space to insert a record
     *--------------------------------------------------------------------------------------------*/
    while (true) {
      current_status = node->GetStatusWordProtected();
      if (current_status.IsFrozen()) return {NodeReturnCode::kFrozen, StatusWord{}};
      if (!HasSpace(current_status, block_size)) return {NodeReturnCode::kNoSpace, StatusWord{}};

      // check whether a node includes a target key
      record_count = current_status.GetRecordCount();
      if (uniqueness != KeyExistence::kUncertain) {
        std::tie(uniqueness, target_index) = CheckUniqueness(node, key, record_count, index_epoch);
        if (uniqueness == KeyExistence::kNotExist || uniqueness == KeyExistence::kDeleted) {
          return {NodeReturnCode::kKeyNotExist, current_status};
        }
      }

      // prepare new status for MwCAS
      const auto target_meta = node->GetMetadataProtected(target_index);
      const auto deleted_size = kWordLength + GetAlignedSize(target_meta.GetTotalLength());
      const auto new_status = current_status.AddRecordInfo(1, block_size, deleted_size);

      // perform MwCAS to reserve space
      auto desc = MwCASDescriptor{};
      node->SetStatusForMwCAS(desc, current_status, new_status);
      node->SetMetadataForMwCAS(desc, record_count, Metadata{}, in_progress_meta);
      if (desc.MwCAS()) break;
    }

    /*----------------------------------------------------------------------------------------------
     * Phase 2: insert a record and check conflicts
     *--------------------------------------------------------------------------------------------*/

    // insert a record
    auto offset = kPageSize - current_status.GetBlockSize();
    offset = node->SetPayload(offset, payload, payload_length);
    offset = node->SetKey(offset, key, key_length);

    // prepare record metadata for MwCAS
    const auto inserted_meta = in_progress_meta.SetRecordInfo(offset, key_length, total_length);

    while (true) {
      // check conflicts (concurrent SMOs)
      current_status = node->GetStatusWordProtected();
      if (current_status.IsFrozen()) {
        return {NodeReturnCode::kFrozen, StatusWord{}};
      }

      // recheck uniqueness if required
      if (uniqueness == KeyExistence::kUncertain) {
        uniqueness = CheckUniqueness(node, key, record_count, index_epoch).first;
        if (uniqueness == KeyExistence::kNotExist || uniqueness == KeyExistence::kDeleted) {
          // delete an inserted record
          node->SetMetadataByCAS(record_count, in_progress_meta.UpdateOffset(0));
          return {NodeReturnCode::kKeyNotExist, current_status};
        } else if (uniqueness == KeyExistence::kUncertain) {
          continue;
        }
      }

      // perform MwCAS to complete an update
      auto desc = MwCASDescriptor{};
      node->SetStatusForMwCAS(desc, current_status, current_status);
      node->SetMetadataForMwCAS(desc, record_count, in_progress_meta, inserted_meta);
      if (desc.MwCAS()) break;
    }

    return {NodeReturnCode::kSuccess, current_status};
  }

  static constexpr std::pair<NodeReturnCode, StatusWord>
  Delete(  //
      BaseNode_t *node,
      const Key key,
      const size_t key_length,
      const size_t index_epoch = 1)
  {
    // variables and constants
    const auto in_progress_meta = Metadata::GetInsertingMeta(index_epoch);
    StatusWord current_status;
    size_t record_count, target_index = 0;
    auto uniqueness = KeyExistence::kNotExist;

    /*----------------------------------------------------------------------------------------------
     * Phase 1: reserve free space to insert a null record
     *--------------------------------------------------------------------------------------------*/
    while (true) {
      current_status = node->GetStatusWordProtected();
      if (current_status.IsFrozen()) return {NodeReturnCode::kFrozen, StatusWord{}};
      if (!HasSpace(current_status, key_length)) return {NodeReturnCode::kNoSpace, StatusWord{}};

      // check whether a node includes a target key
      record_count = current_status.GetRecordCount();
      if (uniqueness != KeyExistence::kUncertain) {
        std::tie(uniqueness, target_index) = CheckUniqueness(node, key, record_count, index_epoch);
        if (uniqueness == KeyExistence::kNotExist || uniqueness == KeyExistence::kDeleted) {
          return {NodeReturnCode::kKeyNotExist, current_status};
        }
      }

      // prepare new status for MwCAS
      const auto target_meta = node->GetMetadataProtected(target_index);
      const auto deleted_block_size =
          (kWordLength << 1) + GetAlignedSize(target_meta.GetTotalLength()) + key_length;
      const auto new_status = current_status.AddRecordInfo(1, key_length, deleted_block_size);

      // perform MwCAS to reserve space
      auto desc = MwCASDescriptor{};
      node->SetStatusForMwCAS(desc, current_status, new_status);
      node->SetMetadataForMwCAS(desc, record_count, Metadata{}, in_progress_meta);
      if (desc.MwCAS()) break;
    }

    /*----------------------------------------------------------------------------------------------
     * Phase 2: insert a record and check conflicts
     *--------------------------------------------------------------------------------------------*/

    // insert a null record
    auto offset = kPageSize - current_status.GetBlockSize();
    offset = node->SetKey(offset, key, key_length);

    // prepare record metadata for MwCAS
    const auto deleted_meta = in_progress_meta.SetDeleteInfo(offset, key_length, key_length);

    while (true) {
      // check concurrent SMOs
      current_status = node->GetStatusWordProtected();
      if (current_status.IsFrozen()) {
        // delete an inserted record
        node->SetMetadataByCAS(record_count, in_progress_meta.UpdateOffset(0));
        return {NodeReturnCode::kFrozen, StatusWord{}};
      }

      // recheck uniqueness if required
      if (uniqueness == KeyExistence::kUncertain) {
        uniqueness = CheckUniqueness(node, key, record_count, index_epoch).first;
        if (uniqueness == KeyExistence::kNotExist || uniqueness == KeyExistence::kDeleted) {
          // delete an inserted record
          node->SetMetadataByCAS(record_count, in_progress_meta.UpdateOffset(0));
          return {NodeReturnCode::kKeyNotExist, current_status};
        } else if (uniqueness == KeyExistence::kUncertain) {
          // retry if there are still uncertain records
          continue;
        }
      }

      // perform MwCAS to complete an insert
      auto desc = MwCASDescriptor{};
      node->SetStatusForMwCAS(desc, current_status, current_status);
      node->SetMetadataForMwCAS(desc, record_count, in_progress_meta, deleted_meta);
      if (desc.MwCAS()) break;
    }

    return {NodeReturnCode::kSuccess, current_status};
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
    auto new_node = BaseNode_t::CreateEmptyNode(kLeafFlag);
    CopyRecordsViaMetadata(new_node, node, live_meta.begin(), live_meta.end());

    return new_node;
  }

  static constexpr std::pair<BaseNode_t *, BaseNode_t *>
  Split(  //
      const BaseNode_t *node,
      const std::vector<std::pair<Key, Metadata>> &sorted_meta,
      const size_t left_record_count)
  {
    const auto split_iter = sorted_meta.begin() + left_record_count;

    // create a split left node
    auto left_node = BaseNode_t::CreateEmptyNode(kLeafFlag);
    CopyRecordsViaMetadata(left_node, node, sorted_meta.begin(), split_iter);

    // create a split right node
    auto right_node = BaseNode_t::CreateEmptyNode(kLeafFlag);
    CopyRecordsViaMetadata(right_node, node, split_iter, sorted_meta.end());

    return {left_node, right_node};
  }

  static constexpr BaseNode_t *
  Merge(  //
      const BaseNode_t *left_node,
      const std::vector<std::pair<Key, Metadata>> &left_meta,
      const BaseNode_t *right_node,
      const std::vector<std::pair<Key, Metadata>> &right_meta)
  {
    // create a merged node
    auto merged_node = BaseNode_t::CreateEmptyNode(kLeafFlag);
    CopyRecordsViaMetadata(merged_node, left_node, left_meta.begin(), left_meta.end());
    CopyRecordsViaMetadata(merged_node, right_node, right_meta.begin(), right_meta.end());

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
        const auto key = Cast<Key>(node->GetKeyAddr(meta));
        meta_arr.emplace_back(key, meta);
      } else {
        // there is a key, but it is in inserting or corrupted.
        // NOTE: we can ignore inserting records because concurrent writes are aborted due to SMOs.
      }
    }

    // search sorted metadata
    for (int64_t index = 0; index < sorted_count; ++index) {
      const auto meta = node->GetMetadataProtected(index);
      const auto key = Cast<Key>(node->GetKeyAddr(meta));
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
