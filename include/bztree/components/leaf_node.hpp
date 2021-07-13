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
template <class Key, class Payload, class Compare>
class LeafNode
{
  using BaseNode_t = BaseNode<Key, Payload, Compare>;
  using KeyExistence = typename BaseNode_t::KeyExistence;
  using NodeReturnCode = typename BaseNode_t::NodeReturnCode;

 private:
  /*################################################################################################
   * Internal structs to cpmare key & metadata pairs
   *##############################################################################################*/

  struct MetaRecord {
    Metadata meta = Metadata{};
    Key key = Key{};

    constexpr bool
    operator<(const MetaRecord &obj)
    {
      return Compare{}(this->key, obj.key);
    }

    constexpr bool
    operator==(const MetaRecord &obj)
    {
      return !Compare{}(this->key, obj.key) && !Compare{}(obj.key, this->key);
    }

    constexpr bool
    operator!=(const MetaRecord &obj)
    {
      return Compare{}(this->key, obj.key) || Compare{}(obj.key, this->key);
    }
  };

  /*################################################################################################
   * Internal constants
   *##############################################################################################*/

  /// the maximum number of records in a node
  static constexpr size_t kMaxRecordNum = GetMaxRecordNum<Key, Payload>();

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
      const int64_t rec_count,
      const size_t epoch)
  {
    if constexpr (CanCASUpdate<Payload>()) {
      const auto [rc, index] = node->SearchSortedMetadata(key, true);
      if (rc == KeyExistence::kNotExist || rc == KeyExistence::kDeleted) {
        return SearchUnsortedMetaToWrite(node, key, rec_count - 1, node->GetSortedCount(), epoch);
      } else {
        return {rc, index};
      }
    } else {
      const auto [rc, index] =
          SearchUnsortedMetaToWrite(node, key, rec_count - 1, node->GetSortedCount(), epoch);
      if (rc == KeyExistence::kNotExist) {
        return node->SearchSortedMetadata(key, true);
      }
      return {rc, index};
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
    if constexpr (CanCASUpdate<Payload>()) {
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

  static constexpr size_t
  AlignOffset(const size_t offset)
  {
    if constexpr (CanCASUpdate<Payload>()) {
      if constexpr (std::is_same_v<Key, char *>) {
        const auto align_size = offset & (kWordLength - 1);
        if (align_size > 0) {
          return offset - align_size;
        }
      } else if constexpr (sizeof(Key) % kWordLength != 0) {
        return offset - (kWordLength - (sizeof(Key) % kWordLength));
      }
    }
    return offset;
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

  static constexpr size_t
  CopyRecords(  //
      BaseNode_t *target_node,
      size_t offset,
      size_t current_rec_count,
      const BaseNode_t *original_node,
      const std::array<Metadata, kMaxRecordNum> &metadata,
      const size_t begin_id,
      const size_t end_id)
  {
    for (size_t i = begin_id; i < end_id; ++i, ++current_rec_count) {
      // copy a record
      const auto meta = metadata[i];
      offset = CopyRecord(target_node, offset, original_node, meta);
      // copy metadata
      const auto new_meta = meta.UpdateOffset(offset);
      target_node->SetMetadata(current_rec_count, new_meta);
    }
    return offset;
  }

  static constexpr size_t
  CopyRecord(  //
      BaseNode_t *target_node,
      size_t offset,
      const BaseNode_t *original_node,
      const Metadata meta)
  {
    const auto total_length = meta.GetTotalLength();

    offset = AlignOffset(offset) - total_length;
    memcpy(ShiftAddress(target_node, offset), original_node->GetKeyAddr(meta), total_length);

    return offset;
  }

  static constexpr bool
  HasSpace(  //
      const BaseNode_t *node,
      const StatusWord status,
      const size_t block_size)
  {
    return status.GetRecordCount() - node->GetSortedCount() < kMaxUnsortedRecNum
           && status.GetOccupiedSize() + block_size <= kPageSize - kWordLength
           && status.GetDeletedSize() < kMaxDeletedSpaceSize;
  }

  static constexpr std::pair<std::array<MetaRecord, kMaxUnsortedRecNum>, size_t>
  SortUnsortedRecords(const BaseNode_t *node)
  {
    const auto record_count = node->GetStatusWordProtected().GetRecordCount();
    const int64_t sorted_count = node->GetSortedCount();

    std::array<MetaRecord, kMaxUnsortedRecNum> arr;

    // sort unsorted records by insertion sort
    size_t count = 0;
    for (int64_t index = record_count - 1; index >= sorted_count; --index) {
      const auto meta = node->GetMetadataProtected(index);
      if (!meta.IsInProgress()) {
        if (count == 0) {
          // insert a first record
          arr[0] = MetaRecord{meta, Cast<Key>(node->GetKeyAddr(meta))};
          ++count;
          continue;
        }

        // insert a new record into an appropiate position
        MetaRecord target{meta, Cast<Key>(node->GetKeyAddr(meta))};
        const auto ins_iter = std::lower_bound(arr.begin(), arr.begin() + count, target);
        if (*ins_iter != target) {
          const size_t ins_id = std::distance(arr.begin(), ins_iter);
          if (ins_id < count) {
            // shift upper records
            memmove(&(arr[ins_id + 1]), &(arr[ins_id]), sizeof(MetaRecord) * (count - ins_id));
          }

          // insert a new record
          arr[ins_id] = std::move(target);
          ++count;
        }
      }
    }

    return {std::move(arr), count};
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
    StatusWord cur_status;
    size_t record_count;

    /*----------------------------------------------------------------------------------------------
     * Phase 1: reserve free space to write a record
     *--------------------------------------------------------------------------------------------*/
    while (true) {
      cur_status = node->GetStatusWordProtected();
      if (cur_status.IsFrozen()) return {NodeReturnCode::kFrozen, StatusWord{}};
      if (!HasSpace(node, cur_status, block_size)) return {NodeReturnCode::kNoSpace, StatusWord{}};

      // prepare for MwCAS
      record_count = cur_status.GetRecordCount();
      const auto new_status = cur_status.AddRecordInfo(1, block_size, 0);

      // perform MwCAS to reserve space
      auto desc = MwCASDescriptor{};
      node->SetStatusForMwCAS(desc, cur_status, new_status);
      node->SetMetadataForMwCAS(desc, record_count, Metadata{}, in_progress_meta);
      if (desc.MwCAS()) break;
    }

    /*----------------------------------------------------------------------------------------------
     * Phase 2: write a record and check conflicts
     *--------------------------------------------------------------------------------------------*/

    // insert a record
    auto offset = kPageSize - cur_status.GetBlockSize();
    offset = node->SetPayload(offset, payload, payload_length);
    offset = node->SetKey(offset, key, key_length);

    // prepare record metadata for MwCAS
    const auto inserted_meta = in_progress_meta.SetRecordInfo(offset, key_length, total_length);

    // check conflicts (concurrent SMOs)
    while (true) {
      cur_status = node->GetStatusWordProtected();
      if (cur_status.IsFrozen()) {
        return {NodeReturnCode::kFrozen, StatusWord{}};
      }

      // perform MwCAS to complete a write
      auto desc = MwCASDescriptor{};
      node->SetStatusForMwCAS(desc, cur_status, cur_status);
      node->SetMetadataForMwCAS(desc, record_count, in_progress_meta, inserted_meta);
      if (desc.MwCAS()) break;
    }

    return {NodeReturnCode::kSuccess, cur_status};
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
    StatusWord cur_status;
    size_t record_count;

    // local flags for insertion
    auto uniqueness = KeyExistence::kNotExist;

    /*----------------------------------------------------------------------------------------------
     * Phase 1: reserve free space to insert a record
     *--------------------------------------------------------------------------------------------*/
    while (true) {
      cur_status = node->GetStatusWordProtected();
      if (cur_status.IsFrozen()) return {NodeReturnCode::kFrozen, StatusWord{}};
      if (!HasSpace(node, cur_status, block_size)) return {NodeReturnCode::kNoSpace, StatusWord{}};

      // check uniqueness
      record_count = cur_status.GetRecordCount();
      if (uniqueness != KeyExistence::kUncertain) {
        uniqueness = CheckUniqueness(node, key, record_count, index_epoch).first;
        if (uniqueness == KeyExistence::kExist) {
          return {NodeReturnCode::kKeyExist, cur_status};
        }
      }

      // prepare new status for MwCAS
      const auto new_status = cur_status.AddRecordInfo(1, block_size, 0);

      // perform MwCAS to reserve space
      auto desc = MwCASDescriptor{};
      node->SetStatusForMwCAS(desc, cur_status, new_status);
      node->SetMetadataForMwCAS(desc, record_count, Metadata{}, in_progress_meta);
      if (desc.MwCAS()) break;

      // set retry flag (described in Section 4.2.1 Inserts: Concurrency issues)
      uniqueness = KeyExistence::kUncertain;
    }

    /*----------------------------------------------------------------------------------------------
     * Phase 2: insert a record and check conflicts
     *--------------------------------------------------------------------------------------------*/

    // insert a record
    auto offset = kPageSize - cur_status.GetBlockSize();
    offset = node->SetPayload(offset, payload, payload_length);
    offset = node->SetKey(offset, key, key_length);

    // prepare record metadata for MwCAS
    const auto inserted_meta = in_progress_meta.SetRecordInfo(offset, key_length, total_length);

    while (true) {
      // check concurrent SMOs
      cur_status = node->GetStatusWordProtected();
      if (cur_status.IsFrozen()) {
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
          return {NodeReturnCode::kKeyExist, cur_status};
        } else if (uniqueness == KeyExistence::kUncertain) {
          // retry if there are still uncertain records
          continue;
        }
      }

      // perform MwCAS to complete an insert
      auto desc = MwCASDescriptor{};
      node->SetStatusForMwCAS(desc, cur_status, cur_status);
      node->SetMetadataForMwCAS(desc, record_count, in_progress_meta, inserted_meta);
      if (desc.MwCAS()) break;
    }

    return {NodeReturnCode::kSuccess, cur_status};
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
    StatusWord cur_status;
    size_t record_count, target_index = 0;
    auto uniqueness = KeyExistence::kNotExist;

    /*----------------------------------------------------------------------------------------------
     * Phase 1: reserve free space to insert a record
     *--------------------------------------------------------------------------------------------*/
    while (true) {
      cur_status = node->GetStatusWordProtected();
      if (cur_status.IsFrozen()) return {NodeReturnCode::kFrozen, StatusWord{}};
      if (!HasSpace(node, cur_status, block_size)) return {NodeReturnCode::kNoSpace, StatusWord{}};

      // check whether a node includes a target key
      record_count = cur_status.GetRecordCount();
      if (uniqueness != KeyExistence::kUncertain) {
        std::tie(uniqueness, target_index) = CheckUniqueness(node, key, record_count, index_epoch);
        if (uniqueness == KeyExistence::kNotExist || uniqueness == KeyExistence::kDeleted) {
          return {NodeReturnCode::kKeyNotExist, cur_status};
        }
      }

      // prepare new status for MwCAS
      const auto target_meta = node->GetMetadataProtected(target_index);
      const auto deleted_size = kWordLength + GetAlignedSize(target_meta.GetTotalLength());
      const auto new_status = cur_status.AddRecordInfo(1, block_size, deleted_size);

      // perform MwCAS to reserve space
      auto desc = MwCASDescriptor{};
      node->SetStatusForMwCAS(desc, cur_status, new_status);
      node->SetMetadataForMwCAS(desc, record_count, Metadata{}, in_progress_meta);
      if (desc.MwCAS()) break;
    }

    /*----------------------------------------------------------------------------------------------
     * Phase 2: insert a record and check conflicts
     *--------------------------------------------------------------------------------------------*/

    // insert a record
    auto offset = kPageSize - cur_status.GetBlockSize();
    offset = node->SetPayload(offset, payload, payload_length);
    offset = node->SetKey(offset, key, key_length);

    // prepare record metadata for MwCAS
    const auto inserted_meta = in_progress_meta.SetRecordInfo(offset, key_length, total_length);

    while (true) {
      // check conflicts (concurrent SMOs)
      cur_status = node->GetStatusWordProtected();
      if (cur_status.IsFrozen()) {
        return {NodeReturnCode::kFrozen, StatusWord{}};
      }

      // recheck uniqueness if required
      if (uniqueness == KeyExistence::kUncertain) {
        uniqueness = CheckUniqueness(node, key, record_count, index_epoch).first;
        if (uniqueness == KeyExistence::kNotExist || uniqueness == KeyExistence::kDeleted) {
          // delete an inserted record
          node->SetMetadataByCAS(record_count, in_progress_meta.UpdateOffset(0));
          return {NodeReturnCode::kKeyNotExist, cur_status};
        } else if (uniqueness == KeyExistence::kUncertain) {
          continue;
        }
      }

      // perform MwCAS to complete an update
      auto desc = MwCASDescriptor{};
      node->SetStatusForMwCAS(desc, cur_status, cur_status);
      node->SetMetadataForMwCAS(desc, record_count, in_progress_meta, inserted_meta);
      if (desc.MwCAS()) break;
    }

    return {NodeReturnCode::kSuccess, cur_status};
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
    StatusWord cur_status;
    size_t record_count, target_index = 0;
    auto uniqueness = KeyExistence::kNotExist;

    /*----------------------------------------------------------------------------------------------
     * Phase 1: reserve free space to insert a null record
     *--------------------------------------------------------------------------------------------*/
    while (true) {
      cur_status = node->GetStatusWordProtected();
      if (cur_status.IsFrozen()) return {NodeReturnCode::kFrozen, StatusWord{}};
      if (!HasSpace(node, cur_status, key_length)) return {NodeReturnCode::kNoSpace, StatusWord{}};

      // check whether a node includes a target key
      record_count = cur_status.GetRecordCount();
      if (uniqueness != KeyExistence::kUncertain) {
        std::tie(uniqueness, target_index) = CheckUniqueness(node, key, record_count, index_epoch);
        if (uniqueness == KeyExistence::kNotExist || uniqueness == KeyExistence::kDeleted) {
          return {NodeReturnCode::kKeyNotExist, cur_status};
        }
      }

      // prepare new status for MwCAS
      const auto target_meta = node->GetMetadataProtected(target_index);
      const auto deleted_block_size =
          (kWordLength << 1) + GetAlignedSize(target_meta.GetTotalLength()) + key_length;
      const auto new_status = cur_status.AddRecordInfo(1, key_length, deleted_block_size);

      // perform MwCAS to reserve space
      auto desc = MwCASDescriptor{};
      node->SetStatusForMwCAS(desc, cur_status, new_status);
      node->SetMetadataForMwCAS(desc, record_count, Metadata{}, in_progress_meta);
      if (desc.MwCAS()) break;
    }

    /*----------------------------------------------------------------------------------------------
     * Phase 2: insert a record and check conflicts
     *--------------------------------------------------------------------------------------------*/

    // insert a null record
    auto offset = kPageSize - cur_status.GetBlockSize();
    offset = node->SetKey(offset, key, key_length);

    // prepare record metadata for MwCAS
    const auto deleted_meta = in_progress_meta.SetDeleteInfo(offset, key_length, key_length);

    while (true) {
      // check concurrent SMOs
      cur_status = node->GetStatusWordProtected();
      if (cur_status.IsFrozen()) {
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
          return {NodeReturnCode::kKeyNotExist, cur_status};
        } else if (uniqueness == KeyExistence::kUncertain) {
          // retry if there are still uncertain records
          continue;
        }
      }

      // perform MwCAS to complete an insert
      auto desc = MwCASDescriptor{};
      node->SetStatusForMwCAS(desc, cur_status, cur_status);
      node->SetMetadataForMwCAS(desc, record_count, in_progress_meta, deleted_meta);
      if (desc.MwCAS()) break;
    }

    return {NodeReturnCode::kSuccess, cur_status};
  }

  /*################################################################################################
   * Public structure modification operations
   *##############################################################################################*/

  static constexpr BaseNode_t *
  Consolidate(  //
      const BaseNode_t *orig_node,
      const std::array<Metadata, kMaxRecordNum> &metadata,
      const size_t rec_count)
  {
    // create a new node and copy records
    auto new_node = BaseNode_t::CreateEmptyNode(kLeafFlag);
    const auto offset = CopyRecords(new_node, kPageSize, 0, orig_node, metadata, 0, rec_count);
    new_node->SetSortedCount(rec_count);
    new_node->SetStatus(StatusWord{}.AddRecordInfo(rec_count, kPageSize - AlignOffset(offset), 0));

    return new_node;
  }

  static constexpr std::pair<BaseNode_t *, BaseNode_t *>
  Split(  //
      const BaseNode_t *orig_node,
      const std::array<Metadata, kMaxRecordNum> &metadata,
      const size_t rec_count,
      const size_t left_rec_count)
  {
    const auto right_rec_count = rec_count - left_rec_count;

    // create a split left node
    auto left_node = BaseNode_t::CreateEmptyNode(kLeafFlag);
    auto offset = CopyRecords(left_node, kPageSize, 0, orig_node, metadata, 0, left_rec_count);
    left_node->SetSortedCount(left_rec_count);
    left_node->SetStatus(
        StatusWord{}.AddRecordInfo(left_rec_count, kPageSize - AlignOffset(offset), 0));

    // create a split right node
    auto right_node = BaseNode_t::CreateEmptyNode(kLeafFlag);
    offset = CopyRecords(right_node, kPageSize, 0, orig_node, metadata, left_rec_count, rec_count);
    right_node->SetSortedCount(right_rec_count);
    right_node->SetStatus(
        StatusWord{}.AddRecordInfo(right_rec_count, kPageSize - AlignOffset(offset), 0));

    return {left_node, right_node};
  }

  static constexpr BaseNode_t *
  Merge(  //
      const BaseNode_t *left_node,
      const std::array<Metadata, kMaxRecordNum> &left_meta,
      const size_t l_rec_count,
      const BaseNode_t *right_node,
      const std::array<Metadata, kMaxRecordNum> &right_meta,
      const size_t r_rec_count)
  {
    const auto rec_count = l_rec_count + r_rec_count;

    // create a merged node
    auto new_node = BaseNode_t::CreateEmptyNode(kLeafFlag);
    auto offset = CopyRecords(new_node, kPageSize, 0, left_node, left_meta, 0, l_rec_count);
    offset = CopyRecords(new_node, offset, l_rec_count, right_node, right_meta, 0, r_rec_count);
    new_node->SetSortedCount(rec_count);
    new_node->SetStatus(StatusWord{}.AddRecordInfo(rec_count, kPageSize - AlignOffset(offset), 0));

    return new_node;
  }

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  static constexpr std::pair<std::array<Metadata, kMaxRecordNum>, size_t>
  GatherSortedLiveMetadata(const BaseNode_t *node)
  {
    const auto sorted_count = node->GetSortedCount();

    // sort records in an unsorted region
    auto [new_records, new_rec_num] = SortUnsortedRecords(node);

    // sort all records by merge sort
    std::array<Metadata, kMaxRecordNum> results;
    size_t count = 0, j = 0;
    for (size_t i = 0; i < sorted_count; ++i) {
      const auto meta = node->GetMetadataProtected(i);
      MetaRecord target{meta, Cast<Key>(node->GetKeyAddr(meta))};

      // move lower new records
      while (j < new_rec_num && new_records[j] < target) {
        if (new_records[j].meta.IsVisible()) {
          results[count++] = new_records[j].meta;
        }
        ++j;
      }

      // insert a target record
      if (j < new_rec_num && new_records[j] == target) {
        if (new_records[j].meta.IsVisible()) {
          results[count++] = new_records[j].meta;
        }
        ++j;
      } else {
        if (target.meta.IsVisible()) {
          results[count++] = meta;
        }
      }
    }

    // move remaining new records
    while (j < new_rec_num) {
      if (new_records[j].meta.IsVisible()) {
        results[count++] = new_records[j].meta;
      }
      ++j;
    }

    return {std::move(results), count};
  }
};

}  // namespace dbgroup::index::bztree
