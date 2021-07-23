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

#include <array>
#include <functional>
#include <memory>
#include <utility>

#include "base_node.hpp"
#include "record_page.hpp"

namespace dbgroup::index::bztree
{
template <class Key, class Payload, class Compare>
class LeafNode
{
  using BaseNode_t = BaseNode<Key, Payload, Compare>;
  using KeyExistence = typename BaseNode_t::KeyExistence;
  using NodeReturnCode = typename BaseNode_t::NodeReturnCode;
  using RecordPage_t = RecordPage<Key, Payload>;

 private:
  /*################################################################################################
   * Internal structs to cpmare key & metadata pairs
   *##############################################################################################*/

  struct MetaRecord {
    Metadata meta = Metadata{};
    Key key = Key{};

    constexpr bool
    operator<(const MetaRecord &obj) const
    {
      return Compare{}(this->key, obj.key);
    }

    constexpr bool
    operator==(const MetaRecord &obj) const
    {
      return !Compare{}(this->key, obj.key) && !Compare{}(obj.key, this->key);
    }

    constexpr bool
    operator!=(const MetaRecord &obj) const
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
  SearchUnsortedMeta(  //
      const BaseNode_t *node,
      const Key key,
      const int64_t begin_index,
      const int64_t sorted_count,
      const size_t index_epoch = 0)
  {
    // perform a linear search in revese order
    for (int64_t index = begin_index; index >= sorted_count; --index) {
      const auto meta = node->GetMetadataProtected(index);
      if (meta.IsInProgress()) {
        if (index_epoch > 0 && meta.GetOffset() == index_epoch) {
          return {KeyExistence::kUncertain, index};
        }
        continue;
      }

      const auto target_key = node->GetKey(meta);
      if (IsEqual<Compare>(key, target_key)) {
        if (meta.IsVisible()) return {KeyExistence::kExist, index};
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
      const size_t epoch = 0)
  {
    if constexpr (CanCASUpdate<Payload>()) {
      const auto [rc, index] = node->SearchSortedMetadata(key, true);
      if (rc == KeyExistence::kNotExist || rc == KeyExistence::kDeleted) {
        // a new record may be inserted in an unsorted region
        return SearchUnsortedMeta(node, key, rec_count - 1, node->GetSortedCount(), epoch);
      }
      return {rc, index};
    } else {
      const auto [rc, index] =
          SearchUnsortedMeta(node, key, rec_count - 1, node->GetSortedCount(), epoch);
      if (rc == KeyExistence::kNotExist) {
        // a record may be in a sorted region
        return node->SearchSortedMetadata(key, true);
      }
      return {rc, index};
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

  static constexpr void
  SortUnsortedRecords(  //
      const BaseNode_t *node,
      std::array<MetaRecord, kMaxUnsortedRecNum> &arr,
      size_t &count)
  {
    const auto rec_count = node->GetStatusWordProtected().GetRecordCount();
    const int64_t sorted_count = node->GetSortedCount();

    // sort unsorted records by insertion sort
    for (int64_t index = rec_count - 1; index >= sorted_count; --index) {
      const auto meta = node->GetMetadataProtected(index);
      if (!meta.IsInProgress()) {
        if (count == 0) {
          // insert a first record
          arr[0] = MetaRecord{meta, node->GetKey(meta)};
          ++count;
          continue;
        }

        // insert a new record into an appropiate position
        MetaRecord target{meta, node->GetKey(meta)};
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
  }

  static constexpr void
  SortUnsortedRecords(  //
      const BaseNode_t *node,
      const Key *begin_key,
      const bool begin_closed,
      const Key *end_key,
      const bool end_closed,
      std::array<MetaRecord, kMaxUnsortedRecNum> &arr,
      size_t &count)
  {
    const auto rec_count = node->GetStatusWordProtected().GetRecordCount();
    const int64_t sorted_count = node->GetSortedCount();

    // sort unsorted records by insertion sort
    for (int64_t index = rec_count - 1; index >= sorted_count; --index) {
      const auto meta = node->GetMetadataProtected(index);
      if (!meta.IsInProgress()) {
        auto key = node->GetKey(meta);
        if (!IsInRange<Compare>(key, begin_key, begin_closed, end_key, end_closed)) continue;

        if (count == 0) {
          // insert a first record
          arr[0] = MetaRecord{meta, std::move(key)};
          ++count;
          continue;
        }

        // insert a new record into an appropiate position
        MetaRecord target{meta, node->GetKey(meta)};
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
  }

  static constexpr void
  MergeSortedRecords(  //
      const BaseNode_t *node,
      const std::array<MetaRecord, kMaxUnsortedRecNum> &new_records,
      const size_t new_rec_num,
      std::array<Metadata, kMaxRecordNum> &arr,
      size_t &count)
  {
    const auto sorted_count = node->GetSortedCount();

    size_t j = 0;
    for (size_t i = 0; i < sorted_count; ++i) {
      const auto meta = node->GetMetadataProtected(i);
      MetaRecord target{meta, node->GetKey(meta)};

      // move lower new records
      for (; j < new_rec_num && new_records[j] < target; ++j) {
        if (new_records[j].meta.IsVisible()) {
          arr[count++] = new_records[j].meta;
        }
      }

      // insert a target record
      if (j < new_rec_num && new_records[j] == target) {
        if (new_records[j].meta.IsVisible()) {
          arr[count++] = new_records[j].meta;
        }
        ++j;
      } else {
        if (target.meta.IsVisible()) {
          arr[count++] = meta;
        }
      }
    }

    // move remaining new records
    for (; j < new_rec_num; ++j) {
      if (new_records[j].meta.IsVisible()) {
        arr[count++] = new_records[j].meta;
      }
    }
  }

  static constexpr void
  MergeSortedRecords(  //
      const BaseNode_t *node,
      const std::array<MetaRecord, kMaxUnsortedRecNum> &new_records,
      const size_t new_rec_num,
      const Key *begin_k,
      const bool begin_closed,
      const Key *end_k,
      const bool end_closed,
      std::array<Metadata, kMaxRecordNum> &arr,
      size_t &count)
  {
    const auto sorted_count = node->GetSortedCount();

    size_t j = 0;
    const auto begin_index =
        (begin_k == nullptr) ? 0 : node->SearchSortedMetadata(*begin_k, begin_closed).second;
    for (size_t i = begin_index; i < sorted_count; ++i) {
      const auto meta = node->GetMetadataProtected(i);
      auto key = node->GetKey(meta);
      if (end_k != nullptr && (Compare{}(*end_k, key) || (end_closed && !Compare{}(key, *end_k)))) {
        break;
      }

      // move lower new records
      MetaRecord target{meta, std::move(key)};
      for (; j < new_rec_num && new_records[j] < target; ++j) {
        if (new_records[j].meta.IsVisible()) {
          arr[count++] = new_records[j].meta;
        }
      }

      // insert a target record
      if (j < new_rec_num && new_records[j] == target) {
        if (new_records[j].meta.IsVisible()) {
          arr[count++] = new_records[j].meta;
        }
        ++j;
      } else {
        if (target.meta.IsVisible()) {
          arr[count++] = meta;
        }
      }
    }

    // move remaining new records
    for (; j < new_rec_num; ++j) {
      if (new_records[j].meta.IsVisible()) {
        arr[count++] = new_records[j].meta;
      }
    }
  }

 public:
  /*################################################################################################
   * Read operations
   *##############################################################################################*/

  static constexpr NodeReturnCode
  Read(  //
      const BaseNode_t *node,
      const Key key,
      Payload &out_payload)
  {
    const auto status = node->GetStatusWordProtected();
    const auto [existence, index] = CheckUniqueness(node, key, status.GetRecordCount());
    if (existence == KeyExistence::kNotExist || existence == KeyExistence::kDeleted) {
      return NodeReturnCode::kKeyNotExist;
    }

    const auto meta = node->GetMetadataProtected(index);
    node->CopyPayload(meta, out_payload);
    return NodeReturnCode::kSuccess;
  }

  static constexpr void
  Scan(  //
      const BaseNode_t *node,
      const Key *begin_k,
      const bool begin_closed,
      const Key *end_k,
      const bool end_closed,
      RecordPage_t &page)
  {
    // sort records in an unsorted region
    std::array<MetaRecord, kMaxUnsortedRecNum> new_records;
    size_t new_rec_num = 0;
    SortUnsortedRecords(node, begin_k, begin_closed, end_k, end_closed, new_records, new_rec_num);

    // sort all records by merge sort
    std::array<Metadata, kMaxRecordNum> metadata;
    size_t count = 0;
    MergeSortedRecords(node, new_records, new_rec_num, begin_k, begin_closed, end_k, end_closed,
                       metadata, count);

    // copy scan results to a page for returning
    std::byte *cur_addr = reinterpret_cast<std::byte *>(&page) + kHeaderLength;
    for (size_t i = 0; i < count; ++i) {
      const Metadata meta = metadata[i];
      if constexpr (std::is_same_v<Key, char *>) {
        *(reinterpret_cast<uint32_t *>(cur_addr)) = meta.GetKeyLength();
        cur_addr += sizeof(uint32_t);
      }
      if constexpr (std::is_same_v<Payload, char *>) {
        *(reinterpret_cast<uint32_t *>(cur_addr)) = meta.GetPayloadLength();
        cur_addr += sizeof(uint32_t);
      }
      if constexpr (CanCASUpdate<Payload>()) {
        if constexpr (std::is_same_v<Key, char *>) {
          memcpy(cur_addr, node->GetKeyAddr(meta), meta.GetKeyLength());
          cur_addr += meta.GetKeyLength();
        } else {
          memcpy(cur_addr, node->GetKeyAddr(meta), sizeof(Key));
          cur_addr += sizeof(Key);
        }
        *(reinterpret_cast<Payload *>(cur_addr)) =
            ReadMwCASField<Payload>(node->GetPayloadAddr(meta));
        cur_addr += kWordLength;
      } else {
        memcpy(cur_addr, node->GetKeyAddr(meta), meta.GetTotalLength());
        cur_addr += meta.GetTotalLength();
      }
    }
    page.SetEndAddress(cur_addr);
    if (count > 0) {
      page.SetLastKeyAddress(cur_addr - metadata[count - 1].GetTotalLength());
    }
  }

  /*################################################################################################
   * Write operations
   *##############################################################################################*/

  static constexpr NodeReturnCode
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
    size_t rec_count;

    /*----------------------------------------------------------------------------------------------
     * Phase 1: reserve free space to write a record
     *--------------------------------------------------------------------------------------------*/
    while (true) {
      cur_status = node->GetStatusWordProtected();
      if (cur_status.IsFrozen()) return NodeReturnCode::kFrozen;
      if (!HasSpace(node, cur_status, block_size)) return NodeReturnCode::kNoSpace;

      rec_count = cur_status.GetRecordCount();
      if constexpr (CanCASUpdate<Payload>()) {
        // check whether a node includes a target key
        const auto [uniqueness, target_index] = node->SearchSortedMetadata(key, true);
        if (uniqueness == KeyExistence::kExist && target_index < node->GetSortedCount()) {
          const auto target_meta = node->GetMetadataProtected(target_index);

          // update a record directly
          auto desc = MwCASDescriptor{};
          node->SetStatusForMwCAS(desc, cur_status, cur_status);
          node->SetMetadataForMwCAS(desc, target_index, target_meta, target_meta);
          node->SetPayloadForMwCAS(desc, target_meta, payload);
          if (desc.MwCAS()) return NodeReturnCode::kSuccess;
          continue;
        }
      }

      // prepare new status for MwCAS
      const auto new_status = cur_status.AddRecordInfo(1, block_size, 0);

      // perform MwCAS to reserve space
      auto desc = MwCASDescriptor{};
      node->SetStatusForMwCAS(desc, cur_status, new_status);
      node->SetMetadataForMwCAS(desc, rec_count, Metadata{}, in_progress_meta);
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
      const auto status = node->GetStatusWordProtected();
      if (status.IsFrozen()) {
        return NodeReturnCode::kFrozen;
      }

      // perform MwCAS to complete a write
      auto desc = MwCASDescriptor{};
      node->SetStatusForMwCAS(desc, status, status);
      node->SetMetadataForMwCAS(desc, rec_count, in_progress_meta, inserted_meta);
      if (desc.MwCAS()) break;
    }

    return NodeReturnCode::kSuccess;
  }

  static constexpr NodeReturnCode
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
    size_t rec_count;

    // local flags for insertion
    auto uniqueness = KeyExistence::kNotExist;

    /*----------------------------------------------------------------------------------------------
     * Phase 1: reserve free space to insert a record
     *--------------------------------------------------------------------------------------------*/
    while (true) {
      cur_status = node->GetStatusWordProtected();
      if (cur_status.IsFrozen()) return NodeReturnCode::kFrozen;
      if (!HasSpace(node, cur_status, block_size)) return NodeReturnCode::kNoSpace;

      // check uniqueness
      rec_count = cur_status.GetRecordCount();
      if (uniqueness != KeyExistence::kUncertain) {
        uniqueness = CheckUniqueness(node, key, rec_count, index_epoch).first;
        if (uniqueness == KeyExistence::kExist) {
          return NodeReturnCode::kKeyExist;
        }
      }

      // prepare new status for MwCAS
      const auto new_status = cur_status.AddRecordInfo(1, block_size, 0);

      // perform MwCAS to reserve space
      auto desc = MwCASDescriptor{};
      node->SetStatusForMwCAS(desc, cur_status, new_status);
      node->SetMetadataForMwCAS(desc, rec_count, Metadata{}, in_progress_meta);
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
      const auto status = node->GetStatusWordProtected();
      if (status.IsFrozen()) {
        // delete an inserted record
        node->SetMetadataByCAS(rec_count, in_progress_meta.UpdateOffset(0));
        return NodeReturnCode::kFrozen;
      }

      // recheck uniqueness if required
      if (uniqueness == KeyExistence::kUncertain) {
        uniqueness = CheckUniqueness(node, key, rec_count, index_epoch).first;
        if (uniqueness == KeyExistence::kExist) {
          // delete an inserted record
          node->SetMetadataByCAS(rec_count, in_progress_meta.UpdateOffset(0));
          return NodeReturnCode::kKeyExist;
        } else if (uniqueness == KeyExistence::kUncertain) {
          // retry if there are still uncertain records
          continue;
        }
      }

      // perform MwCAS to complete an insert
      auto desc = MwCASDescriptor{};
      node->SetStatusForMwCAS(desc, status, status);
      node->SetMetadataForMwCAS(desc, rec_count, in_progress_meta, inserted_meta);
      if (desc.MwCAS()) break;
    }

    return NodeReturnCode::kSuccess;
  }

  static constexpr NodeReturnCode
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
    size_t rec_count, target_index = 0;
    auto uniqueness = KeyExistence::kNotExist;

    /*----------------------------------------------------------------------------------------------
     * Phase 1: reserve free space to insert a record
     *--------------------------------------------------------------------------------------------*/
    while (true) {
      cur_status = node->GetStatusWordProtected();
      if (cur_status.IsFrozen()) return NodeReturnCode::kFrozen;
      if (!HasSpace(node, cur_status, block_size)) return NodeReturnCode::kNoSpace;

      // check whether a node includes a target key
      rec_count = cur_status.GetRecordCount();
      if (uniqueness != KeyExistence::kUncertain) {
        std::tie(uniqueness, target_index) = CheckUniqueness(node, key, rec_count, index_epoch);
        if (uniqueness == KeyExistence::kNotExist || uniqueness == KeyExistence::kDeleted) {
          return NodeReturnCode::kKeyNotExist;
        }

        if constexpr (CanCASUpdate<Payload>()) {
          if (uniqueness == KeyExistence::kExist && target_index < node->GetSortedCount()) {
            const auto target_meta = node->GetMetadataProtected(target_index);

            // update a record directly
            auto desc = MwCASDescriptor{};
            node->SetStatusForMwCAS(desc, cur_status, cur_status);
            node->SetMetadataForMwCAS(desc, target_index, target_meta, target_meta);
            node->SetPayloadForMwCAS(desc, target_meta, payload);
            if (desc.MwCAS()) return NodeReturnCode::kSuccess;
            continue;
          }
        }
      }

      // prepare new status for MwCAS
      const auto target_meta = node->GetMetadataProtected(target_index);
      const auto deleted_size = kWordLength + GetAlignedSize(target_meta.GetTotalLength());
      const auto new_status = cur_status.AddRecordInfo(1, block_size, deleted_size);

      // perform MwCAS to reserve space
      auto desc = MwCASDescriptor{};
      node->SetStatusForMwCAS(desc, cur_status, new_status);
      node->SetMetadataForMwCAS(desc, rec_count, Metadata{}, in_progress_meta);
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
      // check conflicts (concurrent SMOs)
      const auto status = node->GetStatusWordProtected();
      if (status.IsFrozen()) {
        return NodeReturnCode::kFrozen;
      }

      // recheck uniqueness if required
      if (uniqueness == KeyExistence::kUncertain) {
        uniqueness = CheckUniqueness(node, key, rec_count, index_epoch).first;
        if (uniqueness == KeyExistence::kNotExist || uniqueness == KeyExistence::kDeleted) {
          // delete an inserted record
          node->SetMetadataByCAS(rec_count, in_progress_meta.UpdateOffset(0));
          return NodeReturnCode::kKeyNotExist;
        } else if (uniqueness == KeyExistence::kUncertain) {
          continue;
        }
      }

      // perform MwCAS to complete an update
      auto desc = MwCASDescriptor{};
      node->SetStatusForMwCAS(desc, status, status);
      node->SetMetadataForMwCAS(desc, rec_count, in_progress_meta, inserted_meta);
      if (desc.MwCAS()) break;
    }

    return NodeReturnCode::kSuccess;
  }

  static constexpr NodeReturnCode
  Delete(  //
      BaseNode_t *node,
      const Key key,
      const size_t key_length,
      const size_t index_epoch = 1)
  {
    // variables and constants
    const auto in_progress_meta = Metadata::GetInsertingMeta(index_epoch);
    StatusWord cur_status;
    size_t rec_count, target_index = 0;
    auto uniqueness = KeyExistence::kNotExist;

    /*----------------------------------------------------------------------------------------------
     * Phase 1: reserve free space to insert a null record
     *--------------------------------------------------------------------------------------------*/
    while (true) {
      cur_status = node->GetStatusWordProtected();
      if (cur_status.IsFrozen()) return NodeReturnCode::kFrozen;
      if (!HasSpace(node, cur_status, key_length)) return NodeReturnCode::kNoSpace;

      // check whether a node includes a target key
      rec_count = cur_status.GetRecordCount();
      if (uniqueness != KeyExistence::kUncertain) {
        std::tie(uniqueness, target_index) = CheckUniqueness(node, key, rec_count, index_epoch);
        if (uniqueness == KeyExistence::kNotExist || uniqueness == KeyExistence::kDeleted) {
          return NodeReturnCode::kKeyNotExist;
        }

        if constexpr (CanCASUpdate<Payload>()) {
          if (uniqueness == KeyExistence::kExist && target_index < node->GetSortedCount()) {
            const auto target_meta = node->GetMetadataProtected(target_index);
            const auto deleted_meta = target_meta.Delete();
            const auto deleted_size = kWordLength + GetAlignedSize(target_meta.GetTotalLength());
            const auto new_status = cur_status.AddRecordInfo(0, 0, deleted_size);

            // delete a record directly
            auto desc = MwCASDescriptor{};
            node->SetStatusForMwCAS(desc, cur_status, new_status);
            node->SetMetadataForMwCAS(desc, target_index, target_meta, deleted_meta);
            if (desc.MwCAS()) return NodeReturnCode::kSuccess;
            continue;
          }
        }
      }

      // prepare new status for MwCAS
      const auto target_meta = node->GetMetadataProtected(target_index);
      const auto deleted_size =
          (2 * kWordLength) + GetAlignedSize(target_meta.GetTotalLength()) + key_length;
      const auto new_status = cur_status.AddRecordInfo(1, key_length, deleted_size);

      // perform MwCAS to reserve space
      auto desc = MwCASDescriptor{};
      node->SetStatusForMwCAS(desc, cur_status, new_status);
      node->SetMetadataForMwCAS(desc, rec_count, Metadata{}, in_progress_meta);
      if (desc.MwCAS()) break;

      // set retry flag (described in Section 4.2.1 Inserts: Concurrency issues)
      uniqueness = KeyExistence::kUncertain;
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
      const auto status = node->GetStatusWordProtected();
      if (status.IsFrozen()) {
        // delete an inserted record
        node->SetMetadataByCAS(rec_count, in_progress_meta.UpdateOffset(0));
        return NodeReturnCode::kFrozen;
      }

      // recheck uniqueness if required
      if (uniqueness == KeyExistence::kUncertain) {
        uniqueness = CheckUniqueness(node, key, rec_count, index_epoch).first;
        if (uniqueness == KeyExistence::kNotExist || uniqueness == KeyExistence::kDeleted) {
          // delete an inserted record
          node->SetMetadataByCAS(rec_count, in_progress_meta.UpdateOffset(0));
          return NodeReturnCode::kKeyNotExist;
        } else if (uniqueness == KeyExistence::kUncertain) {
          // retry if there are still uncertain records
          continue;
        }
      }

      // perform MwCAS to complete an insert
      auto desc = MwCASDescriptor{};
      node->SetStatusForMwCAS(desc, status, status);
      node->SetMetadataForMwCAS(desc, rec_count, in_progress_meta, deleted_meta);
      if (desc.MwCAS()) break;
    }

    return NodeReturnCode::kSuccess;
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
    // sort records in an unsorted region
    std::array<MetaRecord, kMaxUnsortedRecNum> new_records;
    size_t new_rec_num = 0;
    SortUnsortedRecords(node, new_records, new_rec_num);

    // sort all records by merge sort
    std::array<Metadata, kMaxRecordNum> results;
    size_t count = 0;
    MergeSortedRecords(node, new_records, new_rec_num, results, count);

    return {std::move(results), std::move(count)};
  }
};

}  // namespace dbgroup::index::bztree
