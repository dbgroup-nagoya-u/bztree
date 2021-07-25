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

#include "node.hpp"
#include "record_page.hpp"

namespace dbgroup::index::bztree::leaf
{
using component::AlignOffset;
using component::CallocNew;
using component::CanCASUpdate;
using component::IsEqual;
using component::IsInRange;
using component::KeyExistence;
using component::Metadata;
using component::MwCASDescriptor;
using component::Node;
using component::NodeReturnCode;
using component::ReadMwCASField;
using component::RecordPage;
using component::StatusWord;

using KeyIndex = std::pair<KeyExistence, int64_t>;

template <class Key, class Payload, class Compare>
using MetaRecord = typename Node<Key, Payload, Compare>::MetaRecord;

template <class Key, class Payload, class Compare>
using MetaArray = std::array<Metadata, Node<Key, Payload, Compare>::kMaxRecordNum>;

template <class Key, class Payload, class Compare>
using UnsortedMeta = std::array<MetaRecord<Key, Payload, Compare>, kMaxUnsortedRecNum>;

constexpr bool kLeafFlag = true;

/*################################################################################################
 * Internal utility functions
 *##############################################################################################*/

/**
 * @brief Get an index of the specified key by using binary search. If there is no
 * specified key, this returns the minimum metadata index that is greater than the
 * specified key
 *
 * @tparam Compare
 * @param key
 * @param comp
 * @return std::pair<KeyExistence, size_t>
 */
template <class Key, class Payload, class Compare>
KeyIndex
_SearchSortedMetadata(  //
    const Node<Key, Payload, Compare> *node,
    const Key &key)
{
  const int64_t sorted_count = node->GetSortedCount();

  int64_t index;
  int64_t begin_index = 0;
  int64_t end_index = sorted_count - 1;

  while (begin_index <= end_index) {
    index = (begin_index + end_index) >> 1;

    const auto meta = node->GetMetadataProtected(index);
    const auto index_key = node->GetKey(meta);

    if (Compare{}(key, index_key)) {
      // a target key is in a left side
      end_index = index - 1;
    } else if (Compare{}(index_key, key)) {
      // a target key is in a right side
      begin_index = index + 1;
    } else {
      // find an equivalent key
      if (meta.IsVisible()) {
        return {KeyExistence::kExist, index};
      } else {
        return {KeyExistence::kDeleted, index};
      }
    }
  }

  return {KeyExistence::kNotExist, begin_index};
}

template <class Key, class Payload, class Compare>
KeyIndex
_SearchUnsortedMeta(  //
    const Node<Key, Payload, Compare> *node,
    const Key &key,
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

template <class Key, class Payload, class Compare>
KeyIndex
_CheckUniqueness(  //
    const Node<Key, Payload, Compare> *node,
    const Key &key,
    const int64_t rec_count,
    const size_t epoch = 0)
{
  if constexpr (CanCASUpdate<Payload>()) {
    const auto [rc, index] = _SearchSortedMetadata(node, key);
    if (rc == KeyExistence::kNotExist || rc == KeyExistence::kDeleted) {
      // a new record may be inserted in an unsorted region
      return _SearchUnsortedMeta(node, key, rec_count - 1, node->GetSortedCount(), epoch);
    }
    return {rc, index};
  } else {
    const auto [rc, index] =
        _SearchUnsortedMeta(node, key, rec_count - 1, node->GetSortedCount(), epoch);
    if (rc == KeyExistence::kNotExist) {
      // a record may be in a sorted region
      return _SearchSortedMetadata(node, key);
    }
    return {rc, index};
  }
}

template <class Key, class Payload>
constexpr size_t
_GetAlignedSize(const size_t block_size)
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

template <class Key, class Payload, class Compare>
void
_CopyRecord(  //
    Node<Key, Payload, Compare> *target_node,
    size_t &offset,
    const Node<Key, Payload, Compare> *original_node,
    const Metadata meta)
{
  const auto total_length = meta.GetTotalLength();
  if constexpr (CanCASUpdate<Payload>()) {
    AlignOffset<Key>(offset);
  }
  offset -= total_length;

  memcpy(ShiftAddress(target_node, offset), original_node->GetKeyAddr(meta), total_length);
}

template <class Key, class Payload, class Compare>
void
_CopyRecords(  //
    Node<Key, Payload, Compare> *target_node,
    size_t current_rec_count,
    size_t &offset,
    const Node<Key, Payload, Compare> *original_node,
    const MetaArray<Key, Payload, Compare> &metadata,
    const size_t begin_id,
    const size_t end_id)
{
  for (size_t i = begin_id; i < end_id; ++i, ++current_rec_count) {
    // copy a record
    const auto meta = metadata[i];
    _CopyRecord(target_node, offset, original_node, meta);
    // copy metadata
    const auto new_meta = meta.UpdateOffset(offset);
    target_node->SetMetadata(current_rec_count, new_meta);
  }
}

template <class Key, class Payload, class Compare>
constexpr bool
_HasSpace(  //
    const Node<Key, Payload, Compare> *node,
    const StatusWord status,
    const size_t block_size)
{
  return status.GetRecordCount() - node->GetSortedCount() < kMaxUnsortedRecNum
         && status.GetOccupiedSize() + block_size <= kPageSize - kWordLength
         && status.GetDeletedSize() < kMaxDeletedSpaceSize;
}

template <class Key, class Payload, class Compare>
void
_SortUnsortedRecords(  //
    const Node<Key, Payload, Compare> *node,
    UnsortedMeta<Key, Payload, Compare> &arr,
    size_t &count)
{
  const int64_t rec_count = node->GetStatusWordProtected().GetRecordCount();
  const int64_t sorted_count = node->GetSortedCount();

  // sort unsorted records by insertion sort
  for (int64_t index = rec_count - 1; index >= sorted_count; --index) {
    const auto meta = node->GetMetadataProtected(index);
    if (!meta.IsInProgress()) {
      if (count == 0) {
        // insert a first record
        arr[0] = MetaRecord<Key, Payload, Compare>{meta, node->GetKey(meta)};
        ++count;
        continue;
      }

      // insert a new record into an appropiate position
      MetaRecord<Key, Payload, Compare> target{meta, node->GetKey(meta)};
      const auto ins_iter = std::lower_bound(arr.begin(), arr.begin() + count, target);
      if (*ins_iter != target) {
        const size_t ins_id = std::distance(arr.begin(), ins_iter);
        if (ins_id < count) {
          // shift upper records
          memmove(&(arr[ins_id + 1]), &(arr[ins_id]),
                  sizeof(MetaRecord<Key, Payload, Compare>) * (count - ins_id));
        }

        // insert a new record
        arr[ins_id] = std::move(target);
        ++count;
      }
    }
  }
}

template <class Key, class Payload, class Compare>
void
_SortUnsortedRecords(  //
    const Node<Key, Payload, Compare> *node,
    const Key *begin_key,
    const bool begin_closed,
    const Key *end_key,
    const bool end_closed,
    UnsortedMeta<Key, Payload, Compare> &arr,
    size_t &count)
{
  const int64_t rec_count = node->GetStatusWordProtected().GetRecordCount();
  const int64_t sorted_count = node->GetSortedCount();

  // sort unsorted records by insertion sort
  for (int64_t index = rec_count - 1L; index >= sorted_count; --index) {
    const auto meta = node->GetMetadataProtected(index);
    if (!meta.IsInProgress()) {
      auto key = node->GetKey(meta);
      if (!IsInRange<Compare>(key, begin_key, begin_closed, end_key, end_closed)) continue;

      if (count == 0) {
        // insert a first record
        arr[0] = MetaRecord<Key, Payload, Compare>{meta, std::move(key)};
        ++count;
        continue;
      }

      // insert a new record into an appropiate position
      MetaRecord<Key, Payload, Compare> target{meta, node->GetKey(meta)};
      const auto ins_iter = std::lower_bound(arr.begin(), arr.begin() + count, target);
      if (*ins_iter != target) {
        const size_t ins_id = std::distance(arr.begin(), ins_iter);
        if (ins_id < count) {
          // shift upper records
          memmove(&(arr[ins_id + 1]), &(arr[ins_id]),
                  sizeof(MetaRecord<Key, Payload, Compare>) * (count - ins_id));
        }

        // insert a new record
        arr[ins_id] = std::move(target);
        ++count;
      }
    }
  }
}

template <class Key, class Payload, class Compare>
void
_MergeSortedRecords(  //
    const Node<Key, Payload, Compare> *node,
    const UnsortedMeta<Key, Payload, Compare> &new_records,
    const size_t new_rec_num,
    MetaArray<Key, Payload, Compare> &arr,
    size_t &count)
{
  const auto sorted_count = node->GetSortedCount();

  size_t j = 0;
  for (size_t i = 0; i < sorted_count; ++i) {
    const auto meta = node->GetMetadataProtected(i);
    MetaRecord<Key, Payload, Compare> target{meta, node->GetKey(meta)};

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

template <class Key, class Payload, class Compare>
void
_MergeSortedRecords(  //
    const Node<Key, Payload, Compare> *node,
    const UnsortedMeta<Key, Payload, Compare> &new_records,
    const int64_t new_rec_num,
    const Key *begin_k,
    const bool begin_closed,
    const Key *end_k,
    const bool end_closed,
    MetaArray<Key, Payload, Compare> &arr,
    size_t &count)
{
  const int64_t sorted_count = node->GetSortedCount();

  // search a begin index for scan
  int64_t begin_index = 0;
  if (begin_k != nullptr) {
    KeyExistence rc;
    std::tie(rc, begin_index) = _SearchSortedMetadata(node, *begin_k);
    if (rc == KeyExistence::kDeleted || (rc == KeyExistence::kExist && !begin_closed)) {
      ++begin_index;
    }
  }

  int64_t j = 0;
  for (int64_t i = begin_index; i < sorted_count; ++i) {
    const auto meta = node->GetMetadataProtected(i);
    auto key = node->GetKey(meta);
    if (end_k != nullptr && (Compare{}(*end_k, key) || (end_closed && !Compare{}(key, *end_k)))) {
      break;
    }

    // move lower new records
    MetaRecord<Key, Payload, Compare> target{meta, std::move(key)};
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

/*################################################################################################
 * Read operations
 *##############################################################################################*/

template <class Key, class Payload, class Compare>
NodeReturnCode
Read(  //
    const Node<Key, Payload, Compare> *node,
    const Key &key,
    Payload &out_payload)
{
  const auto status = node->GetStatusWordProtected();
  const auto [existence, index] = _CheckUniqueness(node, key, status.GetRecordCount());
  if (existence == KeyExistence::kNotExist || existence == KeyExistence::kDeleted) {
    return NodeReturnCode::kKeyNotExist;
  }

  const auto meta = node->GetMetadataProtected(index);
  node->CopyPayload(meta, out_payload);
  return NodeReturnCode::kSuccess;
}

template <class Key, class Payload, class Compare>
void
Scan(  //
    const Node<Key, Payload, Compare> *node,
    const Key *begin_k,
    const bool begin_closed,
    const Key *end_k,
    const bool end_closed,
    RecordPage<Key, Payload> &page)
{
  // sort records in an unsorted region
  UnsortedMeta<Key, Payload, Compare> new_records;
  size_t new_rec_num = 0;
  _SortUnsortedRecords(node, begin_k, begin_closed, end_k, end_closed, new_records, new_rec_num);

  // sort all records by merge sort
  MetaArray<Key, Payload, Compare> metadata;
  size_t count = 0;
  _MergeSortedRecords(node, new_records, new_rec_num, begin_k, begin_closed, end_k, end_closed,
                      metadata, count);

  // copy scan results to a page for returning
  std::byte *cur_addr = reinterpret_cast<std::byte *>(&page) + component::kHeaderLength;
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

template <class Key, class Payload, class Compare>
NodeReturnCode
Write(  //
    Node<Key, Payload, Compare> *node,
    const Key &key,
    const size_t key_length,
    const Payload &payload,
    const size_t payload_length,
    const size_t index_epoch = 1)
{
  // variables and constants shared in Phase 1 & 2
  const auto total_length = key_length + payload_length;
  const auto block_size = _GetAlignedSize<Key, Payload>(total_length);
  const auto in_progress_meta = Metadata{index_epoch, key_length, total_length, true};
  StatusWord cur_status;
  size_t rec_count;

  /*----------------------------------------------------------------------------------------------
   * Phase 1: reserve free space to write a record
   *--------------------------------------------------------------------------------------------*/
  while (true) {
    cur_status = node->GetStatusWordProtected();
    if (cur_status.IsFrozen()) return NodeReturnCode::kFrozen;
    if (!_HasSpace(node, cur_status, block_size)) return NodeReturnCode::kNoSpace;

    rec_count = cur_status.GetRecordCount();
    if constexpr (CanCASUpdate<Payload>()) {
      // check whether a node includes a target key
      const auto [uniqueness, target_index] = _SearchSortedMetadata(node, key);
      if (uniqueness == KeyExistence::kExist) {
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
    const auto new_status = cur_status.Add(1, block_size);

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
  node->SetPayload(offset, payload, payload_length);
  node->SetKey(offset, key, key_length);

  // prepare record metadata for MwCAS
  const auto inserted_meta = in_progress_meta.MakeVisible(offset);

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

template <class Key, class Payload, class Compare>
NodeReturnCode
Insert(  //
    Node<Key, Payload, Compare> *node,
    const Key &key,
    const size_t key_length,
    const Payload &payload,
    const size_t payload_length,
    const size_t index_epoch = 1)
{
  // variables and constants shared in Phase 1 & 2
  const auto total_length = key_length + payload_length;
  const auto block_size = _GetAlignedSize<Key, Payload>(total_length);
  const auto in_progress_meta = Metadata{index_epoch, key_length, total_length, true};
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
    if (!_HasSpace(node, cur_status, block_size)) return NodeReturnCode::kNoSpace;

    // check uniqueness
    rec_count = cur_status.GetRecordCount();
    if (uniqueness != KeyExistence::kUncertain) {
      uniqueness = _CheckUniqueness(node, key, rec_count, index_epoch).first;
      if (uniqueness == KeyExistence::kExist) {
        return NodeReturnCode::kKeyExist;
      }
    }

    // prepare new status for MwCAS
    const auto new_status = cur_status.Add(1, block_size);

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
  node->SetPayload(offset, payload, payload_length);
  node->SetKey(offset, key, key_length);

  // prepare record metadata for MwCAS
  const auto inserted_meta = in_progress_meta.MakeVisible(offset);

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
      uniqueness = _CheckUniqueness(node, key, rec_count, index_epoch).first;
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

template <class Key, class Payload, class Compare>
NodeReturnCode
Update(  //
    Node<Key, Payload, Compare> *node,
    const Key &key,
    const size_t key_length,
    const Payload &payload,
    const size_t payload_length,
    const size_t index_epoch = 1)
{
  // variables and constants shared in Phase 1 & 2
  const auto total_length = key_length + payload_length;
  const auto block_size = _GetAlignedSize<Key, Payload>(total_length);
  const auto in_progress_meta = Metadata{index_epoch, key_length, total_length, true};
  StatusWord cur_status;
  size_t rec_count, target_index = 0;
  auto uniqueness = KeyExistence::kNotExist;

  /*----------------------------------------------------------------------------------------------
   * Phase 1: reserve free space to insert a record
   *--------------------------------------------------------------------------------------------*/
  while (true) {
    cur_status = node->GetStatusWordProtected();
    if (cur_status.IsFrozen()) return NodeReturnCode::kFrozen;
    if (!_HasSpace(node, cur_status, block_size)) return NodeReturnCode::kNoSpace;

    // check whether a node includes a target key
    rec_count = cur_status.GetRecordCount();
    if (uniqueness != KeyExistence::kUncertain) {
      std::tie(uniqueness, target_index) = _CheckUniqueness(node, key, rec_count, index_epoch);
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
    const auto deleted_size =
        kWordLength + _GetAlignedSize<Key, Payload>(target_meta.GetTotalLength());
    const auto new_status = cur_status.Add(1, block_size).Delete(deleted_size);

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
  node->SetPayload(offset, payload, payload_length);
  node->SetKey(offset, key, key_length);

  // prepare record metadata for MwCAS
  const auto inserted_meta = in_progress_meta.MakeVisible(offset);

  while (true) {
    // check conflicts (concurrent SMOs)
    const auto status = node->GetStatusWordProtected();
    if (status.IsFrozen()) {
      return NodeReturnCode::kFrozen;
    }

    // recheck uniqueness if required
    if (uniqueness == KeyExistence::kUncertain) {
      uniqueness = _CheckUniqueness(node, key, rec_count, index_epoch).first;
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

template <class Key, class Payload, class Compare>
NodeReturnCode
Delete(  //
    Node<Key, Payload, Compare> *node,
    const Key &key,
    const size_t key_length,
    const size_t index_epoch = 1)
{
  // variables and constants
  const auto in_progress_meta = Metadata{index_epoch, key_length, key_length, true};
  StatusWord cur_status;
  size_t rec_count, target_index = 0;
  auto uniqueness = KeyExistence::kNotExist;

  /*----------------------------------------------------------------------------------------------
   * Phase 1: reserve free space to insert a null record
   *--------------------------------------------------------------------------------------------*/
  while (true) {
    cur_status = node->GetStatusWordProtected();
    if (cur_status.IsFrozen()) return NodeReturnCode::kFrozen;
    if (!_HasSpace(node, cur_status, key_length)) return NodeReturnCode::kNoSpace;

    // check whether a node includes a target key
    rec_count = cur_status.GetRecordCount();
    if (uniqueness != KeyExistence::kUncertain) {
      std::tie(uniqueness, target_index) = _CheckUniqueness(node, key, rec_count, index_epoch);
      if (uniqueness == KeyExistence::kNotExist || uniqueness == KeyExistence::kDeleted) {
        return NodeReturnCode::kKeyNotExist;
      }

      if constexpr (CanCASUpdate<Payload>()) {
        if (uniqueness == KeyExistence::kExist && target_index < node->GetSortedCount()) {
          const auto target_meta = node->GetMetadataProtected(target_index);
          const auto deleted_meta = target_meta.Delete();
          const auto deleted_size =
              kWordLength + _GetAlignedSize<Key, Payload>(target_meta.GetTotalLength());
          const auto new_status = cur_status.Delete(deleted_size);

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
    const auto deleted_size = (2 * kWordLength)
                              + _GetAlignedSize<Key, Payload>(target_meta.GetTotalLength())
                              + key_length;
    const auto new_status = cur_status.Add(1, key_length).Delete(deleted_size);

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
  node->SetKey(offset, key, key_length);

  // prepare record metadata for MwCAS
  const auto deleted_meta = in_progress_meta.MakeInvisible(offset);

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
      uniqueness = _CheckUniqueness(node, key, rec_count, index_epoch).first;
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

template <class Key, class Payload, class Compare>
Node<Key, Payload, Compare> *
Consolidate(  //
    const Node<Key, Payload, Compare> *orig_node,
    const MetaArray<Key, Payload, Compare> &metadata,
    const size_t rec_count)
{
  // create a new node and copy records
  auto new_node = CallocNew<Node<Key, Payload, Compare>>(kPageSize, kLeafFlag);
  auto offset = kPageSize;
  _CopyRecords(new_node, 0, offset, orig_node, metadata, 0, rec_count);
  new_node->SetSortedCount(rec_count);
  if constexpr (CanCASUpdate<Payload>()) {
    AlignOffset<Key>(offset);
  }
  new_node->SetStatus(StatusWord{rec_count, kPageSize - offset});

  return new_node;
}

template <class Key, class Payload, class Compare>
std::pair<Node<Key, Payload, Compare> *, Node<Key, Payload, Compare> *>
Split(  //
    const Node<Key, Payload, Compare> *orig_node,
    const MetaArray<Key, Payload, Compare> &metadata,
    const size_t rec_count,
    const size_t left_rec_count)
{
  const auto right_rec_count = rec_count - left_rec_count;

  // create a split left node
  auto left_node = CallocNew<Node<Key, Payload, Compare>>(kPageSize, kLeafFlag);
  auto offset = kPageSize;
  _CopyRecords(left_node, 0, offset, orig_node, metadata, 0, left_rec_count);
  left_node->SetSortedCount(left_rec_count);
  if constexpr (CanCASUpdate<Payload>()) {
    AlignOffset<Key>(offset);
  }
  left_node->SetStatus(StatusWord{left_rec_count, kPageSize - offset});

  // create a split right node
  auto right_node = CallocNew<Node<Key, Payload, Compare>>(kPageSize, kLeafFlag);
  offset = kPageSize;
  _CopyRecords(right_node, 0, offset, orig_node, metadata, left_rec_count, rec_count);
  right_node->SetSortedCount(right_rec_count);
  if constexpr (CanCASUpdate<Payload>()) {
    AlignOffset<Key>(offset);
  }
  right_node->SetStatus(StatusWord{right_rec_count, kPageSize - offset});

  return {left_node, right_node};
}

template <class Key, class Payload, class Compare>
Node<Key, Payload, Compare> *
Merge(  //
    const Node<Key, Payload, Compare> *left_node,
    const MetaArray<Key, Payload, Compare> &left_meta,
    const size_t l_rec_count,
    const Node<Key, Payload, Compare> *right_node,
    const MetaArray<Key, Payload, Compare> &right_meta,
    const size_t r_rec_count)
{
  const auto rec_count = l_rec_count + r_rec_count;

  // create a merged node
  auto new_node = CallocNew<Node<Key, Payload, Compare>>(kPageSize, kLeafFlag);
  auto offset = kPageSize;
  _CopyRecords(new_node, 0, offset, left_node, left_meta, 0, l_rec_count);
  _CopyRecords(new_node, l_rec_count, offset, right_node, right_meta, 0, r_rec_count);
  new_node->SetSortedCount(rec_count);
  if constexpr (CanCASUpdate<Payload>()) {
    AlignOffset<Key>(offset);
  }
  new_node->SetStatus(StatusWord{rec_count, kPageSize - offset});

  return new_node;
}

/*################################################################################################
 * Public utility functions
 *##############################################################################################*/

template <class Key, class Payload, class Compare>
std::pair<MetaArray<Key, Payload, Compare>, size_t>
GatherSortedLiveMetadata(const Node<Key, Payload, Compare> *node)
{
  // sort records in an unsorted region
  UnsortedMeta<Key, Payload, Compare> new_records;
  size_t new_rec_num = 0;
  _SortUnsortedRecords(node, new_records, new_rec_num);

  // sort all records by merge sort
  MetaArray<Key, Payload, Compare> results;
  size_t count = 0;
  _MergeSortedRecords(node, new_records, new_rec_num, results, count);

  return {std::move(results), std::move(count)};
}

}  // namespace dbgroup::index::bztree::leaf
