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
using component::CanCASUpdate;
using component::IsEqual;
using component::IsInRange;
using component::KeyExistence;
using component::Metadata;
using component::MwCASDescriptor;
using component::Node;
using component::NodeReturnCode;
using component::RecordPage;
using component::StatusWord;

using KeyIndex = std::pair<KeyExistence, int64_t>;

template <class Key, class Payload, class Compare>
using MetaRecord = typename Node<Key, Payload, Compare>::MetaRecord;

template <class Key, class Payload, class Compare>
using MetaArray = std::array<Metadata, Node<Key, Payload, Compare>::kMaxRecordNum>;

template <class Key, class Payload, class Compare>
using NewSortedMeta = std::array<MetaRecord<Key, Payload, Compare>, kMaxUnsortedRecNum>;

/*################################################################################################
 * Internal utility functions
 *##############################################################################################*/

/**
 * @brief Align a specified block size if needed.
 *
 * @tparam Key a target key class.
 * @tparam Payload a target payload class.
 * @param block_size a target block size.
 * @return size_t: an aligned block size.
 */
template <class Key, class Payload>
constexpr size_t
_GetAlignedSize(const size_t block_size)
{
  if constexpr (CanCASUpdate<Payload>()) {
    if constexpr (IsVariableLengthData<Key>()) {
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

/**
 * @brief Check a target node requires consolidation.
 *
 * @tparam Key a target key class.
 * @tparam Payload a target payload class.
 * @tparam Compare a comparetor class for keys.
 * @param node a target leaf node.
 * @param status a current status word of a target node.
 * @param block_size a current block size of a target node.
 * @retval true if a target node has sufficient space for inserting a new record.
 * @retval false if a target node requires consolidation.
 */
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

/**
 * @brief Sort unsorted metadata by using inserting sort for SMOs.
 *
 * @tparam Key a target key class.
 * @tparam Payload a target payload class.
 * @tparam Compare a comparetor class for keys.
 * @param node a target leaf node.
 * @param arr a result array of sorted metadata.
 * @param count the number of metadata.
 */
template <class Key, class Payload, class Compare>
auto
_SortNewRecords(const Node<Key, Payload, Compare> *node)  //
    -> std::pair<size_t, NewSortedMeta<Key, Payload, Compare>>
{
  const int64_t rec_count = node->GetStatusWordProtected().GetRecordCount();
  const int64_t sorted_count = node->GetSortedCount();

  NewSortedMeta<Key, Payload, Compare> arr;
  size_t count = 0;

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

  return {count, arr};
}

/*################################################################################################
 * Read operations
 *##############################################################################################*/

/**
 * @brief Read a payload of a specified key if it exists.
 *
 * @tparam Key a target key class.
 * @tparam Payload a target payload class.
 * @tparam Compare a comparetor class for keys.
 * @param node a target node.
 * @param key a target key.
 * @param out_payload a reference to be stored a target payload.
 * @retval kSuccess if a key exists.
 * @retval kKeyNotExist if a key does not exist.
 */
template <class Key, class Payload, class Compare>
NodeReturnCode
Read(  //
    const Node<Key, Payload, Compare> *node,
    const Key &key,
    Payload &out_payload)
{
  const auto status = node->GetStatusWordProtected();
  const auto [existence, index] = node->CheckUniqueness(key, status.GetRecordCount());
  if (existence == KeyExistence::kNotExist || existence == KeyExistence::kDeleted) {
    return NodeReturnCode::kKeyNotExist;
  }

  const auto meta = node->GetMetadataProtected(index);
  node->CopyPayload(meta, out_payload);
  return NodeReturnCode::kSuccess;
}

// /**
//  * @brief Perform a range scan with specified keys.
//  *
//  * If a begin/end key is nullptr, it is treated as negative or positive infinite.
//  *
//  * @tparam Key a target key class.
//  * @tparam Payload a target payload class.
//  * @tparam Compare a comparetor class for keys.
//  * @param node a target node.
//  * @param begin_k the pointer of a begin key of a range scan.
//  * @param begin_closed a flag to indicate whether the begin side of a range is closed.
//  * @param end_k the pointer of an end key of a range scan.
//  * @param end_closed a flag to indicate whether the end side of a range is closed.
//  * @param page a page to copy target keys/payloads. This argument is used internally.
//  * @retval true if scanning finishes.
//  * @retval false if scanning is in progress.
//  */
// template <class Key, class Payload, class Compare>
// bool
// Scan(  //
//     const Node<Key, Payload, Compare> *node,
//     const Key *begin_k,
//     const bool begin_closed,
//     const Key *end_k,
//     const bool end_closed,
//     RecordPage<Key, Payload> *page)
// {
//   // sort records in an unsorted region
//   NewSortedMeta<Key, Payload, Compare> new_records;
//   size_t new_rec_num = 0;
//   _SortUnsortedRecords(node, begin_k, begin_closed, end_k, end_closed, new_records, new_rec_num);

//   // sort all records by merge sort
//   MetaArray<Key, Payload, Compare> metadata;
//   size_t count = 0;
//   const auto scan_finished = _MergeSortedRecords(node, new_records, new_rec_num, begin_k,
//                                                  begin_closed, end_k, end_closed, metadata,
//                                                  count);

//   // copy scan results to a page for returning
//   std::byte *cur_addr = reinterpret_cast<std::byte *>(page) + component::kHeaderLength;
//   for (size_t i = 0; i < count; ++i) {
//     const Metadata meta = metadata[i];
//     if constexpr (IsVariableLengthData<Key>()) {
//       *(reinterpret_cast<uint32_t *>(cur_addr)) = meta.GetKeyLength();
//       cur_addr += sizeof(uint32_t);
//     }
//     if constexpr (IsVariableLengthData<Payload>()) {
//       *(reinterpret_cast<uint32_t *>(cur_addr)) = meta.GetPayloadLength();
//       cur_addr += sizeof(uint32_t);
//     }
//     if constexpr (CanCASUpdate<Payload>()) {
//       if constexpr (IsVariableLengthData<Key>()) {
//         memcpy(cur_addr, node->GetKeyAddr(meta), meta.GetKeyLength());
//         cur_addr += meta.GetKeyLength();
//       } else {
//         memcpy(cur_addr, node->GetKeyAddr(meta), sizeof(Key));
//         cur_addr += sizeof(Key);
//       }
//       *(reinterpret_cast<Payload *>(cur_addr)) =
//           MwCASDescriptor::Read<Payload>(node->GetPayloadAddr(meta));
//       cur_addr += kWordLength;
//     } else {
//       memcpy(cur_addr, node->GetKeyAddr(meta), meta.GetTotalLength());
//       cur_addr += meta.GetTotalLength();
//     }
//   }
//   page->SetEndAddress(cur_addr);
//   if (count > 0) {
//     page->SetLastKeyAddress(cur_addr - metadata[count - 1].GetTotalLength());
//   }

//   return scan_finished;
// }

/*################################################################################################
 * Write operations
 *##############################################################################################*/

/**
 * @brief Write (i.e., upsert) a specified kay/payload pair.
 *
 * If a specified key does not exist in a leaf node, this function performs an insert
 * operation. If a specified key has been already inserted, this function perfroms an
 * update operation.
 *
 * Note that if a target key/payload is binary data, it is required to specify its
 * length in bytes.
 *
 * @tparam Key a target key class.
 * @tparam Payload a target payload class.
 * @tparam Compare a comparetor class for keys.
 * @param node a target node.
 * @param key a target key to be written.
 * @param payload a target payload to be written.
 * @param key_length the length of a target key.
 * @param payload_length the length of a target payload.
 * @param epoch an epoch of the BzTree instance.
 * @retval kSuccess if a key/payload pair is written.
 * @retval kFrozen if a target node is frozen.
 * @retval kNeedConsolidation if a target node requires consolidation.
 */
template <class Key, class Payload, class Compare>
NodeReturnCode
Write(  //
    Node<Key, Payload, Compare> *node,
    const Key &key,
    const size_t key_length,
    const Payload &payload,
    const size_t payload_length,
    const size_t epoch = 1)
{
  // variables and constants shared in Phase 1 & 2
  const auto total_length = key_length + payload_length;
  const auto block_size = _GetAlignedSize<Key, Payload>(total_length);
  const auto in_progress_meta = Metadata{epoch, key_length, total_length, true};
  StatusWord cur_status;
  size_t rec_count;

  /*----------------------------------------------------------------------------------------------
   * Phase 1: reserve free space to write a record
   *--------------------------------------------------------------------------------------------*/
  while (true) {
    cur_status = node->GetStatusWordProtected();
    if (cur_status.IsFrozen()) return NodeReturnCode::kFrozen;
    if (!_HasSpace(node, cur_status, block_size)) return NodeReturnCode::kNeedConsolidation;

    rec_count = cur_status.GetRecordCount();
    if constexpr (CanCASUpdate<Payload>()) {
      // check whether a node includes a target key
      const auto [uniqueness, target_index] = node->SearchSortedRecord(key);
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

/**
 * @brief Insert a specified kay/payload pair.
 *
 * This function performs a uniqueness check in its processing. If a specified key does
 * not exist, this function insert a target payload into a target leaf node. If a
 * specified key exists in a target leaf node, this function does nothing and returns
 * kKeyExist as a return code.
 *
 * Note that if a target key/payload is binary data, it is required to specify its
 * length in bytes.
 *
 * @tparam Key a target key class.
 * @tparam Payload a target payload class.
 * @tparam Compare a comparetor class for keys.
 * @param node a target node.
 * @param key a target key to be written.
 * @param payload a target payload to be written.
 * @param key_length the length of a target key.
 * @param payload_length the length of a target payload.
 * @param epoch an epoch of the BzTree instance.
 * @retval kSuccess if a key/payload pair is written.
 * @retval kKeyExist if a specified key exists.
 * @retval kFrozen if a target node is frozen.
 * @retval kNeedConsolidation if a target node requires consolidation.
 */
template <class Key, class Payload, class Compare>
NodeReturnCode
Insert(  //
    Node<Key, Payload, Compare> *node,
    const Key &key,
    const size_t key_length,
    const Payload &payload,
    const size_t payload_length,
    const size_t epoch = 1)
{
  // variables and constants shared in Phase 1 & 2
  const auto total_length = key_length + payload_length;
  const auto block_size = _GetAlignedSize<Key, Payload>(total_length);
  const auto in_progress_meta = Metadata{epoch, key_length, total_length, true};
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
    if (!_HasSpace(node, cur_status, block_size)) return NodeReturnCode::kNeedConsolidation;

    // check uniqueness
    rec_count = cur_status.GetRecordCount();
    if (uniqueness != KeyExistence::kUncertain) {
      uniqueness = node->CheckUniqueness(key, rec_count).first;
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
      uniqueness = node->CheckUniqueness(key, rec_count).first;
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

/**
 * @brief Update a target kay with a specified payload.
 *
 * This function performs a uniqueness check in its processing. If a specified key
 * exist, this function update a target payload. If a specified key does not exist in
 * a target leaf node, this function does nothing and returns kKeyNotExist as a return
 * code.
 *
 * Note that if a target key/payload is binary data, it is required to specify its
 * length in bytes.
 *
 * @tparam Key a target key class.
 * @tparam Payload a target payload class.
 * @tparam Compare a comparetor class for keys.
 * @param node a target node.
 * @param key a target key to be written.
 * @param payload a target payload to be written.
 * @param key_length the length of a target key.
 * @param payload_length the length of a target payload.
 * @param epoch an epoch of the BzTree instance.
 * @retval kSuccess if a key/payload pair is written.
 * @retval kKeyNotExist if a specified key does not exist.
 * @retval kFrozen if a target node is frozen.
 * @retval kNeedConsolidation if a target node requires consolidation.
 */
template <class Key, class Payload, class Compare>
NodeReturnCode
Update(  //
    Node<Key, Payload, Compare> *node,
    const Key &key,
    const size_t key_length,
    const Payload &payload,
    const size_t payload_length,
    const size_t epoch = 1)
{
  // variables and constants shared in Phase 1 & 2
  const auto total_length = key_length + payload_length;
  const auto block_size = _GetAlignedSize<Key, Payload>(total_length);
  const auto in_progress_meta = Metadata{epoch, key_length, total_length, true};
  StatusWord cur_status;
  size_t rec_count, target_index = 0;
  auto uniqueness = KeyExistence::kNotExist;

  /*----------------------------------------------------------------------------------------------
   * Phase 1: reserve free space to insert a record
   *--------------------------------------------------------------------------------------------*/
  while (true) {
    cur_status = node->GetStatusWordProtected();
    if (cur_status.IsFrozen()) return NodeReturnCode::kFrozen;
    if (!_HasSpace(node, cur_status, block_size)) return NodeReturnCode::kNeedConsolidation;

    // check whether a node includes a target key
    rec_count = cur_status.GetRecordCount();
    if (uniqueness != KeyExistence::kUncertain) {
      std::tie(uniqueness, target_index) = node->CheckUniqueness(key, rec_count);
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
      uniqueness = node->CheckUniqueness(key, rec_count).first;
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

/**
 * @brief Delete a target kay from the index.
 *
 * This function performs a uniqueness check in its processing. If a specified key
 * exist, this function deletes it. If a specified key does not exist in a leaf node,
 * this function does nothing and returns kKeyNotExist as a return code.
 *
 * Note that if a target key is binary data, it is required to specify its length in
 * bytes.
 *
 * @tparam Key a target key class.
 * @tparam Payload a target payload class.
 * @tparam Compare a comparetor class for keys.
 * @param node a target node.
 * @param key a target key to be written.
 * @param key_length the length of a target key.
 * @param epoch an epoch of the BzTree instance.
 * @retval kSuccess if a key/payload pair is written.
 * @retval kKeyNotExist if a specified key does not exist.
 * @retval kFrozen if a target node is frozen.
 * @retval kNeedConsolidation if a target node requires consolidation.
 */
template <class Key, class Payload, class Compare>
NodeReturnCode
Delete(  //
    Node<Key, Payload, Compare> *node,
    const Key &key,
    const size_t key_length,
    const size_t epoch = 1)
{
  // variables and constants
  const auto in_progress_meta = Metadata{epoch, key_length, key_length, true};
  StatusWord cur_status;
  size_t rec_count, target_index = 0;
  auto uniqueness = KeyExistence::kNotExist;

  /*----------------------------------------------------------------------------------------------
   * Phase 1: reserve free space to insert a null record
   *--------------------------------------------------------------------------------------------*/
  while (true) {
    cur_status = node->GetStatusWordProtected();
    if (cur_status.IsFrozen()) return NodeReturnCode::kFrozen;
    if (!_HasSpace(node, cur_status, key_length)) return NodeReturnCode::kNeedConsolidation;

    // check whether a node includes a target key
    rec_count = cur_status.GetRecordCount();
    if (uniqueness != KeyExistence::kUncertain) {
      std::tie(uniqueness, target_index) = node->CheckUniqueness(key, rec_count);
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
      uniqueness = node->CheckUniqueness(key, rec_count).first;
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

/**
 * @brief Consolidate a target leaf node.
 *
 * @tparam Key a target key class.
 * @tparam Payload a target payload class.
 * @tparam Compare a comparetor class for keys.
 * @param new_node a consolidated node.
 * @param old_node an original node.
 */
template <class Key, class Payload, class Compare>
void
Consolidate(  //
    Node<Key, Payload, Compare> *new_node,
    const Node<Key, Payload, Compare> *old_node)
{
  if (!old_node->HasNext()) {
    new_node->SetRightEndFlag();
  }

  // sort records in an unsorted region
  const auto [new_rec_num, records] = _SortNewRecords(old_node);

  // perform merge-sort to consolidate a node
  const auto sorted_count = old_node->GetSortedCount();
  size_t offset = kPageSize;
  size_t rec_count = 0;

  size_t j = 0;
  for (size_t i = 0; i < sorted_count; ++i) {
    const auto meta = node->GetMetadataProtected(i);
    const auto key = node->GetKey(meta);

    // copy new records
    for (; j < new_rec_num; ++j) {
      auto [target_meta, target_key] = records[j];
      if (Compare{}(key, target_key)) break;

      // check a new record is active
      if (target_meta.IsVisible()) {
        offset = new_node->CopyRecordFrom<Payload>(old_node, target_meta, rec_count++, offset);
      }
    }

    // check a new record is updated one
    if (j < new_rec_num && IsEqual(key, records[j].key)) {
      const auto target_meta = records[j++].meta;
      if (target_meta.IsVisible()) {
        offset = new_node->CopyRecordFrom<Payload>(old_node, target_meta, rec_count++, offset);
      }
    } else if (meta.IsVisible()) {
      offset = new_node->CopyRecordFrom<Payload>(old_node, meta, rec_count++, offset);
    }
  }

  // move remaining new records
  for (; j < new_rec_num; ++j) {
    const auto target_meta = records[j++].meta;
    if (target_meta.IsVisible()) {
      offset = new_node->CopyRecordFrom<Payload>(old_node, target_meta, rec_count++, offset);
    }
  }

  // set header information
  new_node->SetSortedCount(rec_count);
  new_node->SetStatus(StatusWord{rec_count, kPageSize - offset});
}

}  // namespace dbgroup::index::bztree::leaf
