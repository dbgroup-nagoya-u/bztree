// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <list>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "base_node.hpp"

namespace bztree
{
class LeafNode : public BaseNode
{
 private:
  /*################################################################################################
   * Internal enum and constants
   *##############################################################################################*/

  /**
   * @brief Flags to check the uniqueness of concurrent writes.
   *
   */
  enum Uniqueness
  {
    kKeyNotExist,
    kKeyExist,
    kReCheck
  };

  /*################################################################################################
   * Internal constructors
   *##############################################################################################*/

  explicit LeafNode(const size_t node_size) : BaseNode(node_size, true) {}

  /*################################################################################################
   * Internal setter/getter
   *##############################################################################################*/

  std::unique_ptr<std::byte[]>
  GetCopiedKey(const Metadata meta)
  {
    auto key_ptr = GetKeyPtr(meta);
    auto key_length = meta.GetKeyLength();
    auto copied_key_ptr = std::make_unique<std::byte[]>(key_length);
    memcpy(copied_key_ptr.get(), key_ptr, key_length);
    return copied_key_ptr;
  }

  std::unique_ptr<std::byte[]>
  GetCopiedPayload(const Metadata meta)
  {
    auto payload_ptr = GetPayloadPtr(meta);
    auto payload_length = meta.GetPayloadLength();
    auto copied_payload_ptr = std::make_unique<std::byte[]>(payload_length);
    memcpy(copied_payload_ptr.get(), payload_ptr, payload_length);
    return copied_payload_ptr;
  }

  /*################################################################################################
   * Internal utility functions
   *##############################################################################################*/

  void
  RemoveDeletedRecords(std::map<std::unique_ptr<std::byte[]>, std::unique_ptr<std::byte[]>> records)
  {
    for (auto iter = records.begin(); iter != records.end();) {
      if (iter->second == nullptr) {
        iter = records.erase(iter);
      } else {
        ++iter;
      }
    }
  }

  void
  RemoveDeletedMetadata(std::map<const std::byte *, uint64_t> metadata)
  {
    for (auto iter = metadata.begin(); iter != metadata.end();) {
      if (iter->second == 0) {
        iter = metadata.erase(iter);
      } else {
        ++iter;
      }
    }
  }

  template <class Compare>
  KeyExistence
  SearchUnsortedMetaToWrite(  //
      const std::byte *key,
      const size_t begin_index,
      const size_t sorted_count,
      const size_t index_epoch,
      Compare comp)
  {
    // perform a linear search in revese order
    for (size_t index = begin_index; index >= sorted_count; --index) {
      const auto meta = GetMetadataProtected(index);
      if (IsEqual(key, GetKeyPtr(meta), comp)) {
        if (meta.IsVisible()) {
          return KeyExistence::kExist;
        } else if (meta.IsDeleted()) {
          return KeyExistence::kDeleted;
        } else if (!meta.IsCorrupted(index_epoch)) {
          // there is in progress records
          return KeyExistence::kUncertain;
        }
        // there is a key, but it is corrupted due to a machine failure
      }
    }
    return KeyExistence::kNotExist;
  }

  template <class Compare>
  KeyExistence
  CheckUniqueness(  //
      const std::byte *key,
      Compare comp,
      const size_t record_count,
      const size_t index_epoch)
  {
    if (auto existence =
            SearchUnsortedMetaToWrite(key, record_count - 1, GetSortedCount(), index_epoch, comp);
        existence == KeyExistence::kNotExist) {
      // there is no key in unsorted metadata, so search a sorted region
      return SearchSortedMetadata(key, true, comp).first;
    } else {
      return existence;
    }
  }

  template <class Compare>
  std::pair<BaseNode::KeyExistence, size_t>
  SearchUnsortedMetadataToRead(  //
      const std::byte *key,
      Compare comp,
      const size_t end_index,
      const size_t record_count)
  {
    for (size_t index = record_count - 1; index >= end_index; index--) {
      const auto meta = GetMetadata(index);
      if (IsEqual(key, GetKeyPtr(meta), comp)) {
        if (meta.IsVisible()) {
          return std::make_pair(KeyExistence::kExist, index);
        } else if (meta.IsDeleted()) {
          return std::make_pair(KeyExistence::kDeleted, index);
        }
        // there is a key, but it is in inserting or corrupted
      }
    }
    return std::make_pair(KeyExistence::kNotExist, 0);
  }

  template <class Compare>
  std::pair<BaseNode::KeyExistence, size_t>
  SearchMetadataToRead(  //
      const std::byte *key,
      Compare comp,
      const size_t record_count)
  {
    auto result_pair = SearchUnsortedMetadataToRead(key, comp, GetSortedCount(), record_count);
    if (result_pair.first == KeyExistence::kExist) {
      return result_pair;
    } else {
      return SearchSortedMetadata(key, true, comp);
    }
  }

  constexpr NodeReturnCode
  CheckRemainingCapacity(  //
      const StatusWord status,
      const size_t block_size_threshold,
      const size_t deleted_size_threshold) const
  {
    if (status.GetBlockSize() < block_size_threshold
        && status.GetDeletedSize() < deleted_size_threshold) {
      return NodeReturnCode::kSuccess;
    } else {
      return NodeReturnCode::kConsolidationRequired;
    }
  }

  template <class Compare>
  std::pair<std::map<std::byte *, uint64_t, Compare>, size_t>
  GatherAndSortLiveMetadata(Compare comp)
  {
    // gather lastest written key and its metadata
    std::map<const std::byte *, uint64_t, Compare> meta_pairs;
    auto new_block_length = 0;
    for (size_t index = GetStatusWord().GetRecordCount() - 1; index >= 0; --index) {
      const auto meta = GetMetadata(index);
      if (meta.IsVisible()) {
        meta_pairs.try_emplace(GetKeyPtr(meta), GetMetadata(index));
        new_block_length += meta.GetTotalLength();
      } else if (meta.IsDeleted()) {
        meta_pairs.try_emplace(GetKeyPtr(meta), 0);
      }
      // there is a key, but it is in inserting or corrupted.
      // NOTE: we can ignore inserting records because concurrent writes are aborted due to FREEZE.
    }
    RemoveDeletedRecords(meta_pairs);

    return std::make_pair(meta_pairs, new_block_length);
  }

  std::map<std::byte *, Metadata>::iterator
  CopyRecordsViaMetadata(  //
      LeafNode *original,
      std::map<std::byte *, Metadata>::iterator meta_iter,
      const size_t record_count)
  {
    auto offset = GetNodeSize();
    for (size_t index = 0; index < record_count; ++index) {
      const auto meta = meta_iter->second;
      // copy a record
      const auto key = meta_iter->first;
      const auto key_length = meta.GetKeyLength();
      const auto payload = original->GetPayloadPtr(meta);
      const auto payload_length = meta.GetPayloadLength();
      offset = CopyRecord(key, key_length, payload, payload_length, offset);
      // copy metadata
      const auto new_meta = meta.UpdateOffset(offset);
      SetMetadata(index, new_meta);
      // get a next capied metadata
      ++meta_iter;
    }
    SetStatusWord(kInitStatusWord.AddRecordInfo(record_count, GetNodeSize() - offset, 0));
    SetSortedCount(record_count);

    return meta_iter;
  }

 public:
  /*################################################################################################
   * Public constructor/destructor
   *##############################################################################################*/

  LeafNode(const LeafNode &) = delete;
  LeafNode &operator=(const LeafNode &) = delete;
  LeafNode(LeafNode &&) = default;
  LeafNode &operator=(LeafNode &&) = default;
  ~LeafNode() = default;

  /*################################################################################################
   * Public builders
   *##############################################################################################*/

  static LeafNode *
  CreateEmptyNode(const size_t node_size)
  {
    assert((node_size % kWordLength) == 0);

    auto aligned_page = aligned_alloc(kWordLength, node_size);
    auto new_node = new (aligned_page) LeafNode{node_size};
    return new_node;
  }

  /*################################################################################################
   * Read operations
   *##############################################################################################*/

  /**
   * @brief
   *
   * @tparam Compare
   * @param key
   * @param key_length
   * @param comp
   * @return std::pair<BaseNode::NodeReturnCode, std::unique_ptr<std::byte[]>>
   */
  template <class Compare>
  std::pair<BaseNode::NodeReturnCode, std::unique_ptr<std::byte[]>>
  Read(  //
      const std::byte *key,
      Compare comp)
  {
    const auto status = GetStatusWord();
    const auto result_pair = SearchMetadataToRead(key, comp, status.GetRecordCount());
    if (result_pair.first == KeyExistence::kNotExist) {
      return std::make_pair(NodeReturnCode::kKeyNotExist, nullptr);
    } else {
      const auto meta = GetMetadata(result_pair.second);
      return std::make_pair(NodeReturnCode::kSuccess, GetCopiedPayload(meta));
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
   * @param comp
   * @return std::pair<BaseNode::NodeReturnCode,
   *         std::vector<std::pair<std::unique_ptr<std::byte[]>, std::unique_ptr<std::byte[]>>>>
   */
  template <class Compare>
  std::pair<BaseNode::NodeReturnCode,
            std::vector<std::pair<std::unique_ptr<std::byte[]>, std::unique_ptr<std::byte[]>>>>
  Scan(  //
      const std::byte *begin_key,
      const bool begin_is_closed,
      const std::byte *end_key,
      const bool end_is_closed,
      Compare comp)
  {
    const auto status = GetStatusWord();
    std::map<std::unique_ptr<std::byte[]>, std::unique_ptr<std::byte[]>,
             UniquePtrComparator<Compare>>
        sorted_records;
    /*----------------------------------------------------------------------------------------------
     * Scan unsorted region
     *--------------------------------------------------------------------------------------------*/
    const auto sorted_count = GetSortedCount();
    for (size_t index = status.GetRecordCount() - 1; index >= sorted_count; --index) {
      const auto meta = GetMetadata(index);
      if (IsInRange(GetKeyPtr(meta), begin_key, begin_is_closed, end_key, end_is_closed, comp)) {
        if (meta.IsVisible()) {
          sorted_records.try_emplace(GetCopiedKey(meta), GetCopiedPayload(meta));
        } else if (meta.IsDeleted()) {
          sorted_records.try_emplace(GetCopiedKey(meta), nullptr);
        }
        // there is a key, but it is in inserting or corrupted
      }
    }

    /*----------------------------------------------------------------------------------------------
     * Scan sorted region
     *--------------------------------------------------------------------------------------------*/
    NodeReturnCode return_code = NodeReturnCode::kScanInProgress;
    const auto index_in_sorted = SearchSortedMetadata(begin_key, begin_is_closed, comp).second;
    for (size_t index = index_in_sorted; index < sorted_count; ++index) {
      const auto meta = GetMetadata(index);
      if (IsInRange(GetKeyPtr(meta), begin_key, begin_is_closed, end_key, end_is_closed, comp)) {
        if (meta.IsVisible()) {
          sorted_records.try_emplace(GetCopiedKey(meta), GetCopiedPayload(meta));
        } else {
          // there is no inserting nor corrupted record in a sorted region
          sorted_records.try_emplace(GetCopiedKey(meta), nullptr);
        }
      } else {
        // a current key is out of range condition
        return_code = NodeReturnCode::kSuccess;
        break;
      }
    }

    // convert a sorted record map into a vector
    std::vector<std::pair<std::unique_ptr<std::byte[]>, std::unique_ptr<std::byte[]>>> scan_results;
    RemoveDeletedRecords(sorted_records);
    scan_results.reserve(sorted_records.size());
    scan_results.insert(scan_results.end(), sorted_records.begin(), sorted_records.end());

    return {return_code, scan_results};
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
   * @param pmwcas_pool
   * @return NodeReturnCode
   */
  NodeReturnCode
  Write(  //
      const std::byte *key,
      const size_t key_length,
      const std::byte *payload,
      const size_t payload_length,
      const size_t index_epoch,
      const size_t block_size_threshold,
      const size_t deleted_size_threshold,
      pmwcas::DescriptorPool *pmwcas_pool)
  {
    // variables and constants shared in Phase 1 & 2
    StatusWord new_status;
    size_t record_count;
    const auto total_length = key_length + payload_length;
    const auto inserting_meta = kInitMetadata.InitForInsert(index_epoch);
    pmwcas::Descriptor *pd;

    /*----------------------------------------------------------------------------------------------
     * Phase 1: reserve free space to write a record
     *--------------------------------------------------------------------------------------------*/
    do {
      const auto current_status = GetStatusWordProtected();
      if (current_status.IsFrozen()) {
        return NodeReturnCode::kFrozen;
      }

      // prepare for MwCAS
      record_count = current_status.GetRecordCount();
      new_status = current_status.AddRecordInfo(1, total_length, 0);
      const auto current_meta = GetMetadata(record_count);

      // perform MwCAS to reserve space
      pd = pmwcas_pool->AllocateDescriptor();
      SetStatusForMwCAS(current_status, new_status, pd);
      SetMetadataForMwCAS(record_count, current_meta, inserting_meta, pd);
    } while (!pd->MwCAS());

    /*----------------------------------------------------------------------------------------------
     * Phase 2: write a record and check conflicts
     *--------------------------------------------------------------------------------------------*/

    // insert a record
    auto offset = GetNodeSize() - new_status.GetBlockSize();
    offset = CopyRecord(key, key_length, payload, payload_length, offset);

    // prepare record metadata for MwCAS
    const auto inserted_meta = inserting_meta.SetRecordInfo(offset, key_length, total_length);

    // check conflicts (concurrent SMOs)
    do {
      new_status = GetStatusWordProtected();
      if (new_status.IsFrozen()) {
        return NodeReturnCode::kFrozen;
      }

      // perform MwCAS to complete a write
      pd = pmwcas_pool->AllocateDescriptor();
      SetStatusForMwCAS(new_status, new_status, pd);
      SetMetadataForMwCAS(record_count, inserting_meta, inserted_meta, pd);
    } while (!pd->MwCAS());

    return CheckRemainingCapacity(new_status, block_size_threshold, deleted_size_threshold);
  }

  /**
   * @brief
   *
   * @tparam Compare
   * @param key
   * @param key_length
   * @param payload
   * @param payload_length
   * @param comp
   * @param pmwcas_pool
   * @return BaseNode::NodeReturnCode
   */
  template <class Compare>
  BaseNode::NodeReturnCode
  Insert(  //
      const std::byte *key,
      const size_t key_length,
      const std::byte *payload,
      const size_t payload_length,
      const size_t index_epoch,
      const size_t block_size_threshold,
      const size_t deleted_size_threshold,
      Compare comp,
      pmwcas::DescriptorPool *pmwcas_pool)
  {
    // variables and constants shared in Phase 1 & 2
    StatusWord new_status;
    size_t record_count;
    const auto total_length = key_length + payload_length;
    const auto inserting_meta = kInitMetadata.InitForInsert(index_epoch);
    pmwcas::Descriptor *pd;
    bool cas_failed;

    // local flags for insertion
    auto uniqueness = KeyExistence::kNotExist;

    /*----------------------------------------------------------------------------------------------
     * Phase 1: reserve free space to insert a record
     *--------------------------------------------------------------------------------------------*/
    do {
      const auto current_status = GetStatusWordProtected();
      if (current_status.IsFrozen()) {
        return NodeReturnCode::kFrozen;
      }

      record_count = current_status.GetRecordCount();
      if (uniqueness != KeyExistence::kUncertain) {
        uniqueness = CheckUniqueness(key, comp, record_count, index_epoch);
        if (uniqueness == KeyExistence::kExist) {
          return NodeReturnCode::kKeyExist;
        }
      }

      // prepare new status for MwCAS
      new_status = current_status.AddRecordInfo(1, total_length, 0);

      // get current metadata for MwCAS
      const auto current_meta = GetMetadata(record_count);

      // perform MwCAS to reserve space
      pd = pmwcas_pool->AllocateDescriptor();
      SetStatusForMwCAS(current_status, new_status, pd);
      SetMetadataForMwCAS(record_count, current_meta, inserting_meta, pd);
      cas_failed = !pd->MwCAS();

      if (cas_failed) {
        uniqueness = KeyExistence::kUncertain;
      }
    } while (cas_failed);

    /*----------------------------------------------------------------------------------------------
     * Phase 2: insert a record and check conflicts
     *--------------------------------------------------------------------------------------------*/

    // insert a record
    auto offset = GetNodeSize() - new_status.GetBlockSize();
    offset = CopyRecord(key, key_length, payload, payload_length, offset);

    // prepare record metadata for MwCAS
    const auto inserted_meta = inserting_meta.SetRecordInfo(offset, key_length, total_length);

    // check conflicts (concurrent inserts and SMOs)
    do {
      if (uniqueness == KeyExistence::kUncertain) {
        uniqueness = CheckUniqueness(key, comp, record_count, index_epoch);
        if (uniqueness == KeyExistence::kExist) {
          // delete an inserted record
          SetMetadata(record_count, inserting_meta.UpdateOffset(0));
          return NodeReturnCode::kKeyExist;
        }
        continue;  // recheck
      }

      new_status = GetStatusWordProtected();
      if (new_status.IsFrozen()) {
        return NodeReturnCode::kFrozen;
      }

      // perform MwCAS to complete an insert
      pd = pmwcas_pool->AllocateDescriptor();
      SetStatusForMwCAS(new_status, new_status, pd);
      SetMetadataForMwCAS(record_count, inserting_meta, inserted_meta, pd);
    } while (!pd->MwCAS());

    return CheckRemainingCapacity(new_status, block_size_threshold, deleted_size_threshold);
  }

  /**
   * @brief
   *
   * @tparam Compare
   * @param key
   * @param key_length
   * @param payload
   * @param payload_length
   * @param comp
   * @param pmwcas_pool
   * @return BaseNode::NodeReturnCode
   */
  template <class Compare>
  BaseNode::NodeReturnCode
  Update(  //
      const std::byte *key,
      const size_t key_length,
      const std::byte *payload,
      const size_t payload_length,
      const size_t index_epoch,
      const size_t block_size_threshold,
      const size_t deleted_size_threshold,
      Compare comp,
      pmwcas::DescriptorPool *pmwcas_pool)
  {
    // variables and constants shared in Phase 1 & 2
    StatusWord new_status;
    size_t record_count;
    const auto total_length = key_length + payload_length;
    const auto inserting_meta = kInitMetadata.InitForInsert(index_epoch);
    pmwcas::Descriptor *pd;

    /*----------------------------------------------------------------------------------------------
     * Phase 1: reserve free space to insert a record
     *--------------------------------------------------------------------------------------------*/
    do {
      const auto current_status = GetStatusWordProtected();
      if (current_status.IsFrozen()) {
        return NodeReturnCode::kFrozen;
      }

      record_count = current_status.GetRecordCount();
      const auto existence = SearchMetadataToRead(key, comp, record_count).first;
      if (existence == KeyExistence::kNotExist) {
        return NodeReturnCode::kKeyNotExist;
      }

      // prepare new status for MwCAS
      new_status = current_status.AddRecordInfo(1, total_length, 0);

      // get current metadata for MwCAS
      const auto current_meta = GetMetadata(record_count);

      // perform MwCAS to reserve space
      pd = pmwcas_pool->AllocateDescriptor();
      SetStatusForMwCAS(current_status, new_status, pd);
      SetMetadataForMwCAS(record_count, current_meta, inserting_meta, pd);
    } while (!pd->MwCAS());

    /*----------------------------------------------------------------------------------------------
     * Phase 2: insert a record and check conflicts
     *--------------------------------------------------------------------------------------------*/

    // insert a record
    auto offset = GetNodeSize() - new_status.GetBlockSize();
    offset = CopyRecord(key, key_length, payload, payload_length, offset);

    // prepare record metadata for MwCAS
    const auto inserted_meta = inserting_meta.SetRecordInfo(offset, key_length, total_length);

    // check conflicts (concurrent SMOs)
    do {
      new_status = GetStatusWordProtected();
      if (new_status.IsFrozen()) {
        return NodeReturnCode::kFrozen;
      }

      // perform MwCAS to complete an update
      pd = pmwcas_pool->AllocateDescriptor();
      SetStatusForMwCAS(new_status, new_status, pd);
      SetMetadataForMwCAS(record_count, inserting_meta, inserted_meta, pd);
    } while (!pd->MwCAS());

    return CheckRemainingCapacity(new_status, block_size_threshold, deleted_size_threshold);
  }

  /**
   * @brief
   *
   * @tparam Compare
   * @param key
   * @param key_length
   * @param comp
   * @param pmwcas_pool
   * @return BaseNode::NodeReturnCode
   */
  template <class Compare>
  BaseNode::NodeReturnCode
  Delete(  //
      const std::byte *key,
      const size_t key_length,
      const size_t block_size_threshold,
      const size_t deleted_size_threshold,
      Compare comp,
      pmwcas::DescriptorPool *pmwcas_pool)
  {
    // variables and constants
    pmwcas::Descriptor *pd;
    StatusWord new_status;

    do {
      const auto current_status = GetStatusWordProtected();
      if (current_status.IsFrozen()) {
        return NodeReturnCode::kFrozen;
      }

      const auto record_count = current_status.GetRecordCount();
      const auto existence = SearchMetadataToRead(key, comp, record_count).first;
      if (existence == KeyExistence::kNotExist) {
        return NodeReturnCode::kKeyNotExist;
      }

      // delete payload infomation from metadata
      const auto current_meta = GetMetadata(record_count);
      const auto deleted_meta = current_meta.DeleteRecordInfo();

      // prepare new status
      const auto total_length = key_length + current_meta.GetPayloadLength();
      new_status = current_status.AddRecordInfo(0, 0, total_length);

      // perform MwCAS to reserve space
      pd = pmwcas_pool->AllocateDescriptor();
      SetStatusForMwCAS(current_status, new_status, pd);
      SetMetadataForMwCAS(record_count, current_meta, deleted_meta, pd);
    } while (!pd->MwCAS());

    return CheckRemainingCapacity(new_status, block_size_threshold, deleted_size_threshold);
  }

  /*################################################################################################
   * Public structure modification operations
   *##############################################################################################*/

  template <class Compare>
  std::tuple<NodeReturnCode, LeafNode *, std::map<std::byte *, uint64_t, Compare>, size_t>
  Consolidate(  //
      const size_t free_space_length,
      const size_t min_node_length,
      Compare comp,
      pmwcas::DescriptorPool *pmwcas_pool)
  {
    assert(IsFrozen());  // a consolidating node must be locked

    /*----------------------------------------------------------------------------------------------
     * This node is now locked, so we can safely perform modification.
     *--------------------------------------------------------------------------------------------*/
    auto [meta_pairs, new_block_length] = GatherAndSortLiveMetadata(comp);

    // if there is no sufficient space for a new node, invoke a node split.
    const auto max_node_length = GetNodeSize();
    const auto new_record_count = meta_pairs.size();
    const auto new_occupied_length =
        kHeaderLength + (kWordLength * new_record_count) + new_block_length;
    if ((new_occupied_length + free_space_length) > max_node_length) {
      return {NodeReturnCode::kSplitRequired, nullptr, meta_pairs, 0};
    } else if (new_occupied_length < min_node_length) {
      return {NodeReturnCode::kMergeRequired, nullptr, meta_pairs, new_occupied_length};
    }

    // create a new node and copy records
    auto new_node = CreateEmptyNode(max_node_length);
    new_node->CopyRecordsViaMetadata(this, meta_pairs.begin(), new_record_count);

    return {NodeReturnCode::kSuccess, new_node, nullptr, 0};
  }

  template <class Compare>
  std::pair<BaseNode *, BaseNode *>
  Split(  //
      const std::map<const std::byte *, uint64_t, Compare> &sorted_meta,
      const size_t left_record_count)
  {
    const auto node_size = GetNodeSize();

    // create a split left node
    auto left_node = CreateEmptyNode(node_size);
    auto meta_iter = sorted_meta.begin();
    meta_iter = left_node->CopyRecordsViaMetadata(this, meta_iter, left_record_count);

    // create a split right node
    auto right_node = CreateEmptyNode(node_size);
    const auto right_record_count = sorted_meta.size() - left_record_count;
    meta_iter = right_node->CopyRecordsViaMetadata(this, meta_iter, right_record_count);

    // all the records must be copied
    assert(meta_iter == sorted_meta.end());

    return {reinterpret_cast<BaseNode *>(left_node), reinterpret_cast<BaseNode *>(right_node)};
  }

  template <class Compare>
  BaseNode *
  Merge(  //
      const std::map<const std::byte *, uint64_t, Compare> &sorted_meta,
      LeafNode *sibling_node,
      const bool sibling_is_left,
      Compare comp)
  {
    const auto node_size = GetNodeSize();

    // create a merged node
    auto merged_node = CreateEmptyNode(node_size);
    auto sibling_meta = sibling_node->GatherAndSortLiveMetadata(comp).first;
    if (sibling_is_left) {
      merged_node->CopyRecordsViaMetadata(sibling_node, sibling_meta.begin(), sibling_meta.size());
      merged_node->CopyRecordsViaMetadata(this, sorted_meta.begin(), sorted_meta.size());

    } else {
      merged_node->CopyRecordsViaMetadata(this, sorted_meta.begin(), sorted_meta.size());
      merged_node->CopyRecordsViaMetadata(sibling_node, sibling_meta.begin(), sibling_meta.size());
    }

    return reinterpret_cast<BaseNode *>(merged_node);
  }
};

}  // namespace bztree
