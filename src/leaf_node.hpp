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
   * Internal structs to cpmare key & metadata pairs
   *##############################################################################################*/

  template <class Compare>
  struct PairComp {
    Compare comp;

    explicit PairComp(Compare comparator) : comp{comparator} {}

    bool
    operator()(  //
        std::pair<void *, Metadata> a,
        std::pair<void *, Metadata> b) const noexcept
    {
      return comp(a.first, b.first);
    }
  };

  template <class Compare>
  struct PairEqual {
    Compare comp;

    explicit PairEqual(Compare comparator) : comp{comparator} {}

    bool
    operator()(  //
        std::pair<void *, Metadata> a,
        std::pair<void *, Metadata> b) const noexcept
    {
      return IsEqual(a.first, b.first, comp);
    }
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
    const auto key_ptr = GetKeyPtr(meta);
    const auto key_length = meta.GetKeyLength();
    auto copied_key_ptr = std::make_unique<std::byte[]>(key_length);
    memcpy(copied_key_ptr.get(), key_ptr, key_length);
    return copied_key_ptr;
  }

  std::unique_ptr<std::byte[]>
  GetCopiedPayload(const Metadata meta)
  {
    const auto payload_ptr = GetPayloadPtr(meta);
    const auto payload_length = meta.GetPayloadLength();
    auto copied_payload_ptr = std::make_unique<std::byte[]>(payload_length);
    memcpy(copied_payload_ptr.get(), payload_ptr, payload_length);
    return copied_payload_ptr;
  }

  /*################################################################################################
   * Internal utility functions
   *##############################################################################################*/

  template <class Compare>
  KeyExistence
  SearchUnsortedMetaToWrite(  //
      const void *key,
      const int64_t begin_index,
      const int64_t sorted_count,
      const size_t index_epoch,
      Compare comp)
  {
    // perform a linear search in revese order
    for (int64_t index = begin_index; index >= sorted_count; --index) {
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
      const void *key,
      const int64_t record_count,
      const size_t index_epoch,
      Compare comp)
  {
    const auto existence =
        SearchUnsortedMetaToWrite(key, record_count - 1, GetSortedCount(), index_epoch, comp);
    if (existence == KeyExistence::kNotExist) {
      // there is no key in unsorted metadata, so search a sorted region
      return SearchSortedMetadata(key, true, comp).first;
    } else {
      return existence;
    }
  }

  template <class Compare>
  std::pair<KeyExistence, size_t>
  SearchUnsortedMetaToRead(  //
      const void *key,
      const int64_t end_index,
      const int64_t record_count,
      Compare comp)
  {
    for (int64_t index = record_count - 1; index >= end_index; --index) {
      const auto meta = GetMetadata(index);
      if (IsEqual(key, GetKeyPtr(meta), comp)) {
        if (meta.IsVisible()) {
          return {KeyExistence::kExist, index};
        } else if (meta.IsDeleted()) {
          return {KeyExistence::kDeleted, index};
        }
        // there is a key, but it is in inserting or corrupted
      }
    }
    return {KeyExistence::kNotExist, 0};
  }

  template <class Compare>
  std::pair<KeyExistence, size_t>
  SearchMetadataToRead(  //
      const void *key,
      const size_t record_count,
      Compare comp)
  {
    const auto [existence, index] =
        SearchUnsortedMetaToRead(key, GetSortedCount(), record_count, comp);
    if (existence == KeyExistence::kExist || existence == KeyExistence::kDeleted) {
      return {existence, index};
    } else {
      return SearchSortedMetadata(key, true, comp);
    }
  }

  std::vector<std::pair<void *, Metadata>>::const_iterator
  CopyRecordsViaMetadata(  //
      LeafNode *original_node,
      std::vector<std::pair<void *, Metadata>>::const_iterator meta_iter,
      const size_t record_count)
  {
    auto offset = GetNodeSize();
    for (size_t index = 0; index < record_count; ++index) {
      const auto meta = meta_iter->second;
      // copy a record
      const auto key = meta_iter->first;
      const auto key_length = meta.GetKeyLength();
      const auto payload = original_node->GetPayloadPtr(meta);
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
   * @return std::pair<NodeReturnCode, std::unique_ptr<std::byte[]>>
   */
  template <class Compare>
  std::pair<NodeReturnCode, std::unique_ptr<std::byte[]>>
  Read(  //
      const void *key,
      Compare comp)
  {
    const auto status = GetStatusWord();
    const auto [existence, index] = SearchMetadataToRead(key, status.GetRecordCount(), comp);
    if (existence == KeyExistence::kNotExist || existence == KeyExistence::kDeleted) {
      return {NodeReturnCode::kKeyNotExist, nullptr};
    } else {
      const auto meta = GetMetadata(index);
      return {NodeReturnCode::kSuccess, GetCopiedPayload(meta)};
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
   * @return std::pair<NodeReturnCode,
   *         std::vector<std::pair<std::unique_ptr<std::byte[]>, std::unique_ptr<std::byte[]>>>>
   */
  template <class Compare>
  std::pair<NodeReturnCode,
            std::vector<std::pair<std::unique_ptr<std::byte[]>, std::unique_ptr<std::byte[]>>>>
  Scan(  //
      const void *begin_key,
      const bool begin_is_closed,
      const void *end_key,
      const bool end_is_closed,
      Compare comp)
  {
    const auto status = GetStatusWord();
    const auto record_count = GetStatusWord().GetRecordCount();
    const auto sorted_count = GetSortedCount();

    // gather valid (live or deleted) records
    std::vector<std::pair<void *, Metadata>> meta_arr;
    meta_arr.reserve(record_count);

    // search unsorted metadata in reverse order
    for (size_t index = record_count - 1; index >= sorted_count; --index) {
      const auto meta = GetMetadata(index);
      if (IsInRange(GetKeyPtr(meta), begin_key, begin_is_closed, end_key, end_is_closed, comp)
          && (meta.IsVisible() || meta.IsDeleted())) {
        meta_arr.emplace_back(GetKeyPtr(meta), meta);
      } else {
        // there is a key, but it is in inserting or corrupted.
      }
    }

    // search sorted metadata
    auto return_code = NodeReturnCode::kScanInProgress;
    const auto begin_index = SearchSortedMetadata(begin_key, begin_is_closed, comp).second;
    for (size_t index = begin_index; index < sorted_count; ++index) {
      const auto meta = GetMetadata(index);
      if (IsInRange(GetKeyPtr(meta), begin_key, begin_is_closed, end_key, end_is_closed, comp)) {
        meta_arr.emplace_back(GetKeyPtr(meta), meta);
      } else {
        // a current key is out of range condition
        return_code = NodeReturnCode::kSuccess;
        break;
      }
    }

    // make unique with keeping the order of writes
    std::stable_sort(meta_arr.begin(), meta_arr.end(), PairComp{comp});
    auto end_iter = std::unique(meta_arr.begin(), meta_arr.end(), PairEqual{comp});
    meta_arr.erase(end_iter, meta_arr.end());

    // copy live records for return
    std::vector<std::pair<std::unique_ptr<std::byte[]>, std::unique_ptr<std::byte[]>>> scan_results;
    scan_results.reserve(meta_arr.size());
    for (auto &&[key, meta] : meta_arr) {
      if (meta.IsVisible()) {
        scan_results.emplace_back(GetCopiedKey(meta), GetCopiedPayload(meta));
      }
    }

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
  std::pair<NodeReturnCode, StatusWord>
  Write(  //
      const void *key,
      const size_t key_length,
      const void *payload,
      const size_t payload_length,
      const size_t index_epoch,
      pmwcas::DescriptorPool *pmwcas_pool)
  {
    // variables and constants shared in Phase 1 & 2
    StatusWord current_status;
    size_t record_count;
    const auto total_length = key_length + payload_length;
    const auto inserting_meta = kInitMetadata.InitForInsert(index_epoch);
    pmwcas::Descriptor *pd;

    /*----------------------------------------------------------------------------------------------
     * Phase 1: reserve free space to write a record
     *--------------------------------------------------------------------------------------------*/
    do {
      current_status = GetStatusWordProtected();
      if (current_status.IsFrozen()) {
        return {NodeReturnCode::kFrozen, kInitStatusWord};
      }

      if (current_status.GetOccupiedSize() + kWordLength + total_length > GetNodeSize()) {
        return {NodeReturnCode::kNoSpace, kInitStatusWord};
      }

      // prepare for MwCAS
      record_count = current_status.GetRecordCount();
      const auto new_status = current_status.AddRecordInfo(1, total_length, 0);
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
    auto offset = GetNodeSize() - current_status.GetBlockSize();
    offset = CopyRecord(key, key_length, payload, payload_length, offset);

    // prepare record metadata for MwCAS
    const auto inserted_meta = inserting_meta.SetRecordInfo(offset, key_length, total_length);

    // check conflicts (concurrent SMOs)
    do {
      current_status = GetStatusWordProtected();
      if (current_status.IsFrozen()) {
        return {NodeReturnCode::kFrozen, kInitStatusWord};
      }

      // perform MwCAS to complete a write
      pd = pmwcas_pool->AllocateDescriptor();
      SetStatusForMwCAS(current_status, current_status, pd);
      SetMetadataForMwCAS(record_count, inserting_meta, inserted_meta, pd);
    } while (!pd->MwCAS());

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
   * @param comp
   * @param pmwcas_pool
   * @return NodeReturnCode
   */
  template <class Compare>
  std::pair<NodeReturnCode, StatusWord>
  Insert(  //
      const void *key,
      const size_t key_length,
      const void *payload,
      const size_t payload_length,
      const size_t index_epoch,
      Compare comp,
      pmwcas::DescriptorPool *pmwcas_pool)
  {
    // variables and constants shared in Phase 1 & 2
    StatusWord current_status;
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
      current_status = GetStatusWordProtected();
      if (current_status.IsFrozen()) {
        return {NodeReturnCode::kFrozen, kInitStatusWord};
      }

      record_count = current_status.GetRecordCount();
      if (uniqueness != KeyExistence::kUncertain) {
        uniqueness = CheckUniqueness(key, record_count, index_epoch, comp);
        if (uniqueness == KeyExistence::kExist) {
          return {NodeReturnCode::kKeyExist, kInitStatusWord};
        }
      }

      if (current_status.GetOccupiedSize() + kWordLength + total_length > GetNodeSize()) {
        return {NodeReturnCode::kNoSpace, kInitStatusWord};
      }

      // prepare new status for MwCAS
      const auto new_status = current_status.AddRecordInfo(1, total_length, 0);

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
    auto offset = GetNodeSize() - current_status.GetBlockSize();
    offset = CopyRecord(key, key_length, payload, payload_length, offset);

    // prepare record metadata for MwCAS
    const auto inserted_meta = inserting_meta.SetRecordInfo(offset, key_length, total_length);

    // check conflicts (concurrent inserts and SMOs)
    do {
      if (uniqueness == KeyExistence::kUncertain) {
        uniqueness = CheckUniqueness(key, record_count, index_epoch, comp);
        if (uniqueness == KeyExistence::kExist) {
          // delete an inserted record
          SetMetadata(record_count, inserting_meta.UpdateOffset(0));
          return {NodeReturnCode::kKeyExist, kInitStatusWord};
        }
        continue;  // recheck
      }

      current_status = GetStatusWordProtected();
      if (current_status.IsFrozen()) {
        return {NodeReturnCode::kFrozen, kInitStatusWord};
      }

      // perform MwCAS to complete an insert
      pd = pmwcas_pool->AllocateDescriptor();
      SetStatusForMwCAS(current_status, current_status, pd);
      SetMetadataForMwCAS(record_count, inserting_meta, inserted_meta, pd);
    } while (!pd->MwCAS());

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
   * @param comp
   * @param pmwcas_pool
   * @return NodeReturnCode
   */
  template <class Compare>
  std::pair<NodeReturnCode, StatusWord>
  Update(  //
      const void *key,
      const size_t key_length,
      const void *payload,
      const size_t payload_length,
      const size_t index_epoch,
      Compare comp,
      pmwcas::DescriptorPool *pmwcas_pool)
  {
    // variables and constants shared in Phase 1 & 2
    StatusWord current_status;
    size_t record_count;
    const auto total_length = key_length + payload_length;
    const auto inserting_meta = kInitMetadata.InitForInsert(index_epoch);
    pmwcas::Descriptor *pd;

    /*----------------------------------------------------------------------------------------------
     * Phase 1: reserve free space to insert a record
     *--------------------------------------------------------------------------------------------*/
    do {
      current_status = GetStatusWordProtected();
      if (current_status.IsFrozen()) {
        return {NodeReturnCode::kFrozen, kInitStatusWord};
      }

      record_count = current_status.GetRecordCount();
      const auto existence = SearchMetadataToRead(key, record_count, comp).first;
      if (existence == KeyExistence::kNotExist || existence == KeyExistence::kDeleted) {
        return {NodeReturnCode::kKeyNotExist, kInitStatusWord};
      }

      if (current_status.GetOccupiedSize() + kWordLength + total_length > GetNodeSize()) {
        return {NodeReturnCode::kNoSpace, kInitStatusWord};
      }

      // prepare new status for MwCAS
      const auto new_status = current_status.AddRecordInfo(1, total_length, 0);

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
    auto offset = GetNodeSize() - current_status.GetBlockSize();
    offset = CopyRecord(key, key_length, payload, payload_length, offset);

    // prepare record metadata for MwCAS
    const auto inserted_meta = inserting_meta.SetRecordInfo(offset, key_length, total_length);

    // check conflicts (concurrent SMOs)
    do {
      current_status = GetStatusWordProtected();
      if (current_status.IsFrozen()) {
        return {NodeReturnCode::kFrozen, kInitStatusWord};
      }

      // perform MwCAS to complete an update
      pd = pmwcas_pool->AllocateDescriptor();
      SetStatusForMwCAS(current_status, current_status, pd);
      SetMetadataForMwCAS(record_count, inserting_meta, inserted_meta, pd);
    } while (!pd->MwCAS());

    return {NodeReturnCode::kSuccess, current_status};
  }

  /**
   * @brief
   *
   * @tparam Compare
   * @param key
   * @param key_length
   * @param comp
   * @param pmwcas_pool
   * @return NodeReturnCode
   */
  template <class Compare>
  std::pair<NodeReturnCode, StatusWord>
  Delete(  //
      const void *key,
      const size_t key_length,
      Compare comp,
      pmwcas::DescriptorPool *pmwcas_pool)
  {
    // variables and constants
    pmwcas::Descriptor *pd;
    StatusWord new_status;

    do {
      const auto current_status = GetStatusWordProtected();
      if (current_status.IsFrozen()) {
        return {NodeReturnCode::kFrozen, kInitStatusWord};
      }

      const auto record_count = current_status.GetRecordCount();
      const auto [existence, index] = SearchMetadataToRead(key, record_count, comp);
      if (existence == KeyExistence::kNotExist || existence == KeyExistence::kDeleted) {
        return {NodeReturnCode::kKeyNotExist, kInitStatusWord};
      }

      // delete payload infomation from metadata
      const auto current_meta = GetMetadata(index);
      const auto deleted_meta = current_meta.DeleteRecordInfo();

      // prepare new status
      const auto total_length = key_length + current_meta.GetPayloadLength();
      new_status = current_status.AddRecordInfo(0, 0, total_length);

      // perform MwCAS to reserve space
      pd = pmwcas_pool->AllocateDescriptor();
      SetStatusForMwCAS(current_status, new_status, pd);
      SetMetadataForMwCAS(index, current_meta, deleted_meta, pd);
    } while (!pd->MwCAS());

    return {NodeReturnCode::kSuccess, new_status};
  }

  /*################################################################################################
   * Public structure modification operations
   *##############################################################################################*/

  LeafNode *
  Consolidate(const std::vector<std::pair<void *, Metadata>> &live_meta)
  {
    // create a new node and copy records
    auto new_node = CreateEmptyNode(GetNodeSize());
    new_node->CopyRecordsViaMetadata(this, live_meta.begin(), live_meta.size());

    return new_node;
  }

  std::pair<BaseNode *, BaseNode *>
  Split(  //
      const std::vector<std::pair<void *, Metadata>> &sorted_meta,
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

    return {dynamic_cast<BaseNode *>(left_node), dynamic_cast<BaseNode *>(right_node)};
  }

  BaseNode *
  Merge(  //
      const std::vector<std::pair<void *, Metadata>> &this_meta,
      LeafNode *sibling_node,
      const std::vector<std::pair<void *, Metadata>> &sibling_meta,
      const bool sibling_is_left)
  {
    // create a merged node
    auto merged_node = CreateEmptyNode(GetNodeSize());
    if (sibling_is_left) {
      merged_node->CopyRecordsViaMetadata(sibling_node, sibling_meta.begin(), sibling_meta.size());
      merged_node->CopyRecordsViaMetadata(this, this_meta.begin(), this_meta.size());
    } else {
      merged_node->CopyRecordsViaMetadata(this, this_meta.begin(), this_meta.size());
      merged_node->CopyRecordsViaMetadata(sibling_node, sibling_meta.begin(), sibling_meta.size());
    }

    return dynamic_cast<BaseNode *>(merged_node);
  }

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  template <class Compare>
  std::vector<std::pair<void *, Metadata>>
  GatherSortedLiveMetadata(Compare comp)
  {
    const auto record_count = GetStatusWord().GetRecordCount();
    const int64_t sorted_count = GetSortedCount();

    // gather valid (live or deleted) records
    std::vector<std::pair<void *, Metadata>> meta_arr;
    meta_arr.reserve(record_count);

    // search unsorted metadata in reverse order
    for (int64_t index = record_count - 1; index >= sorted_count; --index) {
      const auto meta = GetMetadata(index);
      if (meta.IsVisible() || meta.IsDeleted()) {
        meta_arr.emplace_back(GetKeyPtr(meta), meta);
      } else {
        // there is a key, but it is in inserting or corrupted.
        // NOTE: we can ignore inserting records because concurrent writes are aborted due to SMOs.
      }
    }

    // search sorted metadata
    for (int64_t index = 0; index < sorted_count; ++index) {
      const auto meta = GetMetadata(index);
      meta_arr.emplace_back(GetKeyPtr(meta), meta);
    }

    // make unique with keeping the order of writes
    std::stable_sort(meta_arr.begin(), meta_arr.end(), PairComp{comp});
    auto end_iter = std::unique(meta_arr.begin(), meta_arr.end(), PairEqual{comp});

    // gather live records
    end_iter = std::remove_if(meta_arr.begin(), end_iter,
                              [](auto &obj) { return obj.second.IsDeleted(); });
    meta_arr.erase(end_iter, meta_arr.end());

    return meta_arr;
  }
};

}  // namespace bztree
