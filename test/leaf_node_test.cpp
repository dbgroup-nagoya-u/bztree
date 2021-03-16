// Copyright (c) DB Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include "bztree/components/leaf_node.hpp"

#include <gtest/gtest.h>

#include <memory>

using std::byte;

namespace dbgroup::index::bztree
{
using Key = uint64_t;
using Payload = uint64_t;
using NodeReturnCode = BaseNode<Key, Payload>::NodeReturnCode;
using Record_t = Record<Key, Payload>;

static constexpr size_t kNodeSize = 256;
static constexpr size_t kIndexEpoch = 0;
static constexpr size_t kKeyNumForTest = 10000;
static constexpr size_t kKeyLength = sizeof(Key);
static constexpr size_t kPayloadLength = sizeof(Payload);
static constexpr size_t kRecordLength = kKeyLength + kPayloadLength;

/*##################################################################################################
 * Unsigned int 64 bits unit tests
 *################################################################################################*/

class LeafNodeUInt64Fixture : public testing::Test
{
 public:
  Key keys[kKeyNumForTest];
  Payload payloads[kKeyNumForTest];
  Key key_null = 0;          // null key must have 8 bytes to fill a node
  Payload payload_null = 0;  // null payload must have 8 bytes to fill a node

  std::unique_ptr<LeafNode<Key, Payload>> node;
  size_t record_count = 0;
  size_t index = 0;
  size_t block_size = 0;
  size_t deleted_size = 0;

  NodeReturnCode rc;
  StatusWord status;
  std::unique_ptr<Record_t> record;

 protected:
  void
  SetUp() override
  {
    node.reset(LeafNode<Key, Payload>::CreateEmptyNode(kNodeSize));
    record_count = 0;
    index = 0;
    block_size = 0;
    deleted_size = 0;

    for (size_t index = 0; index < kKeyNumForTest; index++) {
      keys[index] = index + 1;
      payloads[index] = index + 1;
    }
  }

  void
  TearDown() override
  {
  }

  void
  WriteNullKey(const size_t write_num)
  {
    for (size_t index = 0; index < write_num; ++index) {
      node->Write(key_null, kKeyLength, payload_null, kPayloadLength);
      ++record_count;
      block_size += kRecordLength;
    }
  }

  std::vector<Key>
  WriteOrderedKeys(  //
      const size_t begin_index,
      const size_t end_index)
  {
    assert(end_index < kKeyNumForTest);

    std::vector<Key> written_keys;
    for (size_t index = begin_index; index <= end_index; ++index) {
      auto key = keys[index];
      auto payload = payloads[index];
      node->Write(key, kKeyLength, payload, kPayloadLength);

      written_keys.emplace_back(key);
      ++record_count;
      block_size += kRecordLength;
    }
    return written_keys;
  }

  void
  VerifyWrittenMetadata(const Metadata meta)
  {
    EXPECT_TRUE(meta.IsVisible());
    EXPECT_FALSE(meta.IsDeleted());
    EXPECT_EQ(kKeyLength, meta.GetKeyLength());
    EXPECT_EQ(kPayloadLength, meta.GetPayloadLength());
  }

  void
  VerifyUnfrozenStatusWord(const StatusWord status)
  {
    EXPECT_EQ(status, node->GetStatusWord());
    EXPECT_FALSE(status.IsFrozen());
    EXPECT_EQ(record_count, status.GetRecordCount());
    EXPECT_EQ(block_size, status.GetBlockSize());
    EXPECT_EQ(deleted_size, status.GetDeletedSize());
  }
};

TEST_F(LeafNodeUInt64Fixture, New_EmptyNode_CorrectlyInitialized)
{
  auto status = *BitCast<StatusWord*>(ShiftAddress(node.get(), kWordLength));

  EXPECT_EQ(kNodeSize, node->GetNodeSize());
  EXPECT_EQ(0, node->GetSortedCount());
  EXPECT_EQ(status, node->GetStatusWord());
}

// /*--------------------------------------------------------------------------------------------------
//  * Read operation
//  *------------------------------------------------------------------------------------------------*/

// TEST_F(LeafNodeUInt64Fixture, Read_NotPresentKey_ReadFailed)
// {
//   std::tie(rc, record) = node->Read(keys[1]);

//   EXPECT_EQ(NodeReturnCode::kKeyNotExist, rc);
// }

// /*--------------------------------------------------------------------------------------------------
//  * Scan operation
//  *------------------------------------------------------------------------------------------------*/

// TEST_F(LeafNodeUInt64Fixture, Scan_EmptyNode_NoResult)
// {
//   auto [rc, scan_results] = node->Scan(keys[1], true, keys[10], true);

//   EXPECT_EQ(NodeReturnCode::kSuccess, rc);
//   EXPECT_EQ(0, scan_results.size());
// }

// TEST_F(LeafNodeUInt64Fixture, Scan_BothClosed_ScanTargetValues)
// {
//   WriteOrderedKeys(5, 10);
//   WriteOrderedKeys(1, 4);

//   auto [rc, scan_results] = node->Scan(keys[4], true, keys[6], true);

//   EXPECT_EQ(NodeReturnCode::kSuccess, rc);
//   EXPECT_EQ(3, scan_results.size());
//   EXPECT_EQ(keys[4], CastToValue(scan_results[0].first.get()));
//   EXPECT_EQ(payloads[4], CastToValue(scan_results[0].second.get()));
//   EXPECT_EQ(keys[5], CastToValue(scan_results[1].first.get()));
//   EXPECT_EQ(payloads[5], CastToValue(scan_results[1].second.get()));
//   EXPECT_EQ(keys[6], CastToValue(scan_results[2].first.get()));
//   EXPECT_EQ(payloads[6], CastToValue(scan_results[2].second.get()));
// }

// TEST_F(LeafNodeUInt64Fixture, Scan_LeftClosed_ScanTargetValues)
// {
//   WriteOrderedKeys(5, 10);
//   WriteOrderedKeys(1, 4);

//   auto [rc, scan_results] = node->Scan(keys[8], true, keys[10], false);

//   EXPECT_EQ(NodeReturnCode::kSuccess, rc);
//   EXPECT_EQ(2, scan_results.size());
//   EXPECT_EQ(keys[8], CastToValue(scan_results[0].first.get()));
//   EXPECT_EQ(payloads[8], CastToValue(scan_results[0].second.get()));
//   EXPECT_EQ(keys[9], CastToValue(scan_results[1].first.get()));
//   EXPECT_EQ(payloads[9], CastToValue(scan_results[1].second.get()));
// }

// TEST_F(LeafNodeUInt64Fixture, Scan_RightClosed_ScanTargetValues)
// {
//   WriteOrderedKeys(5, 10);
//   WriteOrderedKeys(1, 4);

//   auto [rc, scan_results] = node->Scan(keys[8], false, keys[10], true);

//   EXPECT_EQ(NodeReturnCode::kSuccess, rc);
//   EXPECT_EQ(2, scan_results.size());
//   EXPECT_EQ(keys[9], CastToValue(scan_results[0].first.get()));
//   EXPECT_EQ(payloads[9], CastToValue(scan_results[0].second.get()));
//   EXPECT_EQ(keys[10], CastToValue(scan_results[1].first.get()));
//   EXPECT_EQ(payloads[10], CastToValue(scan_results[1].second.get()));
// }

// TEST_F(LeafNodeUInt64Fixture, Scan_BothOpened_ScanTargetValues)
// {
//   WriteOrderedKeys(5, 10);
//   WriteOrderedKeys(1, 4);

//   auto [rc, scan_results] = node->Scan(keys[8], false, keys[10], false);

//   EXPECT_EQ(NodeReturnCode::kSuccess, rc);
//   EXPECT_EQ(1, scan_results.size());
//   EXPECT_EQ(keys[9], CastToValue(scan_results[0].first.get()));
//   EXPECT_EQ(payloads[9], CastToValue(scan_results[0].second.get()));
// }

// TEST_F(LeafNodeUInt64Fixture, Scan_LeftInfinity_ScanTargetValues)
// {
//   WriteOrderedKeys(5, 10);
//   WriteOrderedKeys(1, 4);

//   auto [rc, scan_results] = node->Scan(nullptr, false, keys[2], true);

//   EXPECT_EQ(NodeReturnCode::kSuccess, rc);
//   EXPECT_EQ(2, scan_results.size());
//   EXPECT_EQ(keys[1], CastToValue(scan_results[0].first.get()));
//   EXPECT_EQ(payloads[1], CastToValue(scan_results[0].second.get()));
//   EXPECT_EQ(keys[2], CastToValue(scan_results[1].first.get()));
//   EXPECT_EQ(payloads[2], CastToValue(scan_results[1].second.get()));
// }

// TEST_F(LeafNodeUInt64Fixture, Scan_RightInfinity_ScanTargetValues)
// {
//   WriteOrderedKeys(5, 10);
//   WriteOrderedKeys(1, 4);

//   auto [rc, scan_results] = node->Scan(keys[9], true, nullptr, false);

//   EXPECT_EQ(NodeReturnCode::kScanInProgress, rc);
//   EXPECT_EQ(2, scan_results.size());
//   EXPECT_EQ(keys[9], CastToValue(scan_results[0].first.get()));
//   EXPECT_EQ(payloads[9], CastToValue(scan_results[0].second.get()));
//   EXPECT_EQ(keys[10], CastToValue(scan_results[1].first.get()));
//   EXPECT_EQ(payloads[10], CastToValue(scan_results[1].second.get()));
// }

// TEST_F(LeafNodeUInt64Fixture, Scan_LeftOutsideRange_NoResults)
// {
//   WriteOrderedKeys(5, 10);

//   auto [rc, scan_results] = node->Scan(nullptr, false, keys[3], false);

//   EXPECT_EQ(NodeReturnCode::kSuccess, rc);
//   EXPECT_EQ(0, scan_results.size());
// }

// TEST_F(LeafNodeUInt64Fixture, Scan_RightOutsideRange_NoResults)
// {
//   WriteOrderedKeys(1, 4);

//   auto [rc, scan_results] = node->Scan(keys[5], false, nullptr, false);

//   EXPECT_EQ(NodeReturnCode::kSuccess, rc);
//   EXPECT_EQ(0, scan_results.size());
// }

// TEST_F(LeafNodeUInt64Fixture, Scan_WithUpdateDelete_ScanLatestValues)
// {
//   WriteOrderedKeys(1, 5);
//   node->Update(keys[2], kKeyLength, payloads[0], payload_lengths[0]); node->Delete(keys[3],
//   key_lengths[3]);

//   auto [rc, scan_results] = node->Scan(keys[2], true, keys[4], true);

//   EXPECT_EQ(NodeReturnCode::kSuccess, rc);
//   EXPECT_EQ(2, scan_results.size());
//   EXPECT_EQ(keys[2], CastToValue(scan_results[0].first.get()));
//   EXPECT_EQ(payloads[0], CastToValue(scan_results[0].second.get()));
//   EXPECT_EQ(keys[4], CastToValue(scan_results[1].first.get()));
//   EXPECT_EQ(payloads[4], CastToValue(scan_results[1].second.get()));
// }

// TEST_F(LeafNodeUInt64Fixture, Scan_ConsolidatedNodeWithinRange_ScanTargetValues)
// {
//   // prepare a consolidated node
//   WriteOrderedKeys(3, 7);
//   auto meta_vec = node->GatherSortedLiveMetadata(comp);
//   node.reset(LeafNode::Consolidate(node.get(), meta_vec));

//   auto [rc, scan_results] = node->Scan(keys[4], true, keys[6], true);

//   EXPECT_EQ(NodeReturnCode::kSuccess, rc);
//   EXPECT_EQ(3, scan_results.size());
//   EXPECT_EQ(keys[4], CastToValue(scan_results[0].first.get()));
//   EXPECT_EQ(payloads[4], CastToValue(scan_results[0].second.get()));
//   EXPECT_EQ(keys[5], CastToValue(scan_results[1].first.get()));
//   EXPECT_EQ(payloads[5], CastToValue(scan_results[1].second.get()));
//   EXPECT_EQ(keys[6], CastToValue(scan_results[2].first.get()));
//   EXPECT_EQ(payloads[6], CastToValue(scan_results[2].second.get()));
// }

// TEST_F(LeafNodeUInt64Fixture, Scan_ConsolidatedNodeWithLeftInfinity_ScanTargetValues)
// {
//   // prepare a consolidated node
//   WriteOrderedKeys(3, 7);
//   auto meta_vec = node->GatherSortedLiveMetadata(comp);
//   node.reset(LeafNode::Consolidate(node.get(), meta_vec));

//   auto [rc, scan_results] = node->Scan(nullptr, true, keys[3], true);

//   EXPECT_EQ(NodeReturnCode::kSuccess, rc);
//   EXPECT_EQ(1, scan_results.size());
//   EXPECT_EQ(keys[3], CastToValue(scan_results[0].first.get()));
//   EXPECT_EQ(payloads[3], CastToValue(scan_results[0].second.get()));
// }

// TEST_F(LeafNodeUInt64Fixture, Scan_ConsolidatedNodeWithRightInfinity_ScanTargetValues)
// {
//   // prepare a consolidated node
//   WriteOrderedKeys(3, 7);
//   auto meta_vec = node->GatherSortedLiveMetadata(comp);
//   node.reset(LeafNode::Consolidate(node.get(), meta_vec));

//   auto [rc, scan_results] = node->Scan(keys[7], true, nullptr, true);

//   EXPECT_EQ(NodeReturnCode::kScanInProgress, rc);
//   EXPECT_EQ(1, scan_results.size());
//   EXPECT_EQ(keys[7], CastToValue(scan_results[0].first.get()));
//   EXPECT_EQ(payloads[7], CastToValue(scan_results[0].second.get()));
// }

// TEST_F(LeafNodeUInt64Fixture, Scan_ConsolidatedNodeWithUpdateDelete_ScanLatestValues)
// {
//   // prepare a consolidated node
//   WriteOrderedKeys(3, 7);
//   auto meta_vec = node->GatherSortedLiveMetadata(comp);
//   node.reset(LeafNode::Consolidate(node.get(), meta_vec));
//   node->Update(keys[5], key_lengths[5], payloads[0], payload_lengths[0]); node->Delete(keys[7],
//   key_lengths[7]);

//   auto [rc, scan_results] = node->Scan(keys[5], true, keys[7], true);

//   EXPECT_EQ(NodeReturnCode::kSuccess, rc);
//   EXPECT_EQ(2, scan_results.size());
//   EXPECT_EQ(keys[5], CastToValue(scan_results[0].first.get()));
//   EXPECT_EQ(payloads[0], CastToValue(scan_results[0].second.get()));
//   EXPECT_EQ(keys[6], CastToValue(scan_results[1].first.get()));
//   EXPECT_EQ(payloads[6], CastToValue(scan_results[1].second.get()));
// }

/*--------------------------------------------------------------------------------------------------
 * Write operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(LeafNodeUInt64Fixture, Write_TwoKeys_MetadataCorrectlyUpdated)
{
  std::tie(rc, status) = node->Write(keys[1], kKeyLength, payloads[1], kPayloadLength);
  record_count += 1;
  block_size += kRecordLength;

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  VerifyWrittenMetadata(node->GetMetadata(index++));
  VerifyUnfrozenStatusWord(status);

  std::tie(rc, status) = node->Write(keys[2], kKeyLength, payloads[2], kPayloadLength);
  record_count += 1;
  block_size += kRecordLength;

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  VerifyWrittenMetadata(node->GetMetadata(index++));
  VerifyUnfrozenStatusWord(status);
}

TEST_F(LeafNodeUInt64Fixture, Write_TwoKeys_ReadWrittenValues)
{
  node->Write(keys[1], kKeyLength, payloads[1], kPayloadLength);
  node->Write(keys[2], kKeyLength, payloads[2], kPayloadLength);

  // read 1st input value
  std::tie(rc, record) = node->Read(keys[1]);

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(payloads[1], record->GetPayload());

  // read 2nd input value
  std::tie(rc, record) = node->Read(keys[2]);

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(payloads[2], record->GetPayload());
}

TEST_F(LeafNodeUInt64Fixture, Write_DuplicateKey_ReadLatestValue)
{
  node->Write(keys[1], kKeyLength, payloads[1], kPayloadLength);
  node->Write(keys[1], kKeyLength, payloads[2], kPayloadLength);

  std::tie(rc, record) = node->Read(keys[1]);

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(payloads[2], record->GetPayload());
}

TEST_F(LeafNodeUInt64Fixture, Write_FilledNode_GetCorrectReturnCodes)
{
  WriteNullKey(9);

  std::tie(rc, status) = node->Write(keys[1], kKeyLength, payloads[1], kPayloadLength);

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);

  std::tie(rc, status) = node->Write(keys[1], kKeyLength, payloads[1], kPayloadLength);

  EXPECT_EQ(NodeReturnCode::kNoSpace, rc);
}

// TEST_F(LeafNodeUInt64Fixture, Write_ConsolidatedNode_MetadataCorrectlyUpdated)
// {
//   // prepare a consolidated node
//   WriteOrderedKeys(1, 5);
//   auto meta_vec = node->GatherSortedLiveMetadata(comp);
//   node.reset(LeafNode::Consolidate(node.get(), meta_vec));

//   std::tie(rc, status)  = node->Write(keys[11], key_lengths[11], payloads[11],
//                                   payload_lengths[11]);
//   ++record_count;
//   block_size += key_lengths[11] + payload_lengths[11];
//   index = record_count - 1;
//   auto meta = node->GetMetadata(index);

//   EXPECT_EQ(NodeReturnCode::kSuccess, rc);
//   EXPECT_EQ(status, node->GetStatusWord());
//   EXPECT_TRUE(meta.IsVisible());
//   EXPECT_FALSE(meta.IsDeleted());
//   EXPECT_FALSE(status.IsFrozen());
//   EXPECT_EQ(record_count, status.GetRecordCount());
//   EXPECT_EQ(block_size, status.GetBlockSize());
//   EXPECT_EQ(deleted_size, status.GetDeletedSize());
// }

// TEST_F(LeafNodeUInt64Fixture, Write_ConsolidatedNode_ReadWrittenValue)
// {
//   // prepare a consolidated node
//   WriteOrderedKeys(1, 5);
//   auto meta_vec = node->GatherSortedLiveMetadata(comp);
//   node.reset(LeafNode::Consolidate(node.get(), meta_vec));

//   node->Write(keys[11], key_lengths[11], payloads[11], payload_lengths[11]);
//   std::tie(rc, record) = node->Read(keys[11]);

//   EXPECT_EQ(NodeReturnCode::kSuccess, rc);
//   EXPECT_EQ(payloads[11], record->GetPayload());
// }

/*--------------------------------------------------------------------------------------------------
 * Insert operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(LeafNodeUInt64Fixture, Insert_TwoKeys_MetadataCorrectlyUpdated)
{
  std::tie(rc, status) = node->Insert(keys[1], kKeyLength, payloads[1], kPayloadLength);
  record_count += 1;
  block_size += kRecordLength;

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  VerifyWrittenMetadata(node->GetMetadata(index++));
  VerifyUnfrozenStatusWord(status);

  std::tie(rc, status) = node->Insert(keys[2], kKeyLength, payloads[2], kPayloadLength);
  record_count += 1;
  block_size += kRecordLength;

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  VerifyWrittenMetadata(node->GetMetadata(index++));
  VerifyUnfrozenStatusWord(status);
}

TEST_F(LeafNodeUInt64Fixture, Insert_TwoKeys_ReadInsertedValues)
{
  node->Insert(keys[1], kKeyLength, payloads[1], kPayloadLength);
  node->Insert(keys[2], kKeyLength, payloads[2], kPayloadLength);

  // read 1st input value
  std::tie(rc, record) = node->Read(keys[1]);

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(payloads[1], record->GetPayload());

  // read 2nd input value
  std::tie(rc, record) = node->Read(keys[2]);

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(payloads[2], record->GetPayload());
}

TEST_F(LeafNodeUInt64Fixture, Insert_DuplicateKey_InsertionFailed)
{
  node->Insert(keys[1], kKeyLength, payloads[1], kPayloadLength);

  std::tie(rc, status) = node->Insert(keys[1], kKeyLength, payloads[1], kPayloadLength);

  EXPECT_EQ(NodeReturnCode::kKeyExist, rc);
}

TEST_F(LeafNodeUInt64Fixture, Insert_FilledNode_GetCorrectReturnCodes)
{
  WriteNullKey(9);

  // fill a node
  std::tie(rc, status) = node->Insert(keys[1], kKeyLength, payloads[1], kPayloadLength);

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(kNodeSize, status.GetOccupiedSize());

  // insert a filled node with a not present key
  std::tie(rc, status) = node->Insert(keys[2], kKeyLength, payloads[2], kPayloadLength);

  EXPECT_EQ(NodeReturnCode::kNoSpace, rc);

  // insert a filled node with an present key
  std::tie(rc, status) = node->Insert(keys[1], kKeyLength, payloads[1], kPayloadLength);

  EXPECT_EQ(NodeReturnCode::kKeyExist, rc);
}

// TEST_F(LeafNodeUInt64Fixture, Insert_ConsolidatedNode_MetadataCorrectlyUpdated)
// {
//   // prepare a consolidated node
//   WriteOrderedKeys(1, 5);
//   auto meta_vec = node->GatherSortedLiveMetadata(comp);
//   node.reset(LeafNode::Consolidate(node.get(), meta_vec));

//   std::tie(rc, status)  = node->Insert(keys[11], key_lengths[11], payloads[11],
//                                    payload_lengths[11]);
//   ++record_count;
//   block_size += key_lengths[11] + payload_lengths[11];
//   index = record_count - 1;
//   auto meta = node->GetMetadata(index);

//   EXPECT_EQ(NodeReturnCode::kSuccess, rc);
//   EXPECT_EQ(status, node->GetStatusWord());
//   EXPECT_TRUE(meta.IsVisible());
//   EXPECT_FALSE(meta.IsDeleted());
//   EXPECT_FALSE(status.IsFrozen());
//   EXPECT_EQ(record_count, status.GetRecordCount());
//   EXPECT_EQ(block_size, status.GetBlockSize());
//   EXPECT_EQ(deleted_size, status.GetDeletedSize());
// }

// TEST_F(LeafNodeUInt64Fixture, Insert_ConsolidatedNode_ReadInsertedValue)
// {
//   // prepare a consolidated node
//   WriteOrderedKeys(1, 5);
//   auto meta_vec = node->GatherSortedLiveMetadata(comp);
//   node.reset(LeafNode::Consolidate(node.get(), meta_vec));

//   node->Insert(keys[11], key_lengths[11], payloads[11], payload_lengths[11]);
//   std::tie(rc, record) = node->Read(keys[11]);

//   EXPECT_EQ(NodeReturnCode::kSuccess, rc);
//   EXPECT_EQ(payloads[11], record->GetPayload());
// }

// TEST_F(LeafNodeUInt64Fixture, Insert_ConsolidatedNodeWithDuplicateKey_InsertionFailed)
// {
//   // prepare a consolidated node
//   WriteOrderedKeys(1, 5);
//   auto meta_vec = node->GatherSortedLiveMetadata(comp);
//   node.reset(LeafNode::Consolidate(node.get(), meta_vec));

//   std::tie(rc, status)  = node->Insert(keys[1], kKeyLength, payloads[1],
//   kPayloadLength);

//   EXPECT_EQ(NodeReturnCode::kKeyExist, rc);
// }

// /*--------------------------------------------------------------------------------------------------
//  * Update operation
//  *------------------------------------------------------------------------------------------------*/

// TEST_F(LeafNodeUInt64Fixture, Update_SingleKey_MetadataCorrectlyUpdated)
// {
//   node->Insert(keys[1], kKeyLength, payloads[1], kPayloadLength); record_count = 1; index = 0;
//   block_size = sizeof(Key) + sizeof(Payload); deleted_size = kWordLength + sizeof(Key) +
//   sizeof(Payload);

//   std::tie(rc, status)  = node->Update(keys[1], kKeyLength, payloads[2],
//   kPayloadLength);
//   ++record_count;
//   ++index;
//   block_size += sizeof(Key) + sizeof(Payload);
//   auto meta = node->GetMetadata(index);

//   EXPECT_EQ(NodeReturnCode::kSuccess, rc);
//   EXPECT_EQ(status, node->GetStatusWord());
//   EXPECT_TRUE(meta.IsVisible());
//   EXPECT_FALSE(meta.IsDeleted());
//   EXPECT_EQ(kKeyLength, meta.GetKeyLength());
//   EXPECT_EQ(kPayloadLength, meta.GetPayloadLength());
//   EXPECT_FALSE(status.IsFrozen());
//   EXPECT_EQ(record_count, status.GetRecordCount());
//   EXPECT_EQ(block_size, status.GetBlockSize());
//   EXPECT_EQ(deleted_size, status.GetDeletedSize());
// }

// TEST_F(LeafNodeUInt64Fixture, Update_SingleKey_ReadUpdatedValue)
// {
//   node->Insert(keys[1], kKeyLength, payloads[2], kPayloadLength); node->Update(keys[1],
//   kKeyLength, payloads[2], kPayloadLength);

//   std::tie(rc, record) = node->Read(keys[1]);

//   EXPECT_EQ(NodeReturnCode::kSuccess, rc);
//   EXPECT_EQ(payloads[2], record->GetPayload());
// }

// TEST_F(LeafNodeUInt64Fixture, Update_NotPresentKey_UpdatedFailed)
// {
//   std::tie(rc, status)  = node->Update(keys[1], kKeyLength, payloads[1],
//   kPayloadLength);

//   EXPECT_EQ(NodeReturnCode::kKeyNotExist, rc);
// }

// TEST_F(LeafNodeUInt64Fixture, Update_DeletedKey_UpdateFailed)
// {
//   node->Insert(keys[1], kKeyLength, payloads[2], kPayloadLength); node->Delete(keys[1],
//   kKeyLength); std::tie(rc, status)  = node->Update(keys[1], kKeyLength, payloads[2],
//   kPayloadLength);

//   EXPECT_EQ(NodeReturnCode::kKeyNotExist, rc);
// }

// TEST_F(LeafNodeUInt64Fixture, Update_FilledNode_GetCorrectReturnCodes)
// {
//   WriteNullKey(9);

//   // fill a node
//   std::tie(rc, status)  = node->Update(key_null_ptr, key_length_null, payload_null_ptr,
//                                    payload_length_null);

//   EXPECT_EQ(NodeReturnCode::kSuccess, rc);
//   EXPECT_EQ(kNodeSize, status.GetOccupiedSize());

//   // update a filled node with an present key
//   std::tie(rc, status) = node->Update(key_null_ptr, key_length_null, payload_null_ptr,
//                                       payload_length_null);

//   EXPECT_EQ(NodeReturnCode::kNoSpace, rc);

//   // update a filled node with a not present key
//   std::tie(rc, status) = node->Update(keys[1], kKeyLength, payloads[1],
//                                       kPayloadLength);

//   EXPECT_EQ(NodeReturnCode::kKeyNotExist, rc);
// }

// TEST_F(LeafNodeUInt64Fixture, Update_ConsolidatedNode_MetadataCorrectlyUpdated)
// {
//   // prepare a consolidated node
//   WriteOrderedKeys(1, 5);
//   auto meta_vec = node->GatherSortedLiveMetadata(comp);
//   node.reset(LeafNode::Consolidate(node.get(), meta_vec));

//   std::tie(rc, status)  = node->Update(keys[1], kKeyLength, payloads[11],
//                                    payload_lengths[11]);
//   ++record_count;
//   block_size += sizeof(Key) + payload_lengths[11];
//   deleted_size = kWordLength + sizeof(Key) + sizeof(Payload);
//   index = record_count - 1;
//   auto meta = node->GetMetadata(index);

//   EXPECT_EQ(NodeReturnCode::kSuccess, rc);
//   EXPECT_EQ(status, node->GetStatusWord());
//   EXPECT_TRUE(meta.IsVisible());
//   EXPECT_FALSE(meta.IsDeleted());
//   EXPECT_FALSE(status.IsFrozen());
//   EXPECT_EQ(record_count, status.GetRecordCount());
//   EXPECT_EQ(block_size, status.GetBlockSize());
//   EXPECT_EQ(deleted_size, status.GetDeletedSize());
// }

// TEST_F(LeafNodeUInt64Fixture, Update_ConsolidatedNode_ReadUpdatedValue)
// {
//   // prepare a consolidated node
//   WriteOrderedKeys(1, 5);
//   auto meta_vec = node->GatherSortedLiveMetadata(comp);
//   node.reset(LeafNode::Consolidate(node.get(), meta_vec));

//   node->Update(keys[1], kKeyLength, payloads[11], payload_lengths[11]);
//   std::tie(rc, record) = node->Read(keys[1]);

//   EXPECT_EQ(NodeReturnCode::kSuccess, rc);
//   EXPECT_EQ(payloads[11], record->GetPayload());
// }

// TEST_F(LeafNodeUInt64Fixture, Update_ConsolidatedNodeWithNotPresentKey_UpdatedFailed)
// {
//   // prepare a consolidated node
//   WriteOrderedKeys(1, 5);
//   auto meta_vec = node->GatherSortedLiveMetadata(comp);
//   node.reset(LeafNode::Consolidate(node.get(), meta_vec));

//   std::tie(rc, status)  = node->Update(key_null_ptr, key_length_null, payload_null_ptr,
//                                    payload_length_null);

//   EXPECT_EQ(NodeReturnCode::kKeyNotExist, rc);
// }

// TEST_F(LeafNodeUInt64Fixture, Update_ConsolidatedNodeWithDeletedKey_UpdatedFailed)
// {
//   // prepare a consolidated node
//   WriteOrderedKeys(1, 5);
//   auto meta_vec = node->GatherSortedLiveMetadata(comp);
//   node.reset(LeafNode::Consolidate(node.get(), meta_vec));
//   node->Delete(keys[1], kKeyLength);

//   std::tie(rc, status)  = node->Update(keys[1], kKeyLength, payloads[1],
//   kPayloadLength);

//   EXPECT_EQ(NodeReturnCode::kKeyNotExist, rc);
// }

// /*--------------------------------------------------------------------------------------------------
//  * Delete operation
//  *------------------------------------------------------------------------------------------------*/

// TEST_F(LeafNodeUInt64Fixture, Delete_TwoKeys_MetadataCorrectlyUpdated)
// {
//   node->Insert(keys[1], kKeyLength, payloads[1], kPayloadLength); node->Insert(keys[2],
//   kKeyLength, payloads[2], kPayloadLength); record_count = 2; block_size =
//   sizeof(Key) + sizeof(Payload) + sizeof(Key) + sizeof(Payload);

//   std::tie(rc, status)  = node->Delete(keys[1], kKeyLength);
//   deleted_size = kWordLength + sizeof(Key) + sizeof(Payload);
//   auto first_meta = node->GetMetadata(0);

//   EXPECT_EQ(NodeReturnCode::kSuccess, rc);
//   EXPECT_EQ(status, node->GetStatusWord());
//   EXPECT_FALSE(first_meta.IsVisible());
//   EXPECT_TRUE(first_meta.IsDeleted());
//   EXPECT_FALSE(status.IsFrozen());
//   EXPECT_EQ(record_count, status.GetRecordCount());
//   EXPECT_EQ(block_size, status.GetBlockSize());
//   EXPECT_EQ(deleted_size, status.GetDeletedSize());

//   std::tie(rc, status) = node->Delete(keys[2], kKeyLength);
//   deleted_size += kWordLength + sizeof(Key) + sizeof(Payload);
//   auto second_meta = node->GetMetadata(1);

//   EXPECT_EQ(NodeReturnCode::kSuccess, rc);
//   EXPECT_EQ(status, node->GetStatusWord());
//   EXPECT_FALSE(second_meta.IsVisible());
//   EXPECT_TRUE(second_meta.IsDeleted());
//   EXPECT_FALSE(status.IsFrozen());
//   EXPECT_EQ(record_count, status.GetRecordCount());
//   EXPECT_EQ(block_size, status.GetBlockSize());
//   EXPECT_EQ(deleted_size, status.GetDeletedSize());
// }

// TEST_F(LeafNodeUInt64Fixture, Delete_PresentKey_DeletionSucceed)
// {
//   node->Insert(keys[1], kKeyLength, payloads[1], kPayloadLength);

//   std::tie(rc, status)  = node->Delete(keys[1], kKeyLength);

//   EXPECT_EQ(NodeReturnCode::kSuccess, rc);
// }

// TEST_F(LeafNodeUInt64Fixture, Delete_PresentKey_ReadFailed)
// {
//   node->Insert(keys[1], kKeyLength, payloads[1], kPayloadLength); node->Delete(keys[1],
//   kKeyLength);

//   std::tie(rc, record) = node->Read(keys[1]);

//   EXPECT_EQ(NodeReturnCode::kKeyNotExist, rc);
// }

// TEST_F(LeafNodeUInt64Fixture, Delete_NotPresentKey_DeletionFailed)
// {
//   std::tie(rc, status)  = node->Delete(keys[1], kKeyLength);

//   EXPECT_EQ(NodeReturnCode::kKeyNotExist, rc);
// }

// TEST_F(LeafNodeUInt64Fixture, Delete_DeletedKey_DeletionFailed)
// {
//   node->Insert(keys[1], kKeyLength, payloads[1], kPayloadLength); node->Delete(keys[1],
//   kKeyLength);

//   std::tie(rc, status)  = node->Delete(keys[1], kKeyLength);

//   EXPECT_EQ(NodeReturnCode::kKeyNotExist, rc);
// }

// TEST_F(LeafNodeUInt64Fixture, Delete_FilledNode_GetCorrectReturnCodes)
// {
//   WriteNullKey(10);

//   std::tie(rc, status)  = node->Delete(key_null_ptr, key_length_null);

//   EXPECT_EQ(NodeReturnCode::kSuccess, rc);
// }

// TEST_F(LeafNodeUInt64Fixture, Delete_ConsolidatedNode_MetadataCorrectlyUpdated)
// {
//   // prepare a consolidated node
//   WriteOrderedKeys(1, 5);
//   auto meta_vec = node->GatherSortedLiveMetadata(comp);
//   node.reset(LeafNode::Consolidate(node.get(), meta_vec));

//   std::tie(rc, status)  = node->Delete(keys[1], kKeyLength);
//   deleted_size = kWordLength + sizeof(Key) + sizeof(Payload);
//   index = record_count - 1;
//   auto meta = node->GetMetadata(index);

//   EXPECT_EQ(NodeReturnCode::kSuccess, rc);
//   EXPECT_EQ(status, node->GetStatusWord());
//   EXPECT_TRUE(meta.IsVisible());
//   EXPECT_FALSE(meta.IsDeleted());
//   EXPECT_FALSE(status.IsFrozen());
//   EXPECT_EQ(record_count, status.GetRecordCount());
//   EXPECT_EQ(block_size, status.GetBlockSize());
//   EXPECT_EQ(deleted_size, status.GetDeletedSize());
// }

// TEST_F(LeafNodeUInt64Fixture, Delete_ConsolidatedNode_DeletionSucceed)
// {
//   // prepare a consolidated node
//   WriteOrderedKeys(1, 5);
//   auto meta_vec = node->GatherSortedLiveMetadata(comp);
//   node.reset(LeafNode::Consolidate(node.get(), meta_vec));

//   std::tie(rc, status)  = node->Delete(keys[1], kKeyLength);

//   EXPECT_EQ(NodeReturnCode::kSuccess, rc);
// }

// TEST_F(LeafNodeUInt64Fixture, Delete_ConsolidatedNodeWithNotPresentKey_DeletionFailed)
// {
//   // prepare a consolidated node
//   WriteOrderedKeys(1, 5);
//   auto meta_vec = node->GatherSortedLiveMetadata(comp);
//   node.reset(LeafNode::Consolidate(node.get(), meta_vec));

//   std::tie(rc, status)  = node->Delete(key_null_ptr, key_length_null);

//   EXPECT_EQ(NodeReturnCode::kKeyNotExist, rc);
// }

// TEST_F(LeafNodeUInt64Fixture, Delete_ConsolidatedNodeWithDeletedKey_DeletionFailed)
// {
//   // prepare a consolidated node
//   WriteOrderedKeys(1, 5);
//   auto meta_vec = node->GatherSortedLiveMetadata(comp);
//   node.reset(LeafNode::Consolidate(node.get(), meta_vec));
//   node->Delete(keys[1], kKeyLength);

//   std::tie(rc, status)  = node->Delete(keys[1], kKeyLength);

//   EXPECT_EQ(NodeReturnCode::kKeyNotExist, rc);
// }

// /*--------------------------------------------------------------------------------------------------
//  * Consolide operation
//  *------------------------------------------------------------------------------------------------*/

// TEST_F(LeafNodeUInt64Fixture, Consolidate_SortedTenKeys_GatherSortedLiveMetadata)
// {
//   // fill a node with ordered keys
//   auto written_keys = WriteOrderedKeys(1, 10);

//   // gather live metadata and check equality
//   auto meta_vec = node->GatherSortedLiveMetadata(comp);

//   EXPECT_EQ(record_count, meta_vec.size());

//   for (size_t index = 0; index < meta_vec.size(); index++) {
//     EXPECT_EQ(written_keys[index], CastToValue(meta_vec[index].first));
//   }
// }

// TEST_F(LeafNodeUInt64Fixture, Consolidate_SortedTenKeysWithDelete_GatherSortedLiveMetadata)
// {
//   // fill a node with ordered keys
//   auto written_keys = WriteOrderedKeys(1, 10);
//   node->Delete(keys[2], kKeyLength);
//   written_keys.erase(++(written_keys.begin()));
//   --record_count;

//   // gather live metadata and check equality
//   auto meta_vec = node->GatherSortedLiveMetadata(comp);

//   EXPECT_EQ(record_count, meta_vec.size());

//   for (size_t index = 0; index < meta_vec.size(); index++) {
//     EXPECT_EQ(written_keys[index], CastToValue(meta_vec[index].first));
//   }
// }

// TEST_F(LeafNodeUInt64Fixture, Consolidate_SortedTenKeysWithUpdate_GatherSortedLiveMetadata)
// {
//   // fill a node with ordered keys
//   auto written_keys = WriteOrderedKeys(1, 9);
//   node->Update(keys[2], kKeyLength, payload_null_ptr, payload_length_null);

//   // gather live metadata and check equality
//   auto meta_vec = node->GatherSortedLiveMetadata(comp);

//   EXPECT_EQ(record_count, meta_vec.size());

//   for (size_t index = 0; index < meta_vec.size(); index++) {
//     EXPECT_EQ(written_keys[index], CastToValue(meta_vec[index].first));
//   }
// }

// TEST_F(LeafNodeUInt64Fixture, Consolidate_UnsortedTenKeys_GatherSortedLiveMetadata)
// {
//   // fill a node with ordered keys
//   auto tmp_keys = WriteOrderedKeys(5, 10);
//   auto written_keys = WriteOrderedKeys(1, 4);
//   written_keys.insert(written_keys.end(), tmp_keys.begin(), tmp_keys.end());

//   // gather live metadata and check equality
//   auto meta_vec = node->GatherSortedLiveMetadata(comp);

//   EXPECT_EQ(record_count, meta_vec.size());

//   for (size_t index = 0; index < meta_vec.size(); index++) {
//     EXPECT_EQ(written_keys[index], CastToValue(meta_vec[index].first));
//   }
// }

// TEST_F(LeafNodeUInt64Fixture, Consolidate_SortedTenKeys_NodeHasCorrectStatus)
// {
//   // prepare a consolidated node
//   WriteOrderedKeys(1, 10);
//   auto meta_vec = node->GatherSortedLiveMetadata(comp);
//   node.reset(LeafNode::Consolidate(node.get(), meta_vec));

//   auto status = node->GetStatusWord();

//   EXPECT_EQ(record_count, node->GetSortedCount());
//   EXPECT_FALSE(status.IsFrozen());
//   EXPECT_EQ(record_count, status.GetRecordCount());
//   EXPECT_EQ(block_size, status.GetBlockSize());
//   EXPECT_EQ(deleted_size, status.GetDeletedSize());
// }

// TEST_F(LeafNodeUInt64Fixture, Consolidate_SortedTenKeysWithDelete_NodeHasCorrectStatus)
// {
//   // prepare a consolidated node
//   WriteOrderedKeys(1, 10);
//   node->Delete(keys[2], kKeyLength);
//   auto meta_vec = node->GatherSortedLiveMetadata(comp);
//   node.reset(LeafNode::Consolidate(node.get(), meta_vec));

//   auto status = node->GetStatusWord();
//   --record_count;
//   block_size -= sizeof(Key) + sizeof(Payload);

//   EXPECT_EQ(record_count, node->GetSortedCount());
//   EXPECT_FALSE(status.IsFrozen());
//   EXPECT_EQ(record_count, status.GetRecordCount());
//   EXPECT_EQ(block_size, status.GetBlockSize());
//   EXPECT_EQ(deleted_size, status.GetDeletedSize());
// }

// TEST_F(LeafNodeUInt64Fixture, Consolidate_SortedTenKeysWithUpdate_NodeHasCorrectStatus)
// {
//   // prepare a consolidated node
//   WriteOrderedKeys(1, 9);
//   node->Update(keys[2], kKeyLength, payload_null_ptr, payload_length_null);
//   auto meta_vec = node->GatherSortedLiveMetadata(comp);
//   node.reset(LeafNode::Consolidate(node.get(), meta_vec));

//   auto status = node->GetStatusWord();

//   EXPECT_EQ(record_count, node->GetSortedCount());
//   EXPECT_FALSE(status.IsFrozen());
//   EXPECT_EQ(record_count, status.GetRecordCount());
//   EXPECT_EQ(block_size, status.GetBlockSize());
//   EXPECT_EQ(deleted_size, status.GetDeletedSize());
// }

// TEST_F(LeafNodeUInt64Fixture, Consolidate_UnsortedTenKeys_NodeHasCorrectStatus)
// {
//   // prepare a consolidated node
//   WriteOrderedKeys(5, 10);
//   WriteOrderedKeys(1, 4);
//   auto meta_vec = node->GatherSortedLiveMetadata(comp);
//   node.reset(LeafNode::Consolidate(node.get(), meta_vec));

//   auto status = node->GetStatusWord();

//   EXPECT_EQ(record_count, node->GetSortedCount());
//   EXPECT_FALSE(status.IsFrozen());
//   EXPECT_EQ(record_count, status.GetRecordCount());
//   EXPECT_EQ(block_size, status.GetBlockSize());
//   EXPECT_EQ(deleted_size, status.GetDeletedSize());
// }

// /*--------------------------------------------------------------------------------------------------
//  * Split operation
//  *------------------------------------------------------------------------------------------------*/

// TEST_F(LeafNodeUInt64Fixture, Split_EquallyDivided_NodesHaveCorrectStatus)
// {
//   // prepare split nodes
//   WriteOrderedKeys(1, 10);
//   auto left_record_count = 5;
//   auto meta_vec = node->GatherSortedLiveMetadata(comp);
//   auto [left_node, right_node] = LeafNode::Split(node.get(), meta_vec, left_record_count);

//   auto left_status = left_node->GetStatusWord();
//   auto left_kBlockSize = block_size / 2;
//   auto left_deleted_size = 0;

//   EXPECT_EQ(left_record_count, left_node->GetSortedCount());
//   EXPECT_FALSE(left_status.IsFrozen());
//   EXPECT_EQ(left_record_count, left_status.GetRecordCount());
//   EXPECT_EQ(left_kBlockSize, left_status.GetBlockSize());
//   EXPECT_EQ(left_deleted_size, left_status.GetDeletedSize());

//   auto right_status = right_node->GetStatusWord();
//   auto right_record_count = record_count - left_record_count;
//   auto right_kBlockSize = block_size / 2;
//   auto right_deleted_size = 0;

//   EXPECT_EQ(right_record_count, right_node->GetSortedCount());
//   EXPECT_FALSE(right_status.IsFrozen());
//   EXPECT_EQ(right_record_count, right_status.GetRecordCount());
//   EXPECT_EQ(right_kBlockSize, right_status.GetBlockSize());
//   EXPECT_EQ(right_deleted_size, right_status.GetDeletedSize());
// }

// TEST_F(LeafNodeUInt64Fixture, Split_EquallyDivided_NodesHaveCorrectKeyPayloads)
// {
//   // prepare split nodes
//   WriteOrderedKeys(1, 10);
//   const auto left_record_count = 5;
//   auto meta_vec = node->GatherSortedLiveMetadata(comp);
//   auto [left_node, right_node] = LeafNode::Split(node.get(), meta_vec, left_record_count);

//   // check a split left node
//   size_t index = 1;
//   for (; index <= left_record_count; ++index) {
//     std::tie(rc, record) = left_node->Read(keys[index]);

//     EXPECT_EQ(NodeReturnCode::kSuccess, rc);
//     EXPECT_EQ(payloads[index], CastToValue(record.get()));
//   }

//   // check a split right node
//   for (; index <= record_count; ++index) {
//     std::tie(rc, record) = right_node->Read(keys[index]);

//     EXPECT_EQ(NodeReturnCode::kSuccess, rc);
//     EXPECT_EQ(payloads[index], CastToValue(record.get()));
//   }
// }

// /*--------------------------------------------------------------------------------------------------
//  * Merge operation
//  *------------------------------------------------------------------------------------------------*/

// TEST_F(LeafNodeUInt64Fixture, Merge_LeftSiblingNode_NodeHasCorrectStatus)
// {
//   // prepare a merged node
//   WriteOrderedKeys(4, 6);
//   auto this_meta = node->GatherSortedLiveMetadata(comp);
//   auto sibling_node = std::unique_ptr<LeafNode>(LeafNode::CreateEmptyNode(kNodeSize));
//   sibling_node->Write(keys[3], key_lengths[3], payloads[3], payload_lengths[3]);
//   auto sibling_meta = sibling_node->GatherSortedLiveMetadata(comp);
//   auto merged_node = LeafNode::Merge(node.get(), this_meta, sibling_node.get(), sibling_meta,
//   true);

//   auto merged_status = merged_node->GetStatusWord();
//   auto merged_record_count = record_count + 1;
//   auto merged_kBlockSize = block_size + key_lengths[3] + payload_lengths[3];
//   auto merged_deleted_size = 0;

//   EXPECT_EQ(merged_record_count, merged_node->GetSortedCount());
//   EXPECT_FALSE(merged_status.IsFrozen());
//   EXPECT_EQ(merged_record_count, merged_status.GetRecordCount());
//   EXPECT_EQ(merged_kBlockSize, merged_status.GetBlockSize());
//   EXPECT_EQ(merged_deleted_size, merged_status.GetDeletedSize());
// }

// TEST_F(LeafNodeUInt64Fixture, Merge_RightSiblingNode_NodeHasCorrectStatus)
// {
//   // prepare a merged node
//   WriteOrderedKeys(4, 6);
//   auto this_meta = node->GatherSortedLiveMetadata(comp);
//   auto sibling_node = std::unique_ptr<LeafNode>(LeafNode::CreateEmptyNode(kNodeSize));
//   sibling_node->Write(keys[7], key_lengths[7], payloads[7], payload_lengths[7]);
//   auto sibling_meta = sibling_node->GatherSortedLiveMetadata(comp);
//   auto merged_node =
//       LeafNode::Merge(node.get(), this_meta, sibling_node.get(), sibling_meta, false);

//   auto merged_status = merged_node->GetStatusWord();
//   auto merged_record_count = record_count + 1;
//   auto merged_kBlockSize = block_size + key_lengths[7] + payload_lengths[7];
//   auto merged_deleted_size = 0;

//   EXPECT_EQ(merged_record_count, merged_node->GetSortedCount());
//   EXPECT_FALSE(merged_status.IsFrozen());
//   EXPECT_EQ(merged_record_count, merged_status.GetRecordCount());
//   EXPECT_EQ(merged_kBlockSize, merged_status.GetBlockSize());
//   EXPECT_EQ(merged_deleted_size, merged_status.GetDeletedSize());
// }

// TEST_F(LeafNodeUInt64Fixture, Merge_LeftSiblingNode_NodeHasCorrectKeyPayloads)
// {
//   // prepare a merged node
//   WriteOrderedKeys(4, 6);
//   auto this_meta = node->GatherSortedLiveMetadata(comp);
//   auto sibling_node = std::unique_ptr<LeafNode>(LeafNode::CreateEmptyNode(kNodeSize));
//   sibling_node->Write(keys[3], key_lengths[3], payloads[3], payload_lengths[3]);
//   auto sibling_meta = sibling_node->GatherSortedLiveMetadata(comp);
//   auto merged_node = LeafNode::Merge(node.get(), this_meta, sibling_node.get(), sibling_meta,
//   true);

//   // check keys and payloads
//   for (size_t index = 3; index <= 6; ++index) {
//     std::tie(rc, record) = merged_node->Read(keys[index]);

//     EXPECT_EQ(NodeReturnCode::kSuccess, rc);
//     EXPECT_EQ(payloads[index], CastToValue(record.get()));
//   }
// }

// TEST_F(LeafNodeUInt64Fixture, Merge_RightSiblingNode_NodeHasCorrectKeyPayloads)
// {
//   // prepare a merged node
//   WriteOrderedKeys(4, 6);
//   auto this_meta = node->GatherSortedLiveMetadata(comp);
//   auto sibling_node = std::unique_ptr<LeafNode>(LeafNode::CreateEmptyNode(kNodeSize));
//   sibling_node->Write(keys[7], key_lengths[7], payloads[7], payload_lengths[7]);
//   auto sibling_meta = sibling_node->GatherSortedLiveMetadata(comp);
//   auto merged_node =
//       LeafNode::Merge(node.get(), this_meta, sibling_node.get(), sibling_meta, false);

//   // check keys and payloads
//   for (size_t index = 4; index <= 7; ++index) {
//     std::tie(rc, record) = merged_node->Read(keys[index]);

//     EXPECT_EQ(NodeReturnCode::kSuccess, rc);
//     EXPECT_EQ(payloads[index], CastToValue(record.get()));
//   }
// }

}  // namespace dbgroup::index::bztree
