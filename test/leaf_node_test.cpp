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
using LeafNode_t = LeafNode<Key, Payload>;

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

  std::unique_ptr<LeafNode_t> node;
  size_t index;
  size_t expected_record_count;
  size_t expected_block_size;
  size_t expected_deleted_size;

  NodeReturnCode rc;
  StatusWord status;
  std::unique_ptr<Record_t> record;

 protected:
  void
  SetUp() override
  {
    node.reset(LeafNode_t::CreateEmptyNode(kNodeSize));
    index = 0;
    expected_record_count = 0;
    expected_block_size = 0;
    expected_deleted_size = 0;

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
      ++expected_record_count;
      expected_block_size += kRecordLength;
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
      ++expected_record_count;
      expected_block_size += kRecordLength;
    }
    return written_keys;
  }

  void
  VerifyMetadata(  //
      const Metadata meta,
      const bool record_is_visible = true)
  {
    if (record_is_visible) {
      EXPECT_TRUE(meta.IsVisible());
      EXPECT_FALSE(meta.IsDeleted());
    } else {
      EXPECT_FALSE(meta.IsVisible());
      EXPECT_TRUE(meta.IsDeleted());
    }
    EXPECT_EQ(kKeyLength, meta.GetKeyLength());
    EXPECT_EQ(kPayloadLength, meta.GetPayloadLength());
  }

  void
  VerifyStatusWord(  //
      const StatusWord status,
      const bool status_is_frozen = false)
  {
    EXPECT_EQ(status, node->GetStatusWord());
    if (status_is_frozen) {
      EXPECT_TRUE(status.IsFrozen());
    } else {
      EXPECT_FALSE(status.IsFrozen());
    }
    EXPECT_EQ(expected_record_count, status.GetRecordCount());
    EXPECT_EQ(expected_block_size, status.GetBlockSize());
    EXPECT_EQ(expected_deleted_size, status.GetDeletedSize());
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
//   WriteOrderedKeys(0, 4);
//   node->Update(keys[2], kKeyLength, payloads[0], payload_lengths[0]); node->Delete(keys[3],
//   kKeyLength);

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
//   auto meta_vec = node->GatherSortedLiveMetadata();
//   node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));

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
//   auto meta_vec = node->GatherSortedLiveMetadata();
//   node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));

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
//   auto meta_vec = node->GatherSortedLiveMetadata();
//   node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));

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
//   auto meta_vec = node->GatherSortedLiveMetadata();
//   node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));
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
  expected_record_count += 1;
  expected_block_size += kRecordLength;

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  VerifyMetadata(node->GetMetadata(index++));
  VerifyStatusWord(status);

  std::tie(rc, status) = node->Write(keys[2], kKeyLength, payloads[2], kPayloadLength);
  expected_record_count += 1;
  expected_block_size += kRecordLength;

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  VerifyMetadata(node->GetMetadata(index++));
  VerifyStatusWord(status);
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

TEST_F(LeafNodeUInt64Fixture, Write_ConsolidatedNode_MetadataCorrectlyUpdated)
{
  // prepare a consolidated node
  WriteOrderedKeys(0, 4);
  auto meta_vec = node->GatherSortedLiveMetadata();
  node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));

  std::tie(rc, status) = node->Write(keys[11], kKeyLength, payloads[11], kPayloadLength);
  expected_record_count += 1;
  expected_block_size += kRecordLength;

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  VerifyMetadata(node->GetMetadata(index++));
  VerifyStatusWord(status);
}

TEST_F(LeafNodeUInt64Fixture, Write_ConsolidatedNode_ReadWrittenValue)
{
  // prepare a consolidated node
  WriteOrderedKeys(0, 4);
  auto meta_vec = node->GatherSortedLiveMetadata();
  node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));

  node->Write(keys[11], kKeyLength, payloads[11], kPayloadLength);
  std::tie(rc, record) = node->Read(keys[11]);

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(payloads[11], record->GetPayload());
}

/*--------------------------------------------------------------------------------------------------
 * Insert operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(LeafNodeUInt64Fixture, Insert_TwoKeys_MetadataCorrectlyUpdated)
{
  std::tie(rc, status) = node->Insert(keys[1], kKeyLength, payloads[1], kPayloadLength);
  expected_record_count += 1;
  expected_block_size += kRecordLength;

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  VerifyMetadata(node->GetMetadata(index++));
  VerifyStatusWord(status);

  std::tie(rc, status) = node->Insert(keys[2], kKeyLength, payloads[2], kPayloadLength);
  expected_record_count += 1;
  expected_block_size += kRecordLength;

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  VerifyMetadata(node->GetMetadata(index++));
  VerifyStatusWord(status);
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

TEST_F(LeafNodeUInt64Fixture, Insert_ConsolidatedNode_MetadataCorrectlyUpdated)
{
  // prepare a consolidated node
  WriteOrderedKeys(0, 4);
  auto meta_vec = node->GatherSortedLiveMetadata();
  node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));

  std::tie(rc, status) = node->Insert(keys[11], kKeyLength, payloads[11], kPayloadLength);
  expected_record_count += 1;
  expected_block_size += kRecordLength;

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  VerifyMetadata(node->GetMetadata(index++));
  VerifyStatusWord(status);
}

TEST_F(LeafNodeUInt64Fixture, Insert_ConsolidatedNode_ReadInsertedValue)
{
  // prepare a consolidated node
  WriteOrderedKeys(0, 4);
  auto meta_vec = node->GatherSortedLiveMetadata();
  node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));

  node->Insert(keys[11], kKeyLength, payloads[11], kPayloadLength);
  std::tie(rc, record) = node->Read(keys[11]);

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(payloads[11], record->GetPayload());
}

TEST_F(LeafNodeUInt64Fixture, Insert_ConsolidatedNodeWithDuplicateKey_InsertionFailed)
{
  // prepare a consolidated node
  WriteOrderedKeys(0, 4);
  auto meta_vec = node->GatherSortedLiveMetadata();
  node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));

  std::tie(rc, status) = node->Insert(keys[1], kKeyLength, payloads[1], kPayloadLength);

  EXPECT_EQ(NodeReturnCode::kKeyExist, rc);
}

/*--------------------------------------------------------------------------------------------------
 * Update operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(LeafNodeUInt64Fixture, Update_SingleKey_MetadataCorrectlyUpdated)
{
  node->Insert(keys[1], kKeyLength, payloads[1], kPayloadLength);
  expected_record_count += 1;
  expected_block_size += kRecordLength;

  std::tie(rc, status) = node->Update(keys[1], kKeyLength, payloads[2], kPayloadLength);
  expected_record_count += 1;
  expected_block_size += kRecordLength;
  expected_deleted_size += kWordLength + kRecordLength;

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  VerifyMetadata(node->GetMetadata(index++));
  VerifyStatusWord(status);
}

TEST_F(LeafNodeUInt64Fixture, Update_SingleKey_ReadUpdatedValue)
{
  node->Insert(keys[1], kKeyLength, payloads[1], kPayloadLength);
  node->Update(keys[1], kKeyLength, payloads[2], kPayloadLength);

  std::tie(rc, record) = node->Read(keys[1]);

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(payloads[2], record->GetPayload());
}

TEST_F(LeafNodeUInt64Fixture, Update_NotPresentKey_UpdatedFailed)
{
  std::tie(rc, status) = node->Update(keys[1], kKeyLength, payloads[1], kPayloadLength);

  EXPECT_EQ(NodeReturnCode::kKeyNotExist, rc);
}

TEST_F(LeafNodeUInt64Fixture, Update_DeletedKey_UpdateFailed)
{
  node->Insert(keys[1], kKeyLength, payloads[2], kPayloadLength);
  node->Delete(keys[1], kKeyLength);

  std::tie(rc, status) = node->Update(keys[1], kKeyLength, payloads[2], kPayloadLength);

  EXPECT_EQ(NodeReturnCode::kKeyNotExist, rc);
}

TEST_F(LeafNodeUInt64Fixture, Update_FilledNode_GetCorrectReturnCodes)
{
  WriteNullKey(9);

  // fill a node
  std::tie(rc, status) = node->Update(key_null, kKeyLength, payload_null, kPayloadLength);

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(kNodeSize, status.GetOccupiedSize());

  // update a filled node with an present key
  std::tie(rc, status) = node->Update(key_null, kKeyLength, payload_null, kPayloadLength);

  EXPECT_EQ(NodeReturnCode::kNoSpace, rc);

  // update a filled node with a not present key
  std::tie(rc, status) = node->Update(keys[1], kKeyLength, payloads[1], kPayloadLength);

  EXPECT_EQ(NodeReturnCode::kKeyNotExist, rc);
}

TEST_F(LeafNodeUInt64Fixture, Update_ConsolidatedNode_MetadataCorrectlyUpdated)
{
  // prepare a consolidated node
  WriteOrderedKeys(0, 4);
  auto meta_vec = node->GatherSortedLiveMetadata();
  node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));

  std::tie(rc, status) = node->Update(keys[1], kKeyLength, payloads[11], kPayloadLength);
  expected_record_count += 1;
  expected_block_size += kRecordLength;
  expected_deleted_size += kWordLength + kRecordLength;

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  VerifyMetadata(node->GetMetadata(index++));
  VerifyStatusWord(status);
}

TEST_F(LeafNodeUInt64Fixture, Update_ConsolidatedNode_ReadUpdatedValue)
{
  // prepare a consolidated node
  WriteOrderedKeys(0, 4);
  auto meta_vec = node->GatherSortedLiveMetadata();
  node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));

  node->Update(keys[1], kKeyLength, payloads[11], kPayloadLength);
  std::tie(rc, record) = node->Read(keys[1]);

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(payloads[11], record->GetPayload());
}

TEST_F(LeafNodeUInt64Fixture, Update_ConsolidatedNodeWithNotPresentKey_UpdatedFailed)
{
  // prepare a consolidated node
  WriteOrderedKeys(0, 4);
  auto meta_vec = node->GatherSortedLiveMetadata();
  node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));

  std::tie(rc, status) = node->Update(key_null, kKeyLength, payload_null, kPayloadLength);

  EXPECT_EQ(NodeReturnCode::kKeyNotExist, rc);
}

TEST_F(LeafNodeUInt64Fixture, Update_ConsolidatedNodeWithDeletedKey_UpdatedFailed)
{
  // prepare a consolidated node
  WriteOrderedKeys(0, 4);
  auto meta_vec = node->GatherSortedLiveMetadata();
  node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));

  node->Delete(keys[1], kKeyLength);
  std::tie(rc, status) = node->Update(keys[1], kKeyLength, payloads[1], kPayloadLength);

  EXPECT_EQ(NodeReturnCode::kKeyNotExist, rc);
}

/*--------------------------------------------------------------------------------------------------
 * Delete operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(LeafNodeUInt64Fixture, Delete_TwoKeys_MetadataCorrectlyUpdated)
{
  constexpr bool kRecordIsVisible = false;

  node->Insert(keys[1], kKeyLength, payloads[1], kPayloadLength);
  node->Insert(keys[2], kKeyLength, payloads[2], kPayloadLength);
  expected_record_count += 2;
  expected_block_size += 2 * kRecordLength;

  std::tie(rc, status) = node->Delete(keys[1], kKeyLength);
  expected_deleted_size += kWordLength + kRecordLength;

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  VerifyMetadata(node->GetMetadata(index++), kRecordIsVisible);
  VerifyStatusWord(status);

  std::tie(rc, status) = node->Delete(keys[2], kKeyLength);
  expected_deleted_size += kWordLength + kRecordLength;

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  VerifyMetadata(node->GetMetadata(index++), kRecordIsVisible);
  VerifyStatusWord(status);
}

TEST_F(LeafNodeUInt64Fixture, Delete_PresentKey_DeletionSucceed)
{
  node->Insert(keys[1], kKeyLength, payloads[1], kPayloadLength);

  std::tie(rc, status) = node->Delete(keys[1], kKeyLength);

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
}

TEST_F(LeafNodeUInt64Fixture, Delete_PresentKey_ReadFailed)
{
  node->Insert(keys[1], kKeyLength, payloads[1], kPayloadLength);
  node->Delete(keys[1], kKeyLength);

  std::tie(rc, record) = node->Read(keys[1]);

  EXPECT_EQ(NodeReturnCode::kKeyNotExist, rc);
}

TEST_F(LeafNodeUInt64Fixture, Delete_NotPresentKey_DeletionFailed)
{
  std::tie(rc, status) = node->Delete(keys[1], kKeyLength);

  EXPECT_EQ(NodeReturnCode::kKeyNotExist, rc);
}

TEST_F(LeafNodeUInt64Fixture, Delete_DeletedKey_DeletionFailed)
{
  node->Insert(keys[1], kKeyLength, payloads[1], kPayloadLength);
  node->Delete(keys[1], kKeyLength);

  std::tie(rc, status) = node->Delete(keys[1], kKeyLength);

  EXPECT_EQ(NodeReturnCode::kKeyNotExist, rc);
}

TEST_F(LeafNodeUInt64Fixture, Delete_FilledNode_GetCorrectReturnCodes)
{
  WriteNullKey(10);

  std::tie(rc, status) = node->Delete(key_null, kKeyLength);

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);

  std::tie(rc, status) = node->Delete(key_null, kKeyLength);

  EXPECT_EQ(NodeReturnCode::kKeyNotExist, rc);
}

TEST_F(LeafNodeUInt64Fixture, Delete_ConsolidatedNode_MetadataCorrectlyUpdated)
{
  // prepare a consolidated node
  WriteOrderedKeys(0, 4);
  auto meta_vec = node->GatherSortedLiveMetadata();
  node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));

  std::tie(rc, status) = node->Delete(keys[1], kKeyLength);
  expected_deleted_size += kWordLength + kRecordLength;

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  VerifyMetadata(node->GetMetadata(index++));
  VerifyStatusWord(status);
}

TEST_F(LeafNodeUInt64Fixture, Delete_ConsolidatedNode_DeletionSucceed)
{
  // prepare a consolidated node
  WriteOrderedKeys(0, 4);
  auto meta_vec = node->GatherSortedLiveMetadata();
  node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));

  std::tie(rc, status) = node->Delete(keys[1], kKeyLength);

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
}

TEST_F(LeafNodeUInt64Fixture, Delete_ConsolidatedNodeWithNotPresentKey_DeletionFailed)
{
  // prepare a consolidated node
  WriteOrderedKeys(0, 4);
  auto meta_vec = node->GatherSortedLiveMetadata();
  node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));

  std::tie(rc, status) = node->Delete(key_null, kKeyLength);

  EXPECT_EQ(NodeReturnCode::kKeyNotExist, rc);
}

TEST_F(LeafNodeUInt64Fixture, Delete_ConsolidatedNodeWithDeletedKey_DeletionFailed)
{
  // prepare a consolidated node
  WriteOrderedKeys(0, 4);
  auto meta_vec = node->GatherSortedLiveMetadata();
  node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));

  node->Delete(keys[1], kKeyLength);
  std::tie(rc, status) = node->Delete(keys[1], kKeyLength);

  EXPECT_EQ(NodeReturnCode::kKeyNotExist, rc);
}

/*--------------------------------------------------------------------------------------------------
 * Consolide operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(LeafNodeUInt64Fixture, Consolidate_SortedTenKeys_GatherSortedLiveMetadata)
{
  // fill a node with ordered keys
  auto written_keys = WriteOrderedKeys(0, 9);

  // gather live metadata and check equality
  auto meta_vec = node->GatherSortedLiveMetadata();

  EXPECT_EQ(expected_record_count, meta_vec.size());
  for (size_t index = 0; index < meta_vec.size(); index++) {
    auto [key, meta] = meta_vec[index];
    EXPECT_EQ(written_keys[index], key);
    EXPECT_TRUE(meta.IsVisible());
  }
}

TEST_F(LeafNodeUInt64Fixture, Consolidate_SortedTenKeysWithDelete_GatherSortedLiveMetadata)
{
  // fill a node with ordered keys
  auto written_keys = WriteOrderedKeys(0, 9);

  // delete a key
  node->Delete(keys[2], kKeyLength);
  --expected_record_count;
  written_keys.erase(std::find(written_keys.begin(), written_keys.end(), keys[2]));

  // gather live metadata and check equality
  auto meta_vec = node->GatherSortedLiveMetadata();

  EXPECT_EQ(expected_record_count, meta_vec.size());
  for (size_t index = 0; index < meta_vec.size(); index++) {
    auto [key, meta] = meta_vec[index];
    EXPECT_EQ(written_keys[index], key);
    EXPECT_TRUE(meta.IsVisible());
  }
}

TEST_F(LeafNodeUInt64Fixture, Consolidate_SortedTenKeysWithUpdate_GatherSortedLiveMetadata)
{
  // fill a node with ordered keys
  auto written_keys = WriteOrderedKeys(0, 8);
  node->Update(keys[2], kKeyLength, payload_null, kPayloadLength);

  // gather live metadata and check equality
  auto meta_vec = node->GatherSortedLiveMetadata();

  EXPECT_EQ(expected_record_count, meta_vec.size());
  for (size_t index = 0; index < meta_vec.size(); index++) {
    auto [key, meta] = meta_vec[index];
    EXPECT_EQ(written_keys[index], key);
    EXPECT_TRUE(meta.IsVisible());
  }
}

TEST_F(LeafNodeUInt64Fixture, Consolidate_UnsortedTenKeys_GatherSortedLiveMetadata)
{
  // fill a node with ordered keys
  auto tmp_keys = WriteOrderedKeys(4, 9);
  auto written_keys = WriteOrderedKeys(0, 3);
  written_keys.insert(written_keys.end(), tmp_keys.begin(), tmp_keys.end());

  // gather live metadata and check equality
  auto meta_vec = node->GatherSortedLiveMetadata();

  EXPECT_EQ(expected_record_count, meta_vec.size());
  for (size_t index = 0; index < meta_vec.size(); index++) {
    auto [key, meta] = meta_vec[index];
    EXPECT_EQ(written_keys[index], key);
    EXPECT_TRUE(meta.IsVisible());
  }
}

TEST_F(LeafNodeUInt64Fixture, Consolidate_SortedTenKeys_NodeHasCorrectStatus)
{
  // prepare a consolidated node
  WriteOrderedKeys(0, 9);
  auto meta_vec = node->GatherSortedLiveMetadata();
  node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));

  VerifyStatusWord(node->GetStatusWord());
}

TEST_F(LeafNodeUInt64Fixture, Consolidate_SortedTenKeysWithDelete_NodeHasCorrectStatus)
{
  // prepare a consolidated node
  WriteOrderedKeys(0, 9);
  node->Delete(keys[2], kKeyLength);
  expected_record_count -= 1;
  expected_block_size -= kRecordLength;

  auto meta_vec = node->GatherSortedLiveMetadata();
  node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));

  VerifyStatusWord(node->GetStatusWord());
}

TEST_F(LeafNodeUInt64Fixture, Consolidate_SortedTenKeysWithUpdate_NodeHasCorrectStatus)
{
  // prepare a consolidated node
  WriteOrderedKeys(0, 8);
  node->Update(keys[2], kKeyLength, payload_null, kPayloadLength);

  auto meta_vec = node->GatherSortedLiveMetadata();
  node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));

  VerifyStatusWord(node->GetStatusWord());
}

TEST_F(LeafNodeUInt64Fixture, Consolidate_UnsortedTenKeys_NodeHasCorrectStatus)
{
  // prepare a consolidated node
  WriteOrderedKeys(4, 9);
  WriteOrderedKeys(0, 3);

  auto meta_vec = node->GatherSortedLiveMetadata();
  node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));

  VerifyStatusWord(node->GetStatusWord());
}

// /*--------------------------------------------------------------------------------------------------
//  * Split operation
//  *------------------------------------------------------------------------------------------------*/

TEST_F(LeafNodeUInt64Fixture, Split_EquallyDivided_NodesHaveCorrectStatus)
{
  // prepare split nodes
  WriteOrderedKeys(0, 9);
  auto left_record_count = 5;
  auto meta_vec = node->GatherSortedLiveMetadata();
  auto [left_node, right_node] = LeafNode_t::Split(node.get(), meta_vec, left_record_count);

  auto left_status = left_node->GetStatusWord();
  auto left_kBlockSize = expected_block_size / 2;
  auto left_deleted_size = 0;

  EXPECT_EQ(left_record_count, left_node->GetSortedCount());
  EXPECT_FALSE(left_status.IsFrozen());
  EXPECT_EQ(left_record_count, left_status.GetRecordCount());
  EXPECT_EQ(left_kBlockSize, left_status.GetBlockSize());
  EXPECT_EQ(left_deleted_size, left_status.GetDeletedSize());

  auto right_status = right_node->GetStatusWord();
  auto right_record_count = expected_record_count - left_record_count;
  auto right_kBlockSize = expected_block_size / 2;
  auto right_deleted_size = 0;

  EXPECT_EQ(right_record_count, right_node->GetSortedCount());
  EXPECT_FALSE(right_status.IsFrozen());
  EXPECT_EQ(right_record_count, right_status.GetRecordCount());
  EXPECT_EQ(right_kBlockSize, right_status.GetBlockSize());
  EXPECT_EQ(right_deleted_size, right_status.GetDeletedSize());
}

TEST_F(LeafNodeUInt64Fixture, Split_EquallyDivided_NodesHaveCorrectKeyPayloads)
{
  // prepare split nodes
  WriteOrderedKeys(0, 9);
  const auto left_record_count = 5;
  auto meta_vec = node->GatherSortedLiveMetadata();
  auto [left_node, right_node] = LeafNode_t::Split(node.get(), meta_vec, left_record_count);

  // check a split left node
  size_t index = 0;
  for (; index < left_record_count; ++index) {
    std::tie(rc, record) = left_node->Read(keys[index]);

    EXPECT_EQ(NodeReturnCode::kSuccess, rc);
    EXPECT_EQ(payloads[index], record->GetPayload());
  }

  // check a split right node
  for (; index < expected_record_count; ++index) {
    std::tie(rc, record) = right_node->Read(keys[index]);

    EXPECT_EQ(NodeReturnCode::kSuccess, rc);
    EXPECT_EQ(payloads[index], record->GetPayload());
  }
}

/*--------------------------------------------------------------------------------------------------
 * Merge operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(LeafNodeUInt64Fixture, Merge_LeftSiblingNode_NodeHasCorrectStatus)
{
  // prepare a merged node
  auto target = std::unique_ptr<LeafNode_t>(LeafNode_t::CreateEmptyNode(kNodeSize));
  target->Write(keys[4], kKeyLength, payloads[4], kPayloadLength);
  auto target_meta = target->GatherSortedLiveMetadata();

  auto sibling = std::unique_ptr<LeafNode_t>(LeafNode_t::CreateEmptyNode(kNodeSize));
  sibling->Write(keys[3], kKeyLength, payloads[3], kPayloadLength);
  auto sibling_meta = sibling->GatherSortedLiveMetadata();

  node.reset(LeafNode_t::Merge(target.get(), target_meta, sibling.get(), sibling_meta, true));
  expected_record_count = 2;
  expected_block_size = expected_record_count * kRecordLength;

  VerifyStatusWord(node->GetStatusWord());
}

TEST_F(LeafNodeUInt64Fixture, Merge_RightSiblingNode_NodeHasCorrectStatus)
{
  // prepare a merged node
  auto target = std::unique_ptr<LeafNode_t>(LeafNode_t::CreateEmptyNode(kNodeSize));
  target->Write(keys[2], kKeyLength, payloads[2], kPayloadLength);
  auto target_meta = target->GatherSortedLiveMetadata();

  auto sibling = std::unique_ptr<LeafNode_t>(LeafNode_t::CreateEmptyNode(kNodeSize));
  sibling->Write(keys[3], kKeyLength, payloads[3], kPayloadLength);
  auto sibling_meta = sibling->GatherSortedLiveMetadata();

  node.reset(LeafNode_t::Merge(target.get(), target_meta, sibling.get(), sibling_meta, false));
  expected_record_count = 2;
  expected_block_size = expected_record_count * kRecordLength;

  VerifyStatusWord(node->GetStatusWord());
}

TEST_F(LeafNodeUInt64Fixture, Merge_LeftSiblingNode_NodeHasCorrectKeyPayloads)
{
  // prepare a merged node
  auto target = std::unique_ptr<LeafNode_t>(LeafNode_t::CreateEmptyNode(kNodeSize));
  target->Write(keys[4], kKeyLength, payloads[4], kPayloadLength);
  auto target_meta = target->GatherSortedLiveMetadata();

  auto sibling = std::unique_ptr<LeafNode_t>(LeafNode_t::CreateEmptyNode(kNodeSize));
  sibling->Write(keys[3], kKeyLength, payloads[3], kPayloadLength);
  auto sibling_meta = sibling->GatherSortedLiveMetadata();

  node.reset(LeafNode_t::Merge(target.get(), target_meta, sibling.get(), sibling_meta, true));

  // check keys and payloads
  for (size_t index = 3; index <= 4; ++index) {
    std::tie(rc, record) = node->Read(keys[index]);

    EXPECT_EQ(NodeReturnCode::kSuccess, rc);
    EXPECT_EQ(payloads[index], record->GetPayload());
  }
}

TEST_F(LeafNodeUInt64Fixture, Merge_RightSiblingNode_NodeHasCorrectKeyPayloads)
{
  // prepare a merged node
  auto target = std::unique_ptr<LeafNode_t>(LeafNode_t::CreateEmptyNode(kNodeSize));
  target->Write(keys[2], kKeyLength, payloads[2], kPayloadLength);
  auto target_meta = target->GatherSortedLiveMetadata();

  auto sibling = std::unique_ptr<LeafNode_t>(LeafNode_t::CreateEmptyNode(kNodeSize));
  sibling->Write(keys[3], kKeyLength, payloads[3], kPayloadLength);
  auto sibling_meta = sibling->GatherSortedLiveMetadata();

  node.reset(LeafNode_t::Merge(target.get(), target_meta, sibling.get(), sibling_meta, false));

  // check keys and payloads
  for (size_t index = 2; index <= 3; ++index) {
    std::tie(rc, record) = node->Read(keys[index]);

    EXPECT_EQ(NodeReturnCode::kSuccess, rc);
    EXPECT_EQ(payloads[index], record->GetPayload());
  }
}

}  // namespace dbgroup::index::bztree
