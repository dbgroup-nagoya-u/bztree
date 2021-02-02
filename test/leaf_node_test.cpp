// Copyright (c) DB Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include <gtest/gtest.h>

#include <memory>

#include "bztree.hpp"

using std::byte;

namespace bztree
{
static constexpr size_t kDefaultNodeSize = 256;
static constexpr size_t kDefaultBlockSizeThreshold = 256;
static constexpr size_t kDefaultDeletedSizeThreshold = 256;
static constexpr size_t kIndexEpoch = 0;
static constexpr size_t kKeyNumForTest = 100;

/*##################################################################################################
 * Unsigned int 64 bits unit tests
 *################################################################################################*/

class LeafNodeUInt64Fixture : public testing::Test
{
 public:
  uint64_t keys[kKeyNumForTest];
  uint64_t* key_ptrs[kKeyNumForTest];
  uint64_t key_lengths[kKeyNumForTest];
  uint64_t payloads[kKeyNumForTest];
  uint64_t* payload_ptrs[kKeyNumForTest];
  uint64_t payload_lengths[kKeyNumForTest];
  uint64_t key_null = 0;  // null key must have 8 bytes to fill a node
  uint64_t* key_null_ptr = &key_null;
  size_t key_length_null = kWordLength;  // null key must have 8 bytes to fill a node
  uint64_t payload_null = 0;             // null payload must have 8 bytes to fill a node
  uint64_t* payload_null_ptr = &payload_null;
  size_t payload_length_null = kWordLength;  // null payload must have 8 bytes to fill a node

  std::unique_ptr<pmwcas::DescriptorPool> pool;
  std::unique_ptr<LeafNode> node;
  CompareAsUInt64 comp{};
  size_t record_count = 0;
  size_t index = 0;
  size_t block_size = 0;
  size_t deleted_size = 0;

 protected:
  void
  SetUp() override
  {
    pmwcas::InitLibrary(pmwcas::DefaultAllocator::Create, pmwcas::DefaultAllocator::Destroy,
                        pmwcas::LinuxEnvironment::Create, pmwcas::LinuxEnvironment::Destroy);
    pool.reset(new pmwcas::DescriptorPool{1000, 1, false});
    node.reset(LeafNode::CreateEmptyNode(kDefaultNodeSize));

    for (uint64_t index = 0; index < kKeyNumForTest; index++) {
      keys[index] = index;
      key_ptrs[index] = &keys[index];
      key_lengths[index] = kWordLength;
      payloads[index] = index;
      payload_ptrs[index] = &payloads[index];
      payload_lengths[index] = kWordLength;
    }
  }

  void
  TearDown() override
  {
    pmwcas::Thread::ClearRegistry();
  }

  constexpr uint64_t
  CastToValue(const void* target_addr)
  {
    return *BitCast<uint64_t*>(target_addr);
  }

  void
  WriteNullKey(const size_t write_num)
  {
    for (size_t index = 0; index < write_num; ++index) {
      node->Write(key_null_ptr, key_length_null, payload_null_ptr, payload_length_null, kIndexEpoch,
                  pool.get());
      ++record_count;
      block_size += key_length_null + payload_length_null;
    }
  }

  std::vector<uint64_t>
  WriteOrderedKeys(  //
      const size_t begin_index,
      const size_t end_index)
  {
    assert(end_index < kKeyNumForTest);

    std::vector<uint64_t> written_keys;
    for (size_t index = begin_index; index <= end_index; ++index) {
      auto key = keys[index];
      auto key_ptr = key_ptrs[index];
      auto key_length = key_lengths[index];
      auto payload_ptr = payload_ptrs[index];
      auto payload_length = payload_lengths[index];
      node->Write(key_ptr, key_length, payload_ptr, payload_length, kIndexEpoch, pool.get());

      written_keys.emplace_back(key);
      ++record_count;
      block_size += key_length + payload_length;
    }
    return written_keys;
  }
};

TEST_F(LeafNodeUInt64Fixture, New_EmptyNode_CorrectlyInitialized)
{
  EXPECT_EQ(kWordLength, node->GetStatusWordOffsetForTest());
  EXPECT_EQ(kWordLength, node->GetMetadataOffsetForTest());
  EXPECT_EQ(kDefaultNodeSize, node->GetNodeSize());
  EXPECT_EQ(0, node->GetSortedCount());
}

/*--------------------------------------------------------------------------------------------------
 * Read operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(LeafNodeUInt64Fixture, Read_NotPresentKey_ReadFailed)
{
  auto [rc, u_ptr] = node->Read(key_ptrs[1], comp);

  EXPECT_EQ(BaseNode::NodeReturnCode::kKeyNotExist, rc);
}

/*--------------------------------------------------------------------------------------------------
 * Scan operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(LeafNodeUInt64Fixture, Scan_EmptyNode_NoResult)
{
  auto [rc, scan_results] = node->Scan(key_ptrs[1], true, key_ptrs[10], true, comp);

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(0, scan_results.size());
}

TEST_F(LeafNodeUInt64Fixture, Scan_BothClosed_ScanTargetValues)
{
  WriteOrderedKeys(5, 10);
  WriteOrderedKeys(1, 4);

  auto [rc, scan_results] = node->Scan(key_ptrs[4], true, key_ptrs[6], true, comp);

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(3, scan_results.size());
  EXPECT_EQ(keys[4], CastToValue(scan_results[0].first.get()));
  EXPECT_EQ(payloads[4], CastToValue(scan_results[0].second.get()));
  EXPECT_EQ(keys[5], CastToValue(scan_results[1].first.get()));
  EXPECT_EQ(payloads[5], CastToValue(scan_results[1].second.get()));
  EXPECT_EQ(keys[6], CastToValue(scan_results[2].first.get()));
  EXPECT_EQ(payloads[6], CastToValue(scan_results[2].second.get()));
}

TEST_F(LeafNodeUInt64Fixture, Scan_LeftClosed_ScanTargetValues)
{
  WriteOrderedKeys(5, 10);
  WriteOrderedKeys(1, 4);

  auto [rc, scan_results] = node->Scan(key_ptrs[8], true, key_ptrs[10], false, comp);

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(2, scan_results.size());
  EXPECT_EQ(keys[8], CastToValue(scan_results[0].first.get()));
  EXPECT_EQ(payloads[8], CastToValue(scan_results[0].second.get()));
  EXPECT_EQ(keys[9], CastToValue(scan_results[1].first.get()));
  EXPECT_EQ(payloads[9], CastToValue(scan_results[1].second.get()));
}

TEST_F(LeafNodeUInt64Fixture, Scan_RightClosed_ScanTargetValues)
{
  WriteOrderedKeys(5, 10);
  WriteOrderedKeys(1, 4);

  auto [rc, scan_results] = node->Scan(key_ptrs[8], false, key_ptrs[10], true, comp);

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(2, scan_results.size());
  EXPECT_EQ(keys[9], CastToValue(scan_results[0].first.get()));
  EXPECT_EQ(payloads[9], CastToValue(scan_results[0].second.get()));
  EXPECT_EQ(keys[10], CastToValue(scan_results[1].first.get()));
  EXPECT_EQ(payloads[10], CastToValue(scan_results[1].second.get()));
}

TEST_F(LeafNodeUInt64Fixture, Scan_BothOpened_ScanTargetValues)
{
  WriteOrderedKeys(5, 10);
  WriteOrderedKeys(1, 4);

  auto [rc, scan_results] = node->Scan(key_ptrs[8], false, key_ptrs[10], false, comp);

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(1, scan_results.size());
  EXPECT_EQ(keys[9], CastToValue(scan_results[0].first.get()));
  EXPECT_EQ(payloads[9], CastToValue(scan_results[0].second.get()));
}

TEST_F(LeafNodeUInt64Fixture, Scan_LeftInfinity_ScanTargetValues)
{
  WriteOrderedKeys(5, 10);
  WriteOrderedKeys(1, 4);

  auto [rc, scan_results] = node->Scan(nullptr, false, key_ptrs[2], true, comp);

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(2, scan_results.size());
  EXPECT_EQ(keys[1], CastToValue(scan_results[0].first.get()));
  EXPECT_EQ(payloads[1], CastToValue(scan_results[0].second.get()));
  EXPECT_EQ(keys[2], CastToValue(scan_results[1].first.get()));
  EXPECT_EQ(payloads[2], CastToValue(scan_results[1].second.get()));
}

TEST_F(LeafNodeUInt64Fixture, Scan_RightInfinity_ScanTargetValues)
{
  WriteOrderedKeys(5, 10);
  WriteOrderedKeys(1, 4);

  auto [rc, scan_results] = node->Scan(key_ptrs[9], true, nullptr, false, comp);

  EXPECT_EQ(BaseNode::NodeReturnCode::kScanInProgress, rc);
  EXPECT_EQ(2, scan_results.size());
  EXPECT_EQ(keys[9], CastToValue(scan_results[0].first.get()));
  EXPECT_EQ(payloads[9], CastToValue(scan_results[0].second.get()));
  EXPECT_EQ(keys[10], CastToValue(scan_results[1].first.get()));
  EXPECT_EQ(payloads[10], CastToValue(scan_results[1].second.get()));
}

TEST_F(LeafNodeUInt64Fixture, Scan_LeftOutsideRange_NoResults)
{
  WriteOrderedKeys(5, 10);

  auto [rc, scan_results] = node->Scan(nullptr, false, key_ptrs[3], false, comp);

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(0, scan_results.size());
}

TEST_F(LeafNodeUInt64Fixture, Scan_RightOutsideRange_NoResults)
{
  WriteOrderedKeys(1, 4);

  auto [rc, scan_results] = node->Scan(key_ptrs[5], false, nullptr, false, comp);

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(0, scan_results.size());
}

TEST_F(LeafNodeUInt64Fixture, Scan_WithUpdateDelete_ScanLatestValues)
{
  WriteOrderedKeys(1, 5);
  node->Update(key_ptrs[2], key_lengths[2], payload_ptrs[0], payload_lengths[0], kIndexEpoch, comp,
               pool.get());
  node->Delete(key_ptrs[3], key_lengths[3], comp, pool.get());

  auto [rc, scan_results] = node->Scan(key_ptrs[2], true, key_ptrs[4], true, comp);

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(2, scan_results.size());
  EXPECT_EQ(keys[2], CastToValue(scan_results[0].first.get()));
  EXPECT_EQ(payloads[0], CastToValue(scan_results[0].second.get()));
  EXPECT_EQ(keys[4], CastToValue(scan_results[1].first.get()));
  EXPECT_EQ(payloads[4], CastToValue(scan_results[1].second.get()));
}

TEST_F(LeafNodeUInt64Fixture, Scan_ConsolidatedNodeWithinRange_ScanTargetValues)
{
  // prepare a consolidated node
  WriteOrderedKeys(3, 7);
  auto meta_vec = node->GatherSortedLiveMetadata(comp);
  node.reset(LeafNode::Consolidate(node.get(), meta_vec));

  auto [rc, scan_results] = node->Scan(key_ptrs[4], true, key_ptrs[6], true, comp);

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(3, scan_results.size());
  EXPECT_EQ(keys[4], CastToValue(scan_results[0].first.get()));
  EXPECT_EQ(payloads[4], CastToValue(scan_results[0].second.get()));
  EXPECT_EQ(keys[5], CastToValue(scan_results[1].first.get()));
  EXPECT_EQ(payloads[5], CastToValue(scan_results[1].second.get()));
  EXPECT_EQ(keys[6], CastToValue(scan_results[2].first.get()));
  EXPECT_EQ(payloads[6], CastToValue(scan_results[2].second.get()));
}

TEST_F(LeafNodeUInt64Fixture, Scan_ConsolidatedNodeWithLeftInfinity_ScanTargetValues)
{
  // prepare a consolidated node
  WriteOrderedKeys(3, 7);
  auto meta_vec = node->GatherSortedLiveMetadata(comp);
  node.reset(LeafNode::Consolidate(node.get(), meta_vec));

  auto [rc, scan_results] = node->Scan(nullptr, true, key_ptrs[3], true, comp);

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(1, scan_results.size());
  EXPECT_EQ(keys[3], CastToValue(scan_results[0].first.get()));
  EXPECT_EQ(payloads[3], CastToValue(scan_results[0].second.get()));
}

TEST_F(LeafNodeUInt64Fixture, Scan_ConsolidatedNodeWithRightInfinity_ScanTargetValues)
{
  // prepare a consolidated node
  WriteOrderedKeys(3, 7);
  auto meta_vec = node->GatherSortedLiveMetadata(comp);
  node.reset(LeafNode::Consolidate(node.get(), meta_vec));

  auto [rc, scan_results] = node->Scan(key_ptrs[7], true, nullptr, true, comp);

  EXPECT_EQ(BaseNode::NodeReturnCode::kScanInProgress, rc);
  EXPECT_EQ(1, scan_results.size());
  EXPECT_EQ(keys[7], CastToValue(scan_results[0].first.get()));
  EXPECT_EQ(payloads[7], CastToValue(scan_results[0].second.get()));
}

TEST_F(LeafNodeUInt64Fixture, Scan_ConsolidatedNodeWithUpdateDelete_ScanLatestValues)
{
  // prepare a consolidated node
  WriteOrderedKeys(3, 7);
  auto meta_vec = node->GatherSortedLiveMetadata(comp);
  node.reset(LeafNode::Consolidate(node.get(), meta_vec));
  node->Update(key_ptrs[5], key_lengths[5], payload_ptrs[0], payload_lengths[0], kIndexEpoch, comp,
               pool.get());
  node->Delete(key_ptrs[7], key_lengths[7], comp, pool.get());

  auto [rc, scan_results] = node->Scan(key_ptrs[5], true, key_ptrs[7], true, comp);

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(2, scan_results.size());
  EXPECT_EQ(keys[5], CastToValue(scan_results[0].first.get()));
  EXPECT_EQ(payloads[0], CastToValue(scan_results[0].second.get()));
  EXPECT_EQ(keys[6], CastToValue(scan_results[1].first.get()));
  EXPECT_EQ(payloads[6], CastToValue(scan_results[1].second.get()));
}

/*--------------------------------------------------------------------------------------------------
 * Write operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(LeafNodeUInt64Fixture, Write_TwoKeys_MetadataCorrectlyUpdated)
{
  auto [rc, status] = node->Write(key_ptrs[1], key_lengths[1], payload_ptrs[1], payload_lengths[1],
                                  kIndexEpoch, pool.get());
  record_count = 1;
  index = 0;
  block_size = key_lengths[1] + payload_lengths[1];

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(status, node->GetStatusWord());
  EXPECT_TRUE(node->RecordIsVisible(index));
  EXPECT_FALSE(node->RecordIsDeleted(index));
  EXPECT_EQ(key_lengths[1], node->GetKeyLength(index));
  EXPECT_EQ(payload_lengths[1], node->GetPayloadLength(index));
  EXPECT_FALSE(status.IsFrozen());
  EXPECT_EQ(record_count, status.GetRecordCount());
  EXPECT_EQ(block_size, status.GetBlockSize());
  EXPECT_EQ(deleted_size, status.GetDeletedSize());

  std::tie(rc, status) = node->Write(key_ptrs[2], key_lengths[2], payload_ptrs[2],
                                     payload_lengths[2], kIndexEpoch, pool.get());
  ++record_count;
  ++index;
  block_size += key_lengths[2] + payload_lengths[2];

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(status, node->GetStatusWord());
  EXPECT_TRUE(node->RecordIsVisible(index));
  EXPECT_FALSE(node->RecordIsDeleted(index));
  EXPECT_EQ(key_lengths[2], node->GetKeyLength(index));
  EXPECT_EQ(payload_lengths[2], node->GetPayloadLength(index));
  EXPECT_FALSE(status.IsFrozen());
  EXPECT_EQ(record_count, status.GetRecordCount());
  EXPECT_EQ(block_size, status.GetBlockSize());
  EXPECT_EQ(deleted_size, status.GetDeletedSize());
}

TEST_F(LeafNodeUInt64Fixture, Write_TwoKeys_ReadWrittenValues)
{
  node->Write(key_ptrs[1], key_lengths[1], payload_ptrs[1], payload_lengths[1], kIndexEpoch,
              pool.get());
  node->Write(key_ptrs[2], key_lengths[2], payload_ptrs[2], payload_lengths[2], kIndexEpoch,
              pool.get());

  // read 1st input value
  auto [rc, u_ptr] = node->Read(key_ptrs[1], comp);
  auto read_result = CastToValue(u_ptr.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(payloads[1], read_result);

  // read 2nd input value
  std::tie(rc, u_ptr) = node->Read(key_ptrs[2], comp);
  read_result = CastToValue(u_ptr.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(payloads[2], read_result);
}

TEST_F(LeafNodeUInt64Fixture, Write_DuplicateKey_ReadLatestValue)
{
  node->Write(key_ptrs[1], key_lengths[1], payload_ptrs[1], payload_lengths[1], kIndexEpoch,
              pool.get());
  node->Write(key_ptrs[1], key_lengths[1], payload_ptrs[2], payload_lengths[2], kIndexEpoch,
              pool.get());

  auto [rc, u_ptr] = node->Read(key_ptrs[1], comp);
  auto read_result = CastToValue(u_ptr.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(payloads[2], read_result);
}

TEST_F(LeafNodeUInt64Fixture, Write_FilledNode_GetCorrectReturnCodes)
{
  WriteNullKey(9);

  auto [rc, status] = node->Write(key_ptrs[1], key_lengths[1], payload_ptrs[1], payload_lengths[1],
                                  kIndexEpoch, pool.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);

  std::tie(rc, status) = node->Write(key_ptrs[1], key_lengths[1], payload_ptrs[1],
                                     payload_lengths[1], kIndexEpoch, pool.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kNoSpace, rc);
}

TEST_F(LeafNodeUInt64Fixture, Write_ConsolidatedNode_MetadataCorrectlyUpdated)
{
  // prepare a consolidated node
  WriteOrderedKeys(1, 5);
  auto meta_vec = node->GatherSortedLiveMetadata(comp);
  node.reset(LeafNode::Consolidate(node.get(), meta_vec));

  auto [rc, status] = node->Write(key_ptrs[11], key_lengths[11], payload_ptrs[11],
                                  payload_lengths[11], kIndexEpoch, pool.get());
  ++record_count;
  block_size += key_lengths[11] + payload_lengths[11];
  index = record_count - 1;

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(status, node->GetStatusWord());
  EXPECT_TRUE(node->RecordIsVisible(index));
  EXPECT_FALSE(node->RecordIsDeleted(index));
  EXPECT_EQ(key_lengths[11], node->GetKeyLength(index));
  EXPECT_EQ(payload_lengths[11], node->GetPayloadLength(index));
  EXPECT_FALSE(status.IsFrozen());
  EXPECT_EQ(record_count, status.GetRecordCount());
  EXPECT_EQ(block_size, status.GetBlockSize());
  EXPECT_EQ(deleted_size, status.GetDeletedSize());
}

TEST_F(LeafNodeUInt64Fixture, Write_ConsolidatedNode_ReadWrittenValue)
{
  // prepare a consolidated node
  WriteOrderedKeys(1, 5);
  auto meta_vec = node->GatherSortedLiveMetadata(comp);
  node.reset(LeafNode::Consolidate(node.get(), meta_vec));

  node->Write(key_ptrs[11], key_lengths[11], payload_ptrs[11], payload_lengths[11], kIndexEpoch,
              pool.get());
  auto [rc, u_ptr] = node->Read(key_ptrs[11], comp);
  auto read_result = CastToValue(u_ptr.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(payloads[11], read_result);
}

/*--------------------------------------------------------------------------------------------------
 * Insert operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(LeafNodeUInt64Fixture, Insert_TwoKeys_MetadataCorrectlyUpdated)
{
  auto [rc, status] = node->Insert(key_ptrs[1], key_lengths[1], payload_ptrs[1], payload_lengths[1],
                                   kIndexEpoch, comp, pool.get());
  record_count = 1;
  index = 0;
  block_size = key_lengths[1] + payload_lengths[1];

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(status, node->GetStatusWord());
  EXPECT_TRUE(node->RecordIsVisible(index));
  EXPECT_FALSE(node->RecordIsDeleted(index));
  EXPECT_EQ(key_lengths[1], node->GetKeyLength(index));
  EXPECT_EQ(payload_lengths[1], node->GetPayloadLength(index));
  EXPECT_FALSE(status.IsFrozen());
  EXPECT_EQ(record_count, status.GetRecordCount());
  EXPECT_EQ(block_size, status.GetBlockSize());
  EXPECT_EQ(deleted_size, status.GetDeletedSize());

  std::tie(rc, status) = node->Insert(key_ptrs[2], key_lengths[2], payload_ptrs[2],
                                      payload_lengths[2], kIndexEpoch, comp, pool.get());
  ++record_count;
  ++index;
  block_size += key_lengths[2] + payload_lengths[2];

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(status, node->GetStatusWord());
  EXPECT_TRUE(node->RecordIsVisible(index));
  EXPECT_FALSE(node->RecordIsDeleted(index));
  EXPECT_EQ(key_lengths[2], node->GetKeyLength(index));
  EXPECT_EQ(payload_lengths[2], node->GetPayloadLength(index));
  EXPECT_FALSE(status.IsFrozen());
  EXPECT_EQ(record_count, status.GetRecordCount());
  EXPECT_EQ(block_size, status.GetBlockSize());
  EXPECT_EQ(deleted_size, status.GetDeletedSize());
}

TEST_F(LeafNodeUInt64Fixture, Insert_TwoKeys_ReadInsertedValues)
{
  node->Insert(key_ptrs[1], key_lengths[1], payload_ptrs[1], payload_lengths[1], kIndexEpoch, comp,
               pool.get());
  node->Insert(key_ptrs[2], key_lengths[2], payload_ptrs[2], payload_lengths[2], kIndexEpoch, comp,
               pool.get());

  // read 1st input value
  auto [rc, u_ptr] = node->Read(key_ptrs[1], comp);
  auto read_result = CastToValue(u_ptr.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(payloads[1], read_result);

  // read 2nd input value
  std::tie(rc, u_ptr) = node->Read(key_ptrs[2], comp);
  read_result = CastToValue(u_ptr.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(payloads[2], read_result);
}

TEST_F(LeafNodeUInt64Fixture, Insert_DuplicateKey_InsertionFailed)
{
  node->Insert(key_ptrs[1], key_lengths[1], payload_ptrs[1], payload_lengths[1], kIndexEpoch, comp,
               pool.get());

  auto [rc, status] = node->Insert(key_ptrs[1], key_lengths[1], payload_ptrs[1], payload_lengths[1],
                                   kIndexEpoch, comp, pool.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kKeyExist, rc);
}

TEST_F(LeafNodeUInt64Fixture, Insert_FilledNode_GetCorrectReturnCodes)
{
  WriteNullKey(9);

  // fill a node
  auto [rc, status] = node->Insert(key_ptrs[1], key_lengths[1], payload_ptrs[1], payload_lengths[1],
                                   kIndexEpoch, comp, pool.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(kDefaultNodeSize, status.GetOccupiedSize());

  // insert a filled node with a not present key
  std::tie(rc, status) = node->Insert(key_ptrs[2], key_lengths[2], payload_ptrs[2],
                                      payload_lengths[2], kIndexEpoch, comp, pool.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kNoSpace, rc);

  // insert a filled node with an present key
  std::tie(rc, status) = node->Insert(key_ptrs[1], key_lengths[1], payload_ptrs[1],
                                      payload_lengths[1], kIndexEpoch, comp, pool.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kKeyExist, rc);
}

TEST_F(LeafNodeUInt64Fixture, Insert_ConsolidatedNode_MetadataCorrectlyUpdated)
{
  // prepare a consolidated node
  WriteOrderedKeys(1, 5);
  auto meta_vec = node->GatherSortedLiveMetadata(comp);
  node.reset(LeafNode::Consolidate(node.get(), meta_vec));

  auto [rc, status] = node->Insert(key_ptrs[11], key_lengths[11], payload_ptrs[11],
                                   payload_lengths[11], kIndexEpoch, comp, pool.get());
  ++record_count;
  block_size += key_lengths[11] + payload_lengths[11];
  index = record_count - 1;

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(status, node->GetStatusWord());
  EXPECT_TRUE(node->RecordIsVisible(index));
  EXPECT_FALSE(node->RecordIsDeleted(index));
  EXPECT_EQ(key_lengths[11], node->GetKeyLength(index));
  EXPECT_EQ(payload_lengths[11], node->GetPayloadLength(index));
  EXPECT_FALSE(status.IsFrozen());
  EXPECT_EQ(record_count, status.GetRecordCount());
  EXPECT_EQ(block_size, status.GetBlockSize());
  EXPECT_EQ(deleted_size, status.GetDeletedSize());
}

TEST_F(LeafNodeUInt64Fixture, Insert_ConsolidatedNode_ReadInsertedValue)
{
  // prepare a consolidated node
  WriteOrderedKeys(1, 5);
  auto meta_vec = node->GatherSortedLiveMetadata(comp);
  node.reset(LeafNode::Consolidate(node.get(), meta_vec));

  node->Insert(key_ptrs[11], key_lengths[11], payload_ptrs[11], payload_lengths[11], kIndexEpoch,
               comp, pool.get());
  auto [rc, u_ptr] = node->Read(key_ptrs[11], comp);
  auto read_result = CastToValue(u_ptr.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(payloads[11], read_result);
}

TEST_F(LeafNodeUInt64Fixture, Insert_ConsolidatedNodeWithDuplicateKey_InsertionFailed)
{
  // prepare a consolidated node
  WriteOrderedKeys(1, 5);
  auto meta_vec = node->GatherSortedLiveMetadata(comp);
  node.reset(LeafNode::Consolidate(node.get(), meta_vec));

  auto [rc, status] = node->Insert(key_ptrs[1], key_lengths[1], payload_ptrs[1], payload_lengths[1],
                                   kIndexEpoch, comp, pool.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kKeyExist, rc);
}

/*--------------------------------------------------------------------------------------------------
 * Update operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(LeafNodeUInt64Fixture, Update_SingleKey_MetadataCorrectlyUpdated)
{
  node->Insert(key_ptrs[1], key_lengths[1], payload_ptrs[1], payload_lengths[1], kIndexEpoch, comp,
               pool.get());
  record_count = 1;
  index = 0;
  block_size = key_lengths[1] + payload_lengths[1];

  auto [rc, status] = node->Update(key_ptrs[1], key_lengths[1], payload_ptrs[2], payload_lengths[2],
                                   kIndexEpoch, comp, pool.get());
  ++record_count;
  ++index;
  block_size += key_lengths[1] + payload_lengths[2];

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(status, node->GetStatusWord());
  EXPECT_TRUE(node->RecordIsVisible(index));
  EXPECT_FALSE(node->RecordIsDeleted(index));
  EXPECT_EQ(payload_lengths[2], node->GetPayloadLength(index));
  EXPECT_FALSE(status.IsFrozen());
  EXPECT_EQ(record_count, status.GetRecordCount());
  EXPECT_EQ(block_size, status.GetBlockSize());
  EXPECT_EQ(deleted_size, status.GetDeletedSize());
}

TEST_F(LeafNodeUInt64Fixture, Update_SingleKey_ReadUpdatedValue)
{
  node->Insert(key_ptrs[1], key_lengths[1], payload_ptrs[2], payload_lengths[2], kIndexEpoch, comp,
               pool.get());
  node->Update(key_ptrs[1], key_lengths[1], payload_ptrs[2], payload_lengths[2], kIndexEpoch, comp,
               pool.get());

  auto [rc, u_ptr] = node->Read(key_ptrs[1], comp);
  auto read_result = CastToValue(u_ptr.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(payloads[2], read_result);
}

TEST_F(LeafNodeUInt64Fixture, Update_NotPresentKey_UpdatedFailed)
{
  auto [rc, status] = node->Update(key_ptrs[1], key_lengths[1], payload_ptrs[1], payload_lengths[1],
                                   kIndexEpoch, comp, pool.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kKeyNotExist, rc);
}

TEST_F(LeafNodeUInt64Fixture, Update_DeletedKey_UpdateFailed)
{
  node->Insert(key_ptrs[1], key_lengths[1], payload_ptrs[2], payload_lengths[2], kIndexEpoch, comp,
               pool.get());
  node->Delete(key_ptrs[1], key_lengths[1], comp, pool.get());
  auto [rc, status] = node->Update(key_ptrs[1], key_lengths[1], payload_ptrs[2], payload_lengths[2],
                                   kIndexEpoch, comp, pool.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kKeyNotExist, rc);
}

TEST_F(LeafNodeUInt64Fixture, Update_FilledNode_GetCorrectReturnCodes)
{
  WriteNullKey(9);

  // fill a node
  auto [rc, status] = node->Update(key_null_ptr, key_length_null, payload_null_ptr,
                                   payload_length_null, kIndexEpoch, comp, pool.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(kDefaultNodeSize, status.GetOccupiedSize());

  // update a filled node with an present key
  std::tie(rc, status) = node->Update(key_null_ptr, key_length_null, payload_null_ptr,
                                      payload_length_null, kIndexEpoch, comp, pool.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kNoSpace, rc);

  // update a filled node with a not present key
  std::tie(rc, status) = node->Update(key_ptrs[1], key_lengths[1], payload_ptrs[1],
                                      payload_lengths[1], kIndexEpoch, comp, pool.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kKeyNotExist, rc);
}

TEST_F(LeafNodeUInt64Fixture, Update_ConsolidatedNode_MetadataCorrectlyUpdated)
{
  // prepare a consolidated node
  WriteOrderedKeys(1, 5);
  auto meta_vec = node->GatherSortedLiveMetadata(comp);
  node.reset(LeafNode::Consolidate(node.get(), meta_vec));

  auto [rc, status] = node->Update(key_ptrs[1], key_lengths[1], payload_ptrs[1], payload_lengths[1],
                                   kIndexEpoch, comp, pool.get());
  ++record_count;
  block_size += key_lengths[11] + payload_lengths[11];
  index = record_count - 1;

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(status, node->GetStatusWord());
  EXPECT_TRUE(node->RecordIsVisible(index));
  EXPECT_FALSE(node->RecordIsDeleted(index));
  EXPECT_EQ(key_lengths[11], node->GetKeyLength(index));
  EXPECT_EQ(payload_lengths[11], node->GetPayloadLength(index));
  EXPECT_FALSE(status.IsFrozen());
  EXPECT_EQ(record_count, status.GetRecordCount());
  EXPECT_EQ(block_size, status.GetBlockSize());
  EXPECT_EQ(deleted_size, status.GetDeletedSize());
}

TEST_F(LeafNodeUInt64Fixture, Update_ConsolidatedNode_ReadUpdatedValue)
{
  // prepare a consolidated node
  WriteOrderedKeys(1, 5);
  auto meta_vec = node->GatherSortedLiveMetadata(comp);
  node.reset(LeafNode::Consolidate(node.get(), meta_vec));

  node->Update(key_ptrs[1], key_lengths[1], payload_ptrs[11], payload_lengths[11], kIndexEpoch,
               comp, pool.get());
  auto [rc, u_ptr] = node->Read(key_ptrs[1], comp);
  auto read_result = CastToValue(u_ptr.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(payloads[11], read_result);
}

TEST_F(LeafNodeUInt64Fixture, Update_ConsolidatedNodeWithNotPresentKey_UpdatedFailed)
{
  // prepare a consolidated node
  WriteOrderedKeys(1, 5);
  auto meta_vec = node->GatherSortedLiveMetadata(comp);
  node.reset(LeafNode::Consolidate(node.get(), meta_vec));

  auto [rc, status] = node->Update(key_null_ptr, key_length_null, payload_null_ptr,
                                   payload_length_null, kIndexEpoch, comp, pool.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kKeyNotExist, rc);
}

TEST_F(LeafNodeUInt64Fixture, Update_ConsolidatedNodeWithDeletedKey_UpdatedFailed)
{
  // prepare a consolidated node
  WriteOrderedKeys(1, 5);
  auto meta_vec = node->GatherSortedLiveMetadata(comp);
  node.reset(LeafNode::Consolidate(node.get(), meta_vec));
  node->Delete(key_ptrs[1], key_lengths[1], comp, pool.get());

  auto [rc, status] = node->Update(key_ptrs[1], key_lengths[1], payload_ptrs[1], payload_lengths[1],
                                   kIndexEpoch, comp, pool.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kKeyNotExist, rc);
}

/*--------------------------------------------------------------------------------------------------
 * Delete operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(LeafNodeUInt64Fixture, Delete_TwoKeys_MetadataCorrectlyUpdated)
{
  node->Insert(key_ptrs[1], key_lengths[1], payload_ptrs[1], payload_lengths[1], kIndexEpoch, comp,
               pool.get());
  node->Insert(key_ptrs[2], key_lengths[2], payload_ptrs[2], payload_lengths[2], kIndexEpoch, comp,
               pool.get());
  record_count = 2;
  block_size = key_lengths[1] + payload_lengths[1] + key_lengths[2] + payload_lengths[2];

  auto [rc, status] = node->Delete(key_ptrs[1], key_lengths[1], comp, pool.get());
  deleted_size = key_lengths[1] + payload_lengths[1];

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(status, node->GetStatusWord());
  EXPECT_FALSE(node->RecordIsVisible(0));
  EXPECT_TRUE(node->RecordIsDeleted(0));
  EXPECT_FALSE(status.IsFrozen());
  EXPECT_EQ(record_count, status.GetRecordCount());
  EXPECT_EQ(block_size, status.GetBlockSize());
  EXPECT_EQ(deleted_size, status.GetDeletedSize());

  std::tie(rc, status) = node->Delete(key_ptrs[2], key_lengths[2], comp, pool.get());
  deleted_size += key_lengths[2] + payload_lengths[2];

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(status, node->GetStatusWord());
  EXPECT_FALSE(node->RecordIsVisible(1));
  EXPECT_TRUE(node->RecordIsDeleted(1));
  EXPECT_FALSE(status.IsFrozen());
  EXPECT_EQ(record_count, status.GetRecordCount());
  EXPECT_EQ(block_size, status.GetBlockSize());
  EXPECT_EQ(deleted_size, status.GetDeletedSize());
}

TEST_F(LeafNodeUInt64Fixture, Delete_PresentKey_DeletionSucceed)
{
  node->Insert(key_ptrs[1], key_lengths[1], payload_ptrs[1], payload_lengths[1], kIndexEpoch, comp,
               pool.get());

  auto [rc, status] = node->Delete(key_ptrs[1], key_lengths[1], comp, pool.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
}

TEST_F(LeafNodeUInt64Fixture, Delete_PresentKey_ReadFailed)
{
  node->Insert(key_ptrs[1], key_lengths[1], payload_ptrs[1], payload_lengths[1], kIndexEpoch, comp,
               pool.get());
  node->Delete(key_ptrs[1], key_lengths[1], comp, pool.get());

  auto [rc, u_ptr] = node->Read(key_ptrs[1], comp);

  EXPECT_EQ(BaseNode::NodeReturnCode::kKeyNotExist, rc);
}

TEST_F(LeafNodeUInt64Fixture, Delete_NotPresentKey_DeletionFailed)
{
  auto [rc, status] = node->Delete(key_ptrs[1], key_lengths[1], comp, pool.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kKeyNotExist, rc);
}

TEST_F(LeafNodeUInt64Fixture, Delete_DeletedKey_DeletionFailed)
{
  node->Insert(key_ptrs[1], key_lengths[1], payload_ptrs[1], payload_lengths[1], kIndexEpoch, comp,
               pool.get());
  node->Delete(key_ptrs[1], key_lengths[1], comp, pool.get());

  auto [rc, status] = node->Delete(key_ptrs[1], key_lengths[1], comp, pool.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kKeyNotExist, rc);
}

TEST_F(LeafNodeUInt64Fixture, Delete_FilledNode_GetCorrectReturnCodes)
{
  WriteNullKey(10);

  auto [rc, status] = node->Delete(key_null_ptr, key_length_null, comp, pool.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
}

TEST_F(LeafNodeUInt64Fixture, Delete_ConsolidatedNode_MetadataCorrectlyUpdated)
{
  // prepare a consolidated node
  WriteOrderedKeys(1, 5);
  auto meta_vec = node->GatherSortedLiveMetadata(comp);
  node.reset(LeafNode::Consolidate(node.get(), meta_vec));

  auto [rc, status] = node->Delete(key_ptrs[1], key_lengths[1], comp, pool.get());
  deleted_size = key_lengths[1] + payload_lengths[1];
  index = record_count - 1;

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(status, node->GetStatusWord());
  EXPECT_TRUE(node->RecordIsVisible(index));
  EXPECT_FALSE(node->RecordIsDeleted(index));
  EXPECT_EQ(key_lengths[11], node->GetKeyLength(index));
  EXPECT_EQ(payload_lengths[11], node->GetPayloadLength(index));
  EXPECT_FALSE(status.IsFrozen());
  EXPECT_EQ(record_count, status.GetRecordCount());
  EXPECT_EQ(block_size, status.GetBlockSize());
  EXPECT_EQ(deleted_size, status.GetDeletedSize());
}

TEST_F(LeafNodeUInt64Fixture, Delete_ConsolidatedNode_DeletionSucceed)
{
  // prepare a consolidated node
  WriteOrderedKeys(1, 5);
  auto meta_vec = node->GatherSortedLiveMetadata(comp);
  node.reset(LeafNode::Consolidate(node.get(), meta_vec));

  auto [rc, status] = node->Delete(key_ptrs[1], key_lengths[1], comp, pool.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
}

TEST_F(LeafNodeUInt64Fixture, Delete_ConsolidatedNodeWithNotPresentKey_DeletionFailed)
{
  // prepare a consolidated node
  WriteOrderedKeys(1, 5);
  auto meta_vec = node->GatherSortedLiveMetadata(comp);
  node.reset(LeafNode::Consolidate(node.get(), meta_vec));

  auto [rc, status] = node->Delete(key_null_ptr, key_length_null, comp, pool.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kKeyNotExist, rc);
}

TEST_F(LeafNodeUInt64Fixture, Delete_ConsolidatedNodeWithDeletedKey_DeletionFailed)
{
  // prepare a consolidated node
  WriteOrderedKeys(1, 5);
  auto meta_vec = node->GatherSortedLiveMetadata(comp);
  node.reset(LeafNode::Consolidate(node.get(), meta_vec));
  node->Delete(key_ptrs[1], key_lengths[1], comp, pool.get());

  auto [rc, status] = node->Delete(key_ptrs[1], key_lengths[1], comp, pool.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kKeyNotExist, rc);
}

/*--------------------------------------------------------------------------------------------------
 * Consolide operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(LeafNodeUInt64Fixture, Consolidate_SortedTenKeys_GatherSortedLiveMetadata)
{
  // fill a node with ordered keys
  auto written_keys = WriteOrderedKeys(1, 10);

  // gather live metadata and check equality
  auto meta_vec = node->GatherSortedLiveMetadata(comp);

  EXPECT_EQ(record_count, meta_vec.size());

  for (size_t index = 0; index < meta_vec.size(); index++) {
    EXPECT_EQ(written_keys[index], CastToValue(meta_vec[index].first));
  }
}

TEST_F(LeafNodeUInt64Fixture, Consolidate_SortedTenKeysWithDelete_GatherSortedLiveMetadata)
{
  // fill a node with ordered keys
  auto written_keys = WriteOrderedKeys(1, 10);
  node->Delete(key_ptrs[2], key_lengths[2], comp, pool.get());
  written_keys.erase(++(written_keys.begin()));
  --record_count;

  // gather live metadata and check equality
  auto meta_vec = node->GatherSortedLiveMetadata(comp);

  EXPECT_EQ(record_count, meta_vec.size());

  for (size_t index = 0; index < meta_vec.size(); index++) {
    EXPECT_EQ(written_keys[index], CastToValue(meta_vec[index].first));
  }
}

TEST_F(LeafNodeUInt64Fixture, Consolidate_SortedTenKeysWithUpdate_GatherSortedLiveMetadata)
{
  // fill a node with ordered keys
  auto written_keys = WriteOrderedKeys(1, 9);
  node->Update(key_ptrs[2], key_lengths[2], payload_null_ptr, payload_length_null, kIndexEpoch,
               comp, pool.get());

  // gather live metadata and check equality
  auto meta_vec = node->GatherSortedLiveMetadata(comp);

  EXPECT_EQ(record_count, meta_vec.size());

  for (size_t index = 0; index < meta_vec.size(); index++) {
    EXPECT_EQ(written_keys[index], CastToValue(meta_vec[index].first));
  }
}

TEST_F(LeafNodeUInt64Fixture, Consolidate_UnsortedTenKeys_GatherSortedLiveMetadata)
{
  // fill a node with ordered keys
  auto tmp_keys = WriteOrderedKeys(5, 10);
  auto written_keys = WriteOrderedKeys(1, 4);
  written_keys.insert(written_keys.end(), tmp_keys.begin(), tmp_keys.end());

  // gather live metadata and check equality
  auto meta_vec = node->GatherSortedLiveMetadata(comp);

  EXPECT_EQ(record_count, meta_vec.size());

  for (size_t index = 0; index < meta_vec.size(); index++) {
    EXPECT_EQ(written_keys[index], CastToValue(meta_vec[index].first));
  }
}

TEST_F(LeafNodeUInt64Fixture, Consolidate_SortedTenKeys_NodeHasCorrectStatus)
{
  // prepare a consolidated node
  WriteOrderedKeys(1, 10);
  auto meta_vec = node->GatherSortedLiveMetadata(comp);
  node.reset(LeafNode::Consolidate(node.get(), meta_vec));

  auto status = node->GetStatusWord();

  EXPECT_EQ(record_count, node->GetSortedCount());
  EXPECT_FALSE(status.IsFrozen());
  EXPECT_EQ(record_count, status.GetRecordCount());
  EXPECT_EQ(block_size, status.GetBlockSize());
  EXPECT_EQ(deleted_size, status.GetDeletedSize());
}

TEST_F(LeafNodeUInt64Fixture, Consolidate_SortedTenKeysWithDelete_NodeHasCorrectStatus)
{
  // prepare a consolidated node
  WriteOrderedKeys(1, 10);
  node->Delete(key_ptrs[2], key_lengths[2], comp, pool.get());
  auto meta_vec = node->GatherSortedLiveMetadata(comp);
  node.reset(LeafNode::Consolidate(node.get(), meta_vec));

  auto status = node->GetStatusWord();
  --record_count;
  block_size -= key_lengths[2] + payload_lengths[2];

  EXPECT_EQ(record_count, node->GetSortedCount());
  EXPECT_FALSE(status.IsFrozen());
  EXPECT_EQ(record_count, status.GetRecordCount());
  EXPECT_EQ(block_size, status.GetBlockSize());
  EXPECT_EQ(deleted_size, status.GetDeletedSize());
}

TEST_F(LeafNodeUInt64Fixture, Consolidate_SortedTenKeysWithUpdate_NodeHasCorrectStatus)
{
  // prepare a consolidated node
  WriteOrderedKeys(1, 9);
  node->Update(key_ptrs[2], key_lengths[2], payload_null_ptr, payload_length_null, kIndexEpoch,
               comp, pool.get());
  auto meta_vec = node->GatherSortedLiveMetadata(comp);
  node.reset(LeafNode::Consolidate(node.get(), meta_vec));

  auto status = node->GetStatusWord();

  EXPECT_EQ(record_count, node->GetSortedCount());
  EXPECT_FALSE(status.IsFrozen());
  EXPECT_EQ(record_count, status.GetRecordCount());
  EXPECT_EQ(block_size, status.GetBlockSize());
  EXPECT_EQ(deleted_size, status.GetDeletedSize());
}

TEST_F(LeafNodeUInt64Fixture, Consolidate_UnsortedTenKeys_NodeHasCorrectStatus)
{
  // prepare a consolidated node
  WriteOrderedKeys(5, 10);
  WriteOrderedKeys(1, 4);
  auto meta_vec = node->GatherSortedLiveMetadata(comp);
  node.reset(LeafNode::Consolidate(node.get(), meta_vec));

  auto status = node->GetStatusWord();

  EXPECT_EQ(record_count, node->GetSortedCount());
  EXPECT_FALSE(status.IsFrozen());
  EXPECT_EQ(record_count, status.GetRecordCount());
  EXPECT_EQ(block_size, status.GetBlockSize());
  EXPECT_EQ(deleted_size, status.GetDeletedSize());
}

/*--------------------------------------------------------------------------------------------------
 * Split operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(LeafNodeUInt64Fixture, Split_EquallyDivided_NodesHaveCorrectStatus)
{
  // prepare split nodes
  WriteOrderedKeys(1, 10);
  auto left_record_count = 5;
  auto meta_vec = node->GatherSortedLiveMetadata(comp);
  auto [left_node, right_node] = LeafNode::Split(node.get(), meta_vec, left_record_count);

  auto left_status = left_node->GetStatusWord();
  auto left_block_size = block_size / 2;
  auto left_deleted_size = 0;

  EXPECT_EQ(left_record_count, left_node->GetSortedCount());
  EXPECT_FALSE(left_status.IsFrozen());
  EXPECT_EQ(left_record_count, left_status.GetRecordCount());
  EXPECT_EQ(left_block_size, left_status.GetBlockSize());
  EXPECT_EQ(left_deleted_size, left_status.GetDeletedSize());

  auto right_status = right_node->GetStatusWord();
  auto right_record_count = record_count - left_record_count;
  auto right_block_size = block_size / 2;
  auto right_deleted_size = 0;

  EXPECT_EQ(right_record_count, right_node->GetSortedCount());
  EXPECT_FALSE(right_status.IsFrozen());
  EXPECT_EQ(right_record_count, right_status.GetRecordCount());
  EXPECT_EQ(right_block_size, right_status.GetBlockSize());
  EXPECT_EQ(right_deleted_size, right_status.GetDeletedSize());
}

TEST_F(LeafNodeUInt64Fixture, Split_EquallyDivided_NodesHaveCorrectKeyPayloads)
{
  // prepare split nodes
  WriteOrderedKeys(1, 10);
  const auto left_record_count = 5;
  auto meta_vec = node->GatherSortedLiveMetadata(comp);
  auto [left_node, right_node] = LeafNode::Split(node.get(), meta_vec, left_record_count);

  // check a split left node
  size_t index = 1;
  for (; index <= left_record_count; ++index) {
    auto [rc, u_ptr] = left_node->Read(key_ptrs[index], comp);

    EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
    EXPECT_EQ(payloads[index], CastToValue(u_ptr.get()));
  }

  // check a split right node
  for (; index <= record_count; ++index) {
    auto [rc, u_ptr] = right_node->Read(key_ptrs[index], comp);

    EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
    EXPECT_EQ(payloads[index], CastToValue(u_ptr.get()));
  }
}

/*--------------------------------------------------------------------------------------------------
 * Merge operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(LeafNodeUInt64Fixture, Merge_LeftSiblingNode_NodeHasCorrectStatus)
{
  // prepare a merged node
  WriteOrderedKeys(4, 6);
  auto this_meta = node->GatherSortedLiveMetadata(comp);
  auto sibling_node = std::unique_ptr<LeafNode>(LeafNode::CreateEmptyNode(kDefaultNodeSize));
  sibling_node->Write(key_ptrs[3], key_lengths[3], payload_ptrs[3], payload_lengths[3], kIndexEpoch,
                      pool.get());
  auto sibling_meta = sibling_node->GatherSortedLiveMetadata(comp);
  auto merged_node = LeafNode::Merge(node.get(), this_meta, sibling_node.get(), sibling_meta, true);

  auto merged_status = merged_node->GetStatusWord();
  auto merged_record_count = record_count + 1;
  auto merged_block_size = block_size + key_lengths[3] + payload_lengths[3];
  auto merged_deleted_size = 0;

  EXPECT_EQ(merged_record_count, merged_node->GetSortedCount());
  EXPECT_FALSE(merged_status.IsFrozen());
  EXPECT_EQ(merged_record_count, merged_status.GetRecordCount());
  EXPECT_EQ(merged_block_size, merged_status.GetBlockSize());
  EXPECT_EQ(merged_deleted_size, merged_status.GetDeletedSize());
}

TEST_F(LeafNodeUInt64Fixture, Merge_RightSiblingNode_NodeHasCorrectStatus)
{
  // prepare a merged node
  WriteOrderedKeys(4, 6);
  auto this_meta = node->GatherSortedLiveMetadata(comp);
  auto sibling_node = std::unique_ptr<LeafNode>(LeafNode::CreateEmptyNode(kDefaultNodeSize));
  sibling_node->Write(key_ptrs[7], key_lengths[7], payload_ptrs[7], payload_lengths[7], kIndexEpoch,
                      pool.get());
  auto sibling_meta = sibling_node->GatherSortedLiveMetadata(comp);
  auto merged_node =
      LeafNode::Merge(node.get(), this_meta, sibling_node.get(), sibling_meta, false);

  auto merged_status = merged_node->GetStatusWord();
  auto merged_record_count = record_count + 1;
  auto merged_block_size = block_size + key_lengths[7] + payload_lengths[7];
  auto merged_deleted_size = 0;

  EXPECT_EQ(merged_record_count, merged_node->GetSortedCount());
  EXPECT_FALSE(merged_status.IsFrozen());
  EXPECT_EQ(merged_record_count, merged_status.GetRecordCount());
  EXPECT_EQ(merged_block_size, merged_status.GetBlockSize());
  EXPECT_EQ(merged_deleted_size, merged_status.GetDeletedSize());
}

TEST_F(LeafNodeUInt64Fixture, Merge_LeftSiblingNode_NodeHasCorrectKeyPayloads)
{
  // prepare a merged node
  WriteOrderedKeys(4, 6);
  auto this_meta = node->GatherSortedLiveMetadata(comp);
  auto sibling_node = std::unique_ptr<LeafNode>(LeafNode::CreateEmptyNode(kDefaultNodeSize));
  sibling_node->Write(key_ptrs[3], key_lengths[3], payload_ptrs[3], payload_lengths[3], kIndexEpoch,
                      pool.get());
  auto sibling_meta = sibling_node->GatherSortedLiveMetadata(comp);
  auto merged_node = LeafNode::Merge(node.get(), this_meta, sibling_node.get(), sibling_meta, true);

  // check keys and payloads
  for (size_t index = 3; index <= 6; ++index) {
    auto [rc, u_ptr] = merged_node->Read(key_ptrs[index], comp);

    EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
    EXPECT_EQ(payloads[index], CastToValue(u_ptr.get()));
  }
}

TEST_F(LeafNodeUInt64Fixture, Merge_RightSiblingNode_NodeHasCorrectKeyPayloads)
{
  // prepare a merged node
  WriteOrderedKeys(4, 6);
  auto this_meta = node->GatherSortedLiveMetadata(comp);
  auto sibling_node = std::unique_ptr<LeafNode>(LeafNode::CreateEmptyNode(kDefaultNodeSize));
  sibling_node->Write(key_ptrs[7], key_lengths[7], payload_ptrs[7], payload_lengths[7], kIndexEpoch,
                      pool.get());
  auto sibling_meta = sibling_node->GatherSortedLiveMetadata(comp);
  auto merged_node =
      LeafNode::Merge(node.get(), this_meta, sibling_node.get(), sibling_meta, false);

  // check keys and payloads
  for (size_t index = 4; index <= 7; ++index) {
    auto [rc, u_ptr] = merged_node->Read(key_ptrs[index], comp);

    EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
    EXPECT_EQ(payloads[index], CastToValue(u_ptr.get()));
  }
}

}  // namespace bztree
