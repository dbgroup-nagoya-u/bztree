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

/*##################################################################################################
 * Unsigned int 64 bits unit tests
 *################################################################################################*/

class LeafNodeUInt64Fixture : public testing::Test
{
 public:
  uint64_t key_1st = 1;
  uint64_t key_2nd = 2;
  uint64_t key_7th = 7;
  uint64_t key_11th = 11;
  uint64_t key_null = 0;  // null key must have 8 bytes to fill a node
  uint64_t* key_1st_ptr = &key_1st;
  uint64_t* key_2nd_ptr = &key_2nd;
  uint64_t* key_7th_ptr = &key_7th;
  uint64_t* key_11th_ptr = &key_11th;
  uint64_t* key_null_ptr = &key_null;
  size_t key_length_1st = kWordLength;
  size_t key_length_2nd = kWordLength;
  size_t key_length_7th = kWordLength;
  size_t key_length_11th = kWordLength;
  size_t key_length_null = kWordLength;  // null key must have 8 bytes to fill a node
  uint64_t payload_1st = 1;
  uint64_t payload_2nd = 2;
  uint64_t payload_7th = 7;
  uint64_t payload_11th = 11;
  uint64_t payload_null = 0;  // null payload must have 8 bytes to fill a node
  uint64_t* payload_1st_ptr = &payload_1st;
  uint64_t* payload_2nd_ptr = &payload_2nd;
  uint64_t* payload_7th_ptr = &payload_7th;
  uint64_t* payload_11th_ptr = &payload_11th;
  uint64_t* payload_null_ptr = &payload_null;
  size_t payload_length_1st = kWordLength;
  size_t payload_length_2nd = kWordLength;
  size_t payload_length_7th = kWordLength;
  size_t payload_length_11th = kWordLength;
  size_t payload_length_null = kWordLength;  // null payload must have 8 bytes to fill a node

  std::unique_ptr<pmwcas::DescriptorPool> pool;
  std::unique_ptr<LeafNode> node;
  CompareAsUInt64 comp{};
  BaseNode::NodeReturnCode rc;
  StatusWord status;
  std::unique_ptr<std::byte[]> u_ptr;
  uint64_t result;
  size_t rec_count = 0;
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
  }

  void
  TearDown() override
  {
    pmwcas::Thread::ClearRegistry();
  }

  uint64_t
  GetResult()
  {
    return *BitCast<uint64_t*>(u_ptr.get());
  }

  constexpr uint64_t
  CastKey(const void* key)
  {
    return *BitCast<uint64_t*>(key);
  }

  void
  WriteNullKey(const size_t write_num)
  {
    for (size_t index = 0; index < write_num; ++index) {
      node->Write(key_null_ptr, key_length_null, payload_null_ptr, payload_length_null, kIndexEpoch,
                  pool.get());
      ++rec_count;
      block_size += key_length_null + payload_length_null;
    }
  }

  std::vector<uint64_t>
  WriteOrderedKeys(  //
      const size_t begin_index,
      const size_t end_index)
  {
    std::vector<uint64_t> written_keys;
    for (size_t index = begin_index; index <= end_index; ++index) {
      auto key = index;
      auto key_ptr = &key;
      auto key_length = kWordLength;
      auto value = index;
      auto payload_ptr = &value;
      auto payload_length = kWordLength;
      node->Write(key_ptr, key_length, payload_ptr, payload_length, kIndexEpoch, pool.get());

      written_keys.emplace_back(index);
      ++rec_count;
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
 * Write operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(LeafNodeUInt64Fixture, Write_TwoKeys_MetadataCorrectlyUpdated)
{
  std::tie(rc, status) = node->Write(key_1st_ptr, key_length_1st, payload_1st_ptr,
                                     payload_length_1st, kIndexEpoch, pool.get());
  rec_count = 1;
  index = 0;
  block_size = key_length_1st + payload_length_1st;

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(status, node->GetStatusWord());
  EXPECT_TRUE(node->RecordIsVisible(index));
  EXPECT_FALSE(node->RecordIsDeleted(index));
  EXPECT_EQ(key_length_1st, node->GetKeyLength(index));
  EXPECT_EQ(payload_length_1st, node->GetPayloadLength(index));
  EXPECT_FALSE(status.IsFrozen());
  EXPECT_EQ(rec_count, status.GetRecordCount());
  EXPECT_EQ(block_size, status.GetBlockSize());
  EXPECT_EQ(deleted_size, status.GetDeletedSize());

  std::tie(rc, status) = node->Write(key_2nd_ptr, key_length_2nd, payload_2nd_ptr,
                                     payload_length_2nd, kIndexEpoch, pool.get());
  ++rec_count;
  ++index;
  block_size += key_length_2nd + payload_length_2nd;

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(status, node->GetStatusWord());
  EXPECT_TRUE(node->RecordIsVisible(index));
  EXPECT_FALSE(node->RecordIsDeleted(index));
  EXPECT_EQ(key_length_2nd, node->GetKeyLength(index));
  EXPECT_EQ(payload_length_2nd, node->GetPayloadLength(index));
  EXPECT_FALSE(status.IsFrozen());
  EXPECT_EQ(rec_count, status.GetRecordCount());
  EXPECT_EQ(block_size, status.GetBlockSize());
  EXPECT_EQ(deleted_size, status.GetDeletedSize());
}

TEST_F(LeafNodeUInt64Fixture, Write_TwoKeys_ReadWrittenValues)
{
  node->Write(key_1st_ptr, key_length_1st, payload_1st_ptr, payload_length_1st, kIndexEpoch,
              pool.get());
  node->Write(key_2nd_ptr, key_length_2nd, payload_2nd_ptr, payload_length_2nd, kIndexEpoch,
              pool.get());

  // read 1st input value
  std::tie(rc, u_ptr) = node->Read(key_1st_ptr, comp);
  result = GetResult();

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(payload_1st, result);

  // read 2nd input value
  std::tie(rc, u_ptr) = node->Read(key_2nd_ptr, comp);
  result = GetResult();

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(payload_2nd, result);
}

TEST_F(LeafNodeUInt64Fixture, Write_DuplicateKey_ReadLatestValue)
{
  node->Write(key_1st_ptr, key_length_1st, payload_1st_ptr, payload_length_1st, kIndexEpoch,
              pool.get());
  node->Write(key_1st_ptr, key_length_1st, payload_2nd_ptr, payload_length_2nd, kIndexEpoch,
              pool.get());

  std::tie(rc, u_ptr) = node->Read(key_1st_ptr, comp);
  result = GetResult();

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(payload_2nd, result);
}

TEST_F(LeafNodeUInt64Fixture, Write_FilledNode_GetCorrectReturnCodes)
{
  WriteNullKey(9);

  std::tie(rc, status) = node->Write(key_1st_ptr, key_length_1st, payload_1st_ptr,
                                     payload_length_1st, kIndexEpoch, pool.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);

  std::tie(rc, status) = node->Write(key_1st_ptr, key_length_1st, payload_1st_ptr,
                                     payload_length_1st, kIndexEpoch, pool.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kNoSpace, rc);
}

TEST_F(LeafNodeUInt64Fixture, Write_ConsolidatedNode_MetadataCorrectlyUpdated)
{
  // prepare a consolidated node
  WriteOrderedKeys(1, 5);
  auto meta_vec = node->GatherSortedLiveMetadata(comp);
  node.reset(node->Consolidate(meta_vec));

  std::tie(rc, status) = node->Write(key_11th_ptr, key_length_11th, payload_11th_ptr,
                                     payload_length_11th, kIndexEpoch, pool.get());
  ++rec_count;
  block_size += key_length_11th + payload_length_11th;
  index = rec_count - 1;

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(status, node->GetStatusWord());
  EXPECT_TRUE(node->RecordIsVisible(index));
  EXPECT_FALSE(node->RecordIsDeleted(index));
  EXPECT_EQ(key_length_11th, node->GetKeyLength(index));
  EXPECT_EQ(payload_length_11th, node->GetPayloadLength(index));
  EXPECT_FALSE(status.IsFrozen());
  EXPECT_EQ(rec_count, status.GetRecordCount());
  EXPECT_EQ(block_size, status.GetBlockSize());
  EXPECT_EQ(deleted_size, status.GetDeletedSize());
}

TEST_F(LeafNodeUInt64Fixture, Write_ConsolidatedNode_ReadWrittenValue)
{
  // prepare a consolidated node
  WriteOrderedKeys(1, 5);
  auto meta_vec = node->GatherSortedLiveMetadata(comp);
  node.reset(node->Consolidate(meta_vec));

  node->Write(key_11th_ptr, key_length_11th, payload_11th_ptr, payload_length_11th, kIndexEpoch,
              pool.get());
  std::tie(rc, u_ptr) = node->Read(key_11th_ptr, comp);
  result = GetResult();

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(payload_11th, result);
}

/*--------------------------------------------------------------------------------------------------
 * Insert operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(LeafNodeUInt64Fixture, Insert_TwoKeys_MetadataCorrectlyUpdated)
{
  std::tie(rc, status) = node->Insert(key_1st_ptr, key_length_1st, payload_1st_ptr,
                                      payload_length_1st, kIndexEpoch, comp, pool.get());
  rec_count = 1;
  index = 0;
  block_size = key_length_1st + payload_length_1st;

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(status, node->GetStatusWord());
  EXPECT_TRUE(node->RecordIsVisible(index));
  EXPECT_FALSE(node->RecordIsDeleted(index));
  EXPECT_EQ(key_length_1st, node->GetKeyLength(index));
  EXPECT_EQ(payload_length_1st, node->GetPayloadLength(index));
  EXPECT_FALSE(status.IsFrozen());
  EXPECT_EQ(rec_count, status.GetRecordCount());
  EXPECT_EQ(block_size, status.GetBlockSize());
  EXPECT_EQ(deleted_size, status.GetDeletedSize());

  std::tie(rc, status) = node->Insert(key_2nd_ptr, key_length_2nd, payload_2nd_ptr,
                                      payload_length_2nd, kIndexEpoch, comp, pool.get());
  ++rec_count;
  ++index;
  block_size += key_length_2nd + payload_length_2nd;

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(status, node->GetStatusWord());
  EXPECT_TRUE(node->RecordIsVisible(index));
  EXPECT_FALSE(node->RecordIsDeleted(index));
  EXPECT_EQ(key_length_2nd, node->GetKeyLength(index));
  EXPECT_EQ(payload_length_2nd, node->GetPayloadLength(index));
  EXPECT_FALSE(status.IsFrozen());
  EXPECT_EQ(rec_count, status.GetRecordCount());
  EXPECT_EQ(block_size, status.GetBlockSize());
  EXPECT_EQ(deleted_size, status.GetDeletedSize());
}

TEST_F(LeafNodeUInt64Fixture, Insert_TwoKeys_ReadInsertedValues)
{
  node->Insert(key_1st_ptr, key_length_1st, payload_1st_ptr, payload_length_1st, kIndexEpoch, comp,
               pool.get());
  node->Insert(key_2nd_ptr, key_length_2nd, payload_2nd_ptr, payload_length_2nd, kIndexEpoch, comp,
               pool.get());

  // read 1st input value
  std::tie(rc, u_ptr) = node->Read(key_1st_ptr, comp);
  result = GetResult();

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(payload_1st, result);

  // read 2nd input value
  std::tie(rc, u_ptr) = node->Read(key_2nd_ptr, comp);
  result = GetResult();

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(payload_2nd, result);
}

TEST_F(LeafNodeUInt64Fixture, Insert_DuplicateKey_InsertionFailed)
{
  node->Insert(key_1st_ptr, key_length_1st, payload_1st_ptr, payload_length_1st, kIndexEpoch, comp,
               pool.get());

  std::tie(rc, status) = node->Insert(key_1st_ptr, key_length_1st, payload_1st_ptr,
                                      payload_length_1st, kIndexEpoch, comp, pool.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kKeyExist, rc);
}

TEST_F(LeafNodeUInt64Fixture, Insert_FilledNode_GetCorrectReturnCodes)
{
  WriteNullKey(9);

  // fill a node
  std::tie(rc, status) = node->Insert(key_1st_ptr, key_length_1st, payload_1st_ptr,
                                      payload_length_1st, kIndexEpoch, comp, pool.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(kDefaultNodeSize, status.GetOccupiedSize());

  // insert a filled node with a not present key
  std::tie(rc, status) = node->Insert(key_2nd_ptr, key_length_2nd, payload_2nd_ptr,
                                      payload_length_2nd, kIndexEpoch, comp, pool.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kNoSpace, rc);

  // insert a filled node with an present key
  std::tie(rc, status) = node->Insert(key_1st_ptr, key_length_1st, payload_1st_ptr,
                                      payload_length_1st, kIndexEpoch, comp, pool.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kKeyExist, rc);
}

TEST_F(LeafNodeUInt64Fixture, Insert_ConsolidatedNode_MetadataCorrectlyUpdated)
{
  // prepare a consolidated node
  WriteOrderedKeys(1, 5);
  auto meta_vec = node->GatherSortedLiveMetadata(comp);
  node.reset(node->Consolidate(meta_vec));

  std::tie(rc, status) = node->Insert(key_11th_ptr, key_length_11th, payload_11th_ptr,
                                      payload_length_11th, kIndexEpoch, comp, pool.get());
  ++rec_count;
  block_size += key_length_11th + payload_length_11th;
  index = rec_count - 1;

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(status, node->GetStatusWord());
  EXPECT_TRUE(node->RecordIsVisible(index));
  EXPECT_FALSE(node->RecordIsDeleted(index));
  EXPECT_EQ(key_length_11th, node->GetKeyLength(index));
  EXPECT_EQ(payload_length_11th, node->GetPayloadLength(index));
  EXPECT_FALSE(status.IsFrozen());
  EXPECT_EQ(rec_count, status.GetRecordCount());
  EXPECT_EQ(block_size, status.GetBlockSize());
  EXPECT_EQ(deleted_size, status.GetDeletedSize());
}

TEST_F(LeafNodeUInt64Fixture, Insert_ConsolidatedNode_ReadInsertedValue)
{
  // prepare a consolidated node
  WriteOrderedKeys(1, 5);
  auto meta_vec = node->GatherSortedLiveMetadata(comp);
  node.reset(node->Consolidate(meta_vec));

  node->Insert(key_11th_ptr, key_length_11th, payload_11th_ptr, payload_length_11th, kIndexEpoch,
               comp, pool.get());
  std::tie(rc, u_ptr) = node->Read(key_11th_ptr, comp);
  result = GetResult();

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(payload_11th, result);
}

TEST_F(LeafNodeUInt64Fixture, Insert_ConsolidatedNodeWithDuplicateKey_InsertionFailed)
{
  // prepare a consolidated node
  WriteOrderedKeys(1, 5);
  auto meta_vec = node->GatherSortedLiveMetadata(comp);
  node.reset(node->Consolidate(meta_vec));

  std::tie(rc, status) = node->Insert(key_1st_ptr, key_length_1st, payload_1st_ptr,
                                      payload_length_1st, kIndexEpoch, comp, pool.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kKeyExist, rc);
}

/*--------------------------------------------------------------------------------------------------
 * Update operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(LeafNodeUInt64Fixture, Update_SingleKey_MetadataCorrectlyUpdated)
{
  node->Insert(key_1st_ptr, key_length_1st, payload_1st_ptr, payload_length_1st, kIndexEpoch, comp,
               pool.get());
  rec_count = 1;
  index = 0;
  block_size = key_length_1st + payload_length_1st;

  std::tie(rc, status) = node->Update(key_1st_ptr, key_length_1st, payload_2nd_ptr,
                                      payload_length_2nd, kIndexEpoch, comp, pool.get());
  ++rec_count;
  ++index;
  block_size += key_length_1st + payload_length_2nd;

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(status, node->GetStatusWord());
  EXPECT_TRUE(node->RecordIsVisible(index));
  EXPECT_FALSE(node->RecordIsDeleted(index));
  EXPECT_EQ(payload_length_2nd, node->GetPayloadLength(index));
  EXPECT_FALSE(status.IsFrozen());
  EXPECT_EQ(rec_count, status.GetRecordCount());
  EXPECT_EQ(block_size, status.GetBlockSize());
  EXPECT_EQ(deleted_size, status.GetDeletedSize());
}

TEST_F(LeafNodeUInt64Fixture, Update_SingleKey_ReadUpdatedValue)
{
  node->Insert(key_1st_ptr, key_length_1st, payload_2nd_ptr, payload_length_2nd, kIndexEpoch, comp,
               pool.get());
  node->Update(key_1st_ptr, key_length_1st, payload_2nd_ptr, payload_length_2nd, kIndexEpoch, comp,
               pool.get());

  std::tie(rc, u_ptr) = node->Read(key_1st_ptr, comp);
  result = GetResult();

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(payload_2nd, result);
}

TEST_F(LeafNodeUInt64Fixture, Update_NotPresentKey_UpdatedFailed)
{
  std::tie(rc, status) = node->Update(key_1st_ptr, key_length_1st, payload_1st_ptr,
                                      payload_length_1st, kIndexEpoch, comp, pool.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kKeyNotExist, rc);
}

TEST_F(LeafNodeUInt64Fixture, Update_DeletedKey_UpdateFailed)
{
  node->Insert(key_1st_ptr, key_length_1st, payload_2nd_ptr, payload_length_2nd, kIndexEpoch, comp,
               pool.get());
  node->Delete(key_1st_ptr, key_length_1st, comp, pool.get());
  std::tie(rc, status) = node->Update(key_1st_ptr, key_length_1st, payload_2nd_ptr,
                                      payload_length_2nd, kIndexEpoch, comp, pool.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kKeyNotExist, rc);
}

TEST_F(LeafNodeUInt64Fixture, Update_FilledNode_GetCorrectReturnCodes)
{
  WriteNullKey(9);

  // fill a node
  std::tie(rc, status) = node->Update(key_null_ptr, key_length_null, payload_null_ptr,
                                      payload_length_null, kIndexEpoch, comp, pool.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(kDefaultNodeSize, status.GetOccupiedSize());

  // update a filled node with an present key
  std::tie(rc, status) = node->Update(key_null_ptr, key_length_null, payload_null_ptr,
                                      payload_length_null, kIndexEpoch, comp, pool.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kNoSpace, rc);

  // update a filled node with a not present key
  std::tie(rc, status) = node->Update(key_1st_ptr, key_length_1st, payload_1st_ptr,
                                      payload_length_1st, kIndexEpoch, comp, pool.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kKeyNotExist, rc);
}

TEST_F(LeafNodeUInt64Fixture, Update_ConsolidatedNode_MetadataCorrectlyUpdated)
{
  // prepare a consolidated node
  WriteOrderedKeys(1, 5);
  auto meta_vec = node->GatherSortedLiveMetadata(comp);
  node.reset(node->Consolidate(meta_vec));

  std::tie(rc, status) = node->Update(key_1st_ptr, key_length_1st, payload_1st_ptr,
                                      payload_length_1st, kIndexEpoch, comp, pool.get());
  ++rec_count;
  block_size += key_length_11th + payload_length_11th;
  index = rec_count - 1;

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(status, node->GetStatusWord());
  EXPECT_TRUE(node->RecordIsVisible(index));
  EXPECT_FALSE(node->RecordIsDeleted(index));
  EXPECT_EQ(key_length_11th, node->GetKeyLength(index));
  EXPECT_EQ(payload_length_11th, node->GetPayloadLength(index));
  EXPECT_FALSE(status.IsFrozen());
  EXPECT_EQ(rec_count, status.GetRecordCount());
  EXPECT_EQ(block_size, status.GetBlockSize());
  EXPECT_EQ(deleted_size, status.GetDeletedSize());
}

TEST_F(LeafNodeUInt64Fixture, Update_ConsolidatedNode_ReadUpdatedValue)
{
  // prepare a consolidated node
  WriteOrderedKeys(1, 5);
  auto meta_vec = node->GatherSortedLiveMetadata(comp);
  node.reset(node->Consolidate(meta_vec));

  node->Update(key_1st_ptr, key_length_1st, payload_11th_ptr, payload_length_11th, kIndexEpoch,
               comp, pool.get());
  std::tie(rc, u_ptr) = node->Read(key_1st_ptr, comp);
  result = GetResult();

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(payload_11th, result);
}

TEST_F(LeafNodeUInt64Fixture, Update_ConsolidatedNodeWithNotPresentKey_UpdatedFailed)
{
  // prepare a consolidated node
  WriteOrderedKeys(1, 5);
  auto meta_vec = node->GatherSortedLiveMetadata(comp);
  node.reset(node->Consolidate(meta_vec));

  std::tie(rc, status) = node->Update(key_null_ptr, key_length_null, payload_null_ptr,
                                      payload_length_null, kIndexEpoch, comp, pool.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kKeyNotExist, rc);
}

TEST_F(LeafNodeUInt64Fixture, Update_ConsolidatedNodeWithDeletedKey_UpdatedFailed)
{
  // prepare a consolidated node
  WriteOrderedKeys(1, 5);
  auto meta_vec = node->GatherSortedLiveMetadata(comp);
  node.reset(node->Consolidate(meta_vec));
  node->Delete(key_1st_ptr, key_length_1st, comp, pool.get());

  std::tie(rc, status) = node->Update(key_1st_ptr, key_length_1st, payload_1st_ptr,
                                      payload_length_1st, kIndexEpoch, comp, pool.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kKeyNotExist, rc);
}

/*--------------------------------------------------------------------------------------------------
 * Delete operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(LeafNodeUInt64Fixture, Delete_TwoKeys_MetadataCorrectlyUpdated)
{
  node->Insert(key_1st_ptr, key_length_1st, payload_1st_ptr, payload_length_1st, kIndexEpoch, comp,
               pool.get());
  node->Insert(key_2nd_ptr, key_length_2nd, payload_2nd_ptr, payload_length_2nd, kIndexEpoch, comp,
               pool.get());
  rec_count = 2;
  block_size = key_length_1st + payload_length_1st + key_length_2nd + payload_length_2nd;

  std::tie(rc, status) = node->Delete(key_1st_ptr, key_length_1st, comp, pool.get());
  deleted_size = key_length_1st + payload_length_1st;

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(status, node->GetStatusWord());
  EXPECT_FALSE(node->RecordIsVisible(0));
  EXPECT_TRUE(node->RecordIsDeleted(0));
  EXPECT_FALSE(status.IsFrozen());
  EXPECT_EQ(rec_count, status.GetRecordCount());
  EXPECT_EQ(block_size, status.GetBlockSize());
  EXPECT_EQ(deleted_size, status.GetDeletedSize());

  std::tie(rc, status) = node->Delete(key_2nd_ptr, key_length_2nd, comp, pool.get());
  deleted_size += key_length_2nd + payload_length_2nd;

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(status, node->GetStatusWord());
  EXPECT_FALSE(node->RecordIsVisible(1));
  EXPECT_TRUE(node->RecordIsDeleted(1));
  EXPECT_FALSE(status.IsFrozen());
  EXPECT_EQ(rec_count, status.GetRecordCount());
  EXPECT_EQ(block_size, status.GetBlockSize());
  EXPECT_EQ(deleted_size, status.GetDeletedSize());
}

TEST_F(LeafNodeUInt64Fixture, Delete_PresentKey_DeletionSucceed)
{
  node->Insert(key_1st_ptr, key_length_1st, payload_1st_ptr, payload_length_1st, kIndexEpoch, comp,
               pool.get());

  std::tie(rc, status) = node->Delete(key_1st_ptr, key_length_1st, comp, pool.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
}

TEST_F(LeafNodeUInt64Fixture, Delete_NotPresentKey_DeletionFailed)
{
  std::tie(rc, status) = node->Delete(key_1st_ptr, key_length_1st, comp, pool.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kKeyNotExist, rc);
}

TEST_F(LeafNodeUInt64Fixture, Delete_DeletedKey_DeletionFailed)
{
  node->Insert(key_1st_ptr, key_length_1st, payload_1st_ptr, payload_length_1st, kIndexEpoch, comp,
               pool.get());
  node->Delete(key_1st_ptr, key_length_1st, comp, pool.get());

  std::tie(rc, status) = node->Delete(key_1st_ptr, key_length_1st, comp, pool.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kKeyNotExist, rc);
}

TEST_F(LeafNodeUInt64Fixture, Delete_FilledNode_GetCorrectReturnCodes)
{
  WriteNullKey(10);

  std::tie(rc, status) = node->Delete(key_null_ptr, key_length_null, comp, pool.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
}

TEST_F(LeafNodeUInt64Fixture, Delete_ConsolidatedNode_MetadataCorrectlyUpdated)
{
  // prepare a consolidated node
  WriteOrderedKeys(1, 5);
  auto meta_vec = node->GatherSortedLiveMetadata(comp);
  node.reset(node->Consolidate(meta_vec));

  std::tie(rc, status) = node->Delete(key_1st_ptr, key_length_1st, comp, pool.get());
  deleted_size = key_length_1st + payload_length_1st;
  index = rec_count - 1;

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(status, node->GetStatusWord());
  EXPECT_TRUE(node->RecordIsVisible(index));
  EXPECT_FALSE(node->RecordIsDeleted(index));
  EXPECT_EQ(key_length_11th, node->GetKeyLength(index));
  EXPECT_EQ(payload_length_11th, node->GetPayloadLength(index));
  EXPECT_FALSE(status.IsFrozen());
  EXPECT_EQ(rec_count, status.GetRecordCount());
  EXPECT_EQ(block_size, status.GetBlockSize());
  EXPECT_EQ(deleted_size, status.GetDeletedSize());
}

TEST_F(LeafNodeUInt64Fixture, Delete_ConsolidatedNode_DeletionSucceed)
{
  // prepare a consolidated node
  WriteOrderedKeys(1, 5);
  auto meta_vec = node->GatherSortedLiveMetadata(comp);
  node.reset(node->Consolidate(meta_vec));

  std::tie(rc, status) = node->Delete(key_1st_ptr, key_length_1st, comp, pool.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
}

TEST_F(LeafNodeUInt64Fixture, Delete_ConsolidatedNodeWithNotPresentKey_DeletionFailed)
{
  // prepare a consolidated node
  WriteOrderedKeys(1, 5);
  auto meta_vec = node->GatherSortedLiveMetadata(comp);
  node.reset(node->Consolidate(meta_vec));

  std::tie(rc, status) = node->Delete(key_null_ptr, key_length_null, comp, pool.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kKeyNotExist, rc);
}

TEST_F(LeafNodeUInt64Fixture, Delete_ConsolidatedNodeWithDeletedKey_DeletionFailed)
{
  // prepare a consolidated node
  WriteOrderedKeys(1, 5);
  auto meta_vec = node->GatherSortedLiveMetadata(comp);
  node.reset(node->Consolidate(meta_vec));
  node->Delete(key_1st_ptr, key_length_1st, comp, pool.get());

  std::tie(rc, status) = node->Delete(key_1st_ptr, key_length_1st, comp, pool.get());

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

  EXPECT_EQ(rec_count, meta_vec.size());

  for (size_t index = 0; index < meta_vec.size(); index++) {
    EXPECT_EQ(written_keys[index], CastKey(meta_vec[index].first));
  }
}

TEST_F(LeafNodeUInt64Fixture, Consolidate_SortedTenKeysWithDelete_GatherSortedLiveMetadata)
{
  // fill a node with ordered keys
  auto written_keys = WriteOrderedKeys(1, 10);
  node->Delete(key_2nd_ptr, key_length_2nd, comp, pool.get());
  written_keys.erase(++(written_keys.begin()));
  --rec_count;

  // gather live metadata and check equality
  auto meta_vec = node->GatherSortedLiveMetadata(comp);

  EXPECT_EQ(rec_count, meta_vec.size());

  for (size_t index = 0; index < meta_vec.size(); index++) {
    EXPECT_EQ(written_keys[index], CastKey(meta_vec[index].first));
  }
}

TEST_F(LeafNodeUInt64Fixture, Consolidate_SortedTenKeysWithUpdate_GatherSortedLiveMetadata)
{
  // fill a node with ordered keys
  auto written_keys = WriteOrderedKeys(1, 9);
  node->Update(key_2nd_ptr, key_length_2nd, payload_null_ptr, payload_length_null, kIndexEpoch,
               comp, pool.get());

  // gather live metadata and check equality
  auto meta_vec = node->GatherSortedLiveMetadata(comp);

  EXPECT_EQ(rec_count, meta_vec.size());

  for (size_t index = 0; index < meta_vec.size(); index++) {
    EXPECT_EQ(written_keys[index], CastKey(meta_vec[index].first));
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

  EXPECT_EQ(rec_count, meta_vec.size());

  for (size_t index = 0; index < meta_vec.size(); index++) {
    EXPECT_EQ(written_keys[index], CastKey(meta_vec[index].first));
  }
}

TEST_F(LeafNodeUInt64Fixture, Consolidate_SortedTenKeys_NodeHasCorrectStatus)
{
  // prepare a consolidated node
  WriteOrderedKeys(1, 10);
  auto meta_vec = node->GatherSortedLiveMetadata(comp);
  node.reset(node->Consolidate(meta_vec));

  status = node->GetStatusWord();

  EXPECT_EQ(rec_count, node->GetSortedCount());
  EXPECT_FALSE(status.IsFrozen());
  EXPECT_EQ(rec_count, status.GetRecordCount());
  EXPECT_EQ(block_size, status.GetBlockSize());
  EXPECT_EQ(deleted_size, status.GetDeletedSize());
}

TEST_F(LeafNodeUInt64Fixture, Consolidate_SortedTenKeysWithDelete_NodeHasCorrectStatus)
{
  // prepare a consolidated node
  WriteOrderedKeys(1, 10);
  node->Delete(key_2nd_ptr, key_length_2nd, comp, pool.get());
  auto meta_vec = node->GatherSortedLiveMetadata(comp);
  node.reset(node->Consolidate(meta_vec));

  status = node->GetStatusWord();
  --rec_count;
  block_size -= key_length_2nd + payload_length_2nd;

  EXPECT_EQ(rec_count, node->GetSortedCount());
  EXPECT_FALSE(status.IsFrozen());
  EXPECT_EQ(rec_count, status.GetRecordCount());
  EXPECT_EQ(block_size, status.GetBlockSize());
  EXPECT_EQ(deleted_size, status.GetDeletedSize());
}

TEST_F(LeafNodeUInt64Fixture, Consolidate_SortedTenKeysWithUpdate_NodeHasCorrectStatus)
{
  // prepare a consolidated node
  WriteOrderedKeys(1, 9);
  node->Update(key_2nd_ptr, key_length_2nd, payload_null_ptr, payload_length_null, kIndexEpoch,
               comp, pool.get());
  auto meta_vec = node->GatherSortedLiveMetadata(comp);
  node.reset(node->Consolidate(meta_vec));

  status = node->GetStatusWord();

  EXPECT_EQ(rec_count, node->GetSortedCount());
  EXPECT_FALSE(status.IsFrozen());
  EXPECT_EQ(rec_count, status.GetRecordCount());
  EXPECT_EQ(block_size, status.GetBlockSize());
  EXPECT_EQ(deleted_size, status.GetDeletedSize());
}

TEST_F(LeafNodeUInt64Fixture, Consolidate_UnsortedTenKeys_NodeHasCorrectStatus)
{
  // prepare a consolidated node
  WriteOrderedKeys(5, 10);
  WriteOrderedKeys(1, 4);
  auto meta_vec = node->GatherSortedLiveMetadata(comp);
  node.reset(node->Consolidate(meta_vec));

  status = node->GetStatusWord();

  EXPECT_EQ(rec_count, node->GetSortedCount());
  EXPECT_FALSE(status.IsFrozen());
  EXPECT_EQ(rec_count, status.GetRecordCount());
  EXPECT_EQ(block_size, status.GetBlockSize());
  EXPECT_EQ(deleted_size, status.GetDeletedSize());
}

}  // namespace bztree
