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
 * CString unit tests
 *################################################################################################*/

class LeafNodeCStringFixture : public testing::Test
{
 protected:
  const char* key_1st;
  const char* key_2nd;
  const char* key_unknown;
  const char* key_1st_ptr;
  const char* key_2nd_ptr;
  const char* key_unknown_ptr;
  size_t key_length_1st;
  size_t key_length_2nd;
  const char* payload_1st;
  const char* payload_2nd;
  const char* payload_1st_ptr;
  const char* payload_2nd_ptr;
  size_t payload_length_1st;
  size_t payload_length_2nd;
  const char* word;
  const char* word_ptr;
  const char* null_word;
  const char* null_word_ptr;

  std::unique_ptr<pmwcas::DescriptorPool> pool;
  std::unique_ptr<LeafNode> node;
  CompareAsCString comp{};
  BaseNode::NodeReturnCode rc;
  StatusWord status;
  std::unique_ptr<std::byte[]> u_ptr;
  char* result;
  size_t rec_count, index, block_size, deleted_size;

  void
  SetUp() override
  {
    pmwcas::InitLibrary(pmwcas::DefaultAllocator::Create, pmwcas::DefaultAllocator::Destroy,
                        pmwcas::LinuxEnvironment::Create, pmwcas::LinuxEnvironment::Destroy);
    pool.reset(new pmwcas::DescriptorPool{1000, 1, false});
    node.reset(LeafNode::CreateEmptyNode(kDefaultNodeSize));

    key_1st = "123";
    key_2nd = "test";
    key_unknown = "unknown";
    key_1st_ptr = key_1st;
    key_2nd_ptr = key_2nd;
    key_unknown_ptr = key_unknown;
    key_length_1st = 4;
    key_length_2nd = 5;
    payload_1st = "4567";
    payload_2nd = "value";
    payload_1st_ptr = payload_1st;
    payload_2nd_ptr = payload_2nd;
    payload_length_1st = 5;
    payload_length_2nd = 6;
    word = "1234567";
    word_ptr = word;
    null_word = "0000000";
    null_word_ptr = null_word;
  }

  void
  TearDown() override
  {
    pmwcas::Thread::ClearRegistry();
  }

  char*
  GetResult()
  {
    return BitCast<char*>(u_ptr.get());
  }

  void
  FillNode()
  {
    for (size_t i = 0; i < 9; ++i) {
      node->Write(null_word_ptr, kWordLength, null_word_ptr, kWordLength, kIndexEpoch, pool.get());
    }
  }
};

/*##################################################################################################
 * Unsigned int 64 bits unit tests
 *################################################################################################*/

class LeafNodeUInt64Fixture : public testing::Test
{
 protected:
  uint64_t key_1st;
  uint64_t key_2nd;
  uint64_t key_11th;
  uint64_t key_null;
  uint64_t* key_1st_ptr;
  uint64_t* key_2nd_ptr;
  uint64_t* key_11th_ptr;
  uint64_t* key_null_ptr;
  size_t key_length_1st;
  size_t key_length_2nd;
  size_t key_length_11th;
  size_t key_length_null;
  uint64_t payload_1st;
  uint64_t payload_2nd;
  uint64_t payload_11th;
  uint64_t payload_null;
  uint64_t* payload_1st_ptr;
  uint64_t* payload_2nd_ptr;
  uint64_t* payload_11th_ptr;
  uint64_t* payload_null_ptr;
  size_t payload_length_1st;
  size_t payload_length_2nd;
  size_t payload_length_11th;
  size_t payload_length_null;

  std::unique_ptr<pmwcas::DescriptorPool> pool;
  std::unique_ptr<LeafNode> node;
  CompareAsUInt64 comp{};
  BaseNode::NodeReturnCode rc;
  StatusWord status;
  std::unique_ptr<std::byte[]> u_ptr;
  uint64_t result;
  size_t rec_count, index, block_size, deleted_size;

  void
  SetUp() override
  {
    pmwcas::InitLibrary(pmwcas::DefaultAllocator::Create, pmwcas::DefaultAllocator::Destroy,
                        pmwcas::LinuxEnvironment::Create, pmwcas::LinuxEnvironment::Destroy);
    pool.reset(new pmwcas::DescriptorPool{1000, 1, false});
    node.reset(LeafNode::CreateEmptyNode(kDefaultNodeSize));

    key_1st = 1;
    key_2nd = 2;
    key_11th = 11;
    key_null = 0;  // null key must have 8 bytes to fill a node
    key_1st_ptr = &key_1st;
    key_2nd_ptr = &key_2nd;
    key_11th_ptr = &key_11th;
    key_null_ptr = &key_null;
    key_length_1st = kWordLength;
    key_length_2nd = kWordLength;
    key_length_11th = kWordLength;
    key_length_null = kWordLength;  // null key must have 8 bytes to fill a node
    payload_1st = 1;
    payload_2nd = 2;
    payload_11th = 11;
    payload_null = 0;  // null payload must have 8 bytes to fill a node
    payload_1st_ptr = &payload_1st;
    payload_2nd_ptr = &payload_2nd;
    payload_11th_ptr = &payload_11th;
    payload_null_ptr = &payload_null;
    payload_length_1st = kWordLength;
    payload_length_2nd = kWordLength;
    payload_length_11th = kWordLength;
    payload_length_null = kWordLength;  // null payload must have 8 bytes to fill a node
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

  void
  WriteNullKey(const size_t write_num)
  {
    for (size_t index = 0; index < write_num; ++index) {
      node->Write(key_null_ptr, key_length_null, payload_null_ptr, payload_length_null, kIndexEpoch,
                  pool.get());
    }
  }

  void
  WriteOrderedKeys(  //
      const size_t begin_index,
      const size_t end_index)
  {
    for (size_t index = begin_index; index < end_index; ++index) {
      auto key = index;
      auto key_ptr = &key;
      auto key_length = kWordLength;
      auto value = index;
      auto value_ptr = &value;
      auto value_length = kWordLength;
      node->Write(key_ptr, key_length, value_ptr, value_length, kIndexEpoch, pool.get());
    }
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

  ASSERT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  ASSERT_EQ(status, node->GetStatusWord());
  EXPECT_TRUE(node->RecordIsVisible(index));
  EXPECT_FALSE(node->RecordIsDeleted(index));
  EXPECT_EQ(key_length_1st, node->GetKeyLength(index));
  EXPECT_EQ(payload_length_1st, node->GetPayloadLength(index));
  EXPECT_FALSE(status.IsFrozen());
  EXPECT_EQ(rec_count, status.GetRecordCount());
  EXPECT_EQ(block_size, status.GetBlockSize());
  EXPECT_EQ(0, status.GetDeletedSize());

  std::tie(rc, status) = node->Write(key_2nd_ptr, key_length_2nd, payload_2nd_ptr,
                                     payload_length_2nd, kIndexEpoch, pool.get());
  ++rec_count;
  ++index;
  block_size += key_length_2nd + payload_length_2nd;

  ASSERT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  ASSERT_EQ(status, node->GetStatusWord());
  EXPECT_TRUE(node->RecordIsVisible(index));
  EXPECT_FALSE(node->RecordIsDeleted(index));
  EXPECT_EQ(key_length_2nd, node->GetKeyLength(index));
  EXPECT_EQ(payload_length_2nd, node->GetPayloadLength(index));
  EXPECT_FALSE(status.IsFrozen());
  EXPECT_EQ(rec_count, status.GetRecordCount());
  EXPECT_EQ(block_size, status.GetBlockSize());
  EXPECT_EQ(0, status.GetDeletedSize());
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

  ASSERT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(payload_1st, result);

  // read 2nd input value
  std::tie(rc, u_ptr) = node->Read(key_2nd_ptr, comp);
  result = GetResult();

  ASSERT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
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

  ASSERT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
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

  ASSERT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  ASSERT_EQ(status, node->GetStatusWord());
  EXPECT_TRUE(node->RecordIsVisible(index));
  EXPECT_FALSE(node->RecordIsDeleted(index));
  EXPECT_EQ(key_length_1st, node->GetKeyLength(index));
  EXPECT_EQ(payload_length_1st, node->GetPayloadLength(index));
  EXPECT_FALSE(status.IsFrozen());
  EXPECT_EQ(rec_count, status.GetRecordCount());
  EXPECT_EQ(block_size, status.GetBlockSize());
  EXPECT_EQ(0, status.GetDeletedSize());

  std::tie(rc, status) = node->Insert(key_2nd_ptr, key_length_2nd, payload_2nd_ptr,
                                      payload_length_2nd, kIndexEpoch, comp, pool.get());
  ++rec_count;
  ++index;
  block_size += key_length_2nd + payload_length_2nd;

  ASSERT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  ASSERT_EQ(status, node->GetStatusWord());
  EXPECT_TRUE(node->RecordIsVisible(index));
  EXPECT_FALSE(node->RecordIsDeleted(index));
  EXPECT_EQ(key_length_2nd, node->GetKeyLength(index));
  EXPECT_EQ(payload_length_2nd, node->GetPayloadLength(index));
  EXPECT_FALSE(status.IsFrozen());
  EXPECT_EQ(rec_count, status.GetRecordCount());
  EXPECT_EQ(block_size, status.GetBlockSize());
  EXPECT_EQ(0, status.GetDeletedSize());
}

TEST_F(LeafNodeUInt64Fixture, Insert_TwoKeys_ReadWrittenValues)
{
  node->Insert(key_1st_ptr, key_length_1st, payload_1st_ptr, payload_length_1st, kIndexEpoch, comp,
               pool.get());
  node->Insert(key_2nd_ptr, key_length_2nd, payload_2nd_ptr, payload_length_2nd, kIndexEpoch, comp,
               pool.get());

  // read 1st input value
  std::tie(rc, u_ptr) = node->Read(key_1st_ptr, comp);
  result = GetResult();

  ASSERT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(payload_1st, result);

  // read 2nd input value
  std::tie(rc, u_ptr) = node->Read(key_2nd_ptr, comp);
  result = GetResult();

  ASSERT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(payload_2nd, result);
}

TEST_F(LeafNodeUInt64Fixture, Insert_DuplicateKey_InsertionFailed)
{
  node->Insert(key_1st_ptr, key_length_1st, payload_1st_ptr, payload_length_1st, kIndexEpoch, comp,
               pool.get());

  std::tie(rc, status) = node->Insert(key_1st_ptr, key_length_1st, payload_1st_ptr,
                                      payload_length_1st, kIndexEpoch, comp, pool.get());
  ASSERT_EQ(BaseNode::NodeReturnCode::kKeyExist, rc);
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

  ASSERT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  ASSERT_EQ(status, node->GetStatusWord());
  EXPECT_TRUE(node->RecordIsVisible(index));
  EXPECT_FALSE(node->RecordIsDeleted(index));
  EXPECT_EQ(payload_length_2nd, node->GetPayloadLength(index));
  EXPECT_FALSE(status.IsFrozen());
  EXPECT_EQ(rec_count, status.GetRecordCount());
  EXPECT_EQ(block_size, status.GetBlockSize());
  EXPECT_EQ(0, status.GetDeletedSize());
}

TEST_F(LeafNodeUInt64Fixture, Update_SingleKey_ReadUpdatedValue)
{
  node->Insert(key_1st_ptr, key_length_1st, payload_2nd_ptr, payload_length_2nd, kIndexEpoch, comp,
               pool.get());
  node->Update(key_1st_ptr, key_length_1st, payload_2nd_ptr, payload_length_2nd, kIndexEpoch, comp,
               pool.get());

  std::tie(rc, u_ptr) = node->Read(key_1st_ptr, comp);
  result = GetResult();

  ASSERT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(payload_2nd, result);
}

TEST_F(LeafNodeUInt64Fixture, Update_NotPresentKey_UpdatedFailed)
{
  std::tie(rc, status) = node->Update(key_1st_ptr, key_length_1st, payload_1st_ptr,
                                      payload_length_1st, kIndexEpoch, comp, pool.get());

  ASSERT_EQ(BaseNode::NodeReturnCode::kKeyNotExist, rc);
}

TEST_F(LeafNodeUInt64Fixture, Update_DeletedKey_UpdateFailed)
{
  node->Insert(key_1st_ptr, key_length_1st, payload_2nd_ptr, payload_length_2nd, kIndexEpoch, comp,
               pool.get());
  node->Delete(key_1st_ptr, key_length_1st, comp, pool.get());
  std::tie(rc, status) = node->Update(key_1st_ptr, key_length_1st, payload_2nd_ptr,
                                      payload_length_2nd, kIndexEpoch, comp, pool.get());

  ASSERT_EQ(BaseNode::NodeReturnCode::kKeyNotExist, rc);
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

  ASSERT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  ASSERT_EQ(status, node->GetStatusWord());
  EXPECT_FALSE(node->RecordIsVisible(0));
  EXPECT_TRUE(node->RecordIsDeleted(0));
  EXPECT_FALSE(status.IsFrozen());
  EXPECT_EQ(rec_count, status.GetRecordCount());
  EXPECT_EQ(block_size, status.GetBlockSize());
  EXPECT_EQ(deleted_size, status.GetDeletedSize());

  std::tie(rc, status) = node->Delete(key_2nd_ptr, key_length_2nd, comp, pool.get());
  deleted_size += key_length_2nd + payload_length_2nd;

  ASSERT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  ASSERT_EQ(status, node->GetStatusWord());
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

  ASSERT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
}

TEST_F(LeafNodeUInt64Fixture, Delete_NotPresentKey_DeletionFailed)
{
  std::tie(rc, status) = node->Delete(key_1st_ptr, key_length_1st, comp, pool.get());

  ASSERT_EQ(BaseNode::NodeReturnCode::kKeyNotExist, rc);
}

TEST_F(LeafNodeUInt64Fixture, Delete_DeletedKey_DeletionFailed)
{
  node->Insert(key_1st_ptr, key_length_1st, payload_1st_ptr, payload_length_1st, kIndexEpoch, comp,
               pool.get());
  node->Delete(key_1st_ptr, key_length_1st, comp, pool.get());

  std::tie(rc, status) = node->Delete(key_1st_ptr, key_length_1st, comp, pool.get());

  ASSERT_EQ(BaseNode::NodeReturnCode::kKeyNotExist, rc);
}

TEST_F(LeafNodeUInt64Fixture, Delete_FilledNode_GetCorrectReturnCodes)
{
  WriteNullKey(10);

  std::tie(rc, status) = node->Delete(key_null_ptr, key_length_null, comp, pool.get());

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
}

/*--------------------------------------------------------------------------------------------------
 * Consolide operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(LeafNodeUInt64Fixture, Consolidate_TenKeys_GatherSortedLiveMetadata)
{
  //
}

}  // namespace bztree
