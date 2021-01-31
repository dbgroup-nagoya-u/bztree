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

TEST_F(LeafNodeCStringFixture, New_EmptyNode_CorrectlyInitialized)
{
  EXPECT_EQ(kWordLength, node->GetStatusWordOffsetForTest());
  EXPECT_EQ(kWordLength, node->GetMetadataOffsetForTest());
  EXPECT_EQ(kDefaultNodeSize, node->GetNodeSize());
  EXPECT_EQ(0, node->GetSortedCount());
}

/*--------------------------------------------------------------------------------------------------
 * Write operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(LeafNodeCStringFixture, Write_StringValues_MetadataCorrectlyUpdated)
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

TEST_F(LeafNodeCStringFixture, Write_StringValues_ReadWrittenValue)
{
  node->Write(key_1st_ptr, key_length_1st, payload_1st_ptr, payload_length_1st, kIndexEpoch,
              pool.get());
  node->Write(key_2nd_ptr, key_length_2nd, payload_2nd_ptr, payload_length_2nd, kIndexEpoch,
              pool.get());

  // read 1st input value
  std::tie(rc, u_ptr) = node->Read(key_1st_ptr, comp);
  result = GetResult();
  ASSERT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_STREQ(payload_1st, result);

  // read 2nd input value
  std::tie(rc, u_ptr) = node->Read(key_2nd_ptr, comp);
  result = GetResult();
  ASSERT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_STREQ(payload_2nd, result);

  // read latest values
  node->Write(key_1st_ptr, key_length_1st, payload_2nd_ptr, payload_length_2nd, kIndexEpoch,
              pool.get());
  node->Write(key_2nd_ptr, key_length_2nd, payload_1st_ptr, payload_length_1st, kIndexEpoch,
              pool.get());
  std::tie(rc, u_ptr) = node->Read(key_1st_ptr, comp);
  result = GetResult();
  ASSERT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_STREQ(payload_2nd, result);
  std::tie(rc, u_ptr) = node->Read(key_2nd_ptr, comp);
  result = GetResult();
  ASSERT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_STREQ(payload_1st, result);
}

TEST_F(LeafNodeCStringFixture, Write_AlmostFilled_GetCorrectReturnCodes)
{
  FillNode();

  std::tie(rc, status) =
      node->Write(word_ptr, kWordLength, word_ptr, kWordLength, kIndexEpoch, pool.get());
  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(kDefaultNodeSize, status.GetOccupiedSize());

  std::tie(rc, status) = node->Write(key_1st_ptr, key_length_1st, payload_1st_ptr,
                                     payload_length_1st, kIndexEpoch, pool.get());
  EXPECT_EQ(BaseNode::NodeReturnCode::kNoSpace, rc);
}

/*--------------------------------------------------------------------------------------------------
 * Insert operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(LeafNodeCStringFixture, Insert_StringValues_MetadataCorrectlyUpdated)
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

TEST_F(LeafNodeCStringFixture, Insert_StringValues_ReadWrittenValue)
{
  node->Insert(key_1st_ptr, key_length_1st, payload_1st_ptr, payload_length_1st, kIndexEpoch, comp,
               pool.get());
  node->Insert(key_2nd_ptr, key_length_2nd, payload_2nd_ptr, payload_length_2nd, kIndexEpoch, comp,
               pool.get());

  // abort due to inserting duplicated values
  std::tie(rc, status) = node->Insert(key_1st_ptr, key_length_1st, payload_1st_ptr,
                                      payload_length_1st, kIndexEpoch, comp, pool.get());
  ASSERT_EQ(BaseNode::NodeReturnCode::kKeyExist, rc);
  std::tie(rc, status) = node->Insert(key_2nd_ptr, key_length_2nd, payload_2nd_ptr,
                                      payload_length_2nd, kIndexEpoch, comp, pool.get());
  ASSERT_EQ(BaseNode::NodeReturnCode::kKeyExist, rc);

  // read 1st input value
  std::tie(rc, u_ptr) = node->Read(key_1st_ptr, comp);
  result = GetResult();
  ASSERT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_STREQ(payload_1st, result);

  // read 2nd input value
  std::tie(rc, u_ptr) = node->Read(key_2nd_ptr, comp);
  result = GetResult();
  ASSERT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_STREQ(payload_2nd, result);
}

TEST_F(LeafNodeCStringFixture, Insert_AlmostFilled_GetCorrectReturnCodes)
{
  FillNode();

  std::tie(rc, status) =
      node->Insert(word_ptr, kWordLength, word_ptr, kWordLength, kIndexEpoch, comp, pool.get());
  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(kDefaultNodeSize, status.GetOccupiedSize());

  std::tie(rc, status) = node->Insert(key_1st_ptr, key_length_1st, payload_1st_ptr,
                                      payload_length_1st, kIndexEpoch, comp, pool.get());
  EXPECT_EQ(BaseNode::NodeReturnCode::kNoSpace, rc);

  std::tie(rc, status) =
      node->Insert(word_ptr, kWordLength, word_ptr, kWordLength, kIndexEpoch, comp, pool.get());
  EXPECT_EQ(BaseNode::NodeReturnCode::kKeyExist, rc);
}

/*--------------------------------------------------------------------------------------------------
 * Update operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(LeafNodeCStringFixture, Update_StringValues_MetadataCorrectlyUpdated)
{
  std::tie(rc, status) = node->Insert(key_1st_ptr, key_length_1st, payload_1st_ptr,
                                      payload_length_1st, kIndexEpoch, comp, pool.get());
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

TEST_F(LeafNodeCStringFixture, Update_StringValues_ReadWrittenValue)
{
  // abort due to update not exist keys
  std::tie(rc, status) = node->Update(key_1st_ptr, key_length_1st, payload_1st_ptr,
                                      payload_length_1st, kIndexEpoch, comp, pool.get());
  ASSERT_EQ(BaseNode::NodeReturnCode::kKeyNotExist, rc);

  // insert and update value
  node->Insert(key_1st_ptr, key_length_1st, payload_2nd_ptr, payload_length_2nd, kIndexEpoch, comp,
               pool.get());
  node->Update(key_1st_ptr, key_length_1st, payload_2nd_ptr, payload_length_2nd, kIndexEpoch, comp,
               pool.get());

  // read latest values
  std::tie(rc, u_ptr) = node->Read(key_1st_ptr, comp);
  result = GetResult();
  ASSERT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_STREQ(payload_2nd, result);
}

TEST_F(LeafNodeCStringFixture, Update_AlmostFilled_GetCorrectReturnCodes)
{
  FillNode();

  std::tie(rc, status) = node->Update(null_word_ptr, kWordLength, word_ptr, kWordLength,
                                      kIndexEpoch, comp, pool.get());
  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(kDefaultNodeSize, status.GetOccupiedSize());

  std::tie(rc, status) = node->Update(null_word_ptr, kWordLength, payload_1st_ptr,
                                      payload_length_1st, kIndexEpoch, comp, pool.get());
  EXPECT_EQ(BaseNode::NodeReturnCode::kNoSpace, rc);

  std::tie(rc, status) =
      node->Update(word_ptr, kWordLength, word_ptr, kWordLength, kIndexEpoch, comp, pool.get());
  EXPECT_EQ(BaseNode::NodeReturnCode::kKeyNotExist, rc);
}

/*--------------------------------------------------------------------------------------------------
 * Delete operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(LeafNodeCStringFixture, Delete_StringValues_MetadataCorrectlyUpdated)
{
  std::tie(rc, status) = node->Insert(key_1st_ptr, key_length_1st, payload_1st_ptr,
                                      payload_length_1st, kIndexEpoch, comp, pool.get());
  std::tie(rc, status) = node->Insert(key_2nd_ptr, key_length_2nd, payload_2nd_ptr,
                                      payload_length_2nd, kIndexEpoch, comp, pool.get());
  std::tie(rc, status) = node->Delete(key_1st_ptr, key_length_1st, comp, pool.get());

  rec_count = 1;
  block_size = key_length_1st + payload_length_1st + key_length_2nd + payload_length_2nd;
  deleted_size = key_length_1st + payload_length_1st;

  ASSERT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  ASSERT_EQ(status, node->GetStatusWord());
  EXPECT_FALSE(node->RecordIsVisible(0));
  EXPECT_TRUE(node->RecordIsDeleted(0));
  EXPECT_EQ(payload_length_2nd, node->GetPayloadLength(1));
  EXPECT_FALSE(status.IsFrozen());
  EXPECT_EQ(rec_count, status.GetRecordCount());
  EXPECT_EQ(block_size, status.GetBlockSize());
  EXPECT_EQ(deleted_size, status.GetDeletedSize());
}

TEST_F(LeafNodeCStringFixture, Delete_StringValues_UnReadDeletedValue)
{
  // abort due to delete not exist keys
  std::tie(rc, status) = node->Delete(key_1st_ptr, key_length_1st, comp, pool.get());
  ASSERT_EQ(BaseNode::NodeReturnCode::kKeyNotExist, rc);

  // insert and update value
  node->Insert(key_1st_ptr, key_length_1st, payload_2nd_ptr, payload_length_2nd, kIndexEpoch, comp,
               pool.get());
  node->Delete(key_1st_ptr, key_length_1st, comp, pool.get());

  // read latest values
  std::tie(rc, u_ptr) = node->Read(key_1st_ptr, comp);
  ASSERT_EQ(BaseNode::NodeReturnCode::kKeyNotExist, rc);

  // check double-delete
  std::tie(rc, status) = node->Delete(key_1st_ptr, key_length_1st, comp, pool.get());
  ASSERT_EQ(BaseNode::NodeReturnCode::kKeyNotExist, rc);
}

/*##################################################################################################
 * Unsigned int 64 bits unit tests
 *################################################################################################*/

class LeafNodeUInt64Fixture : public testing::Test
{
 protected:
  uint64_t key_1st;
  uint64_t key_2nd;
  uint64_t key_unknown;
  uint64_t* key_1st_ptr;
  uint64_t* key_2nd_ptr;
  uint64_t* key_unknown_ptr;
  size_t key_length_1st;
  size_t key_length_2nd;
  uint64_t payload_1st;
  uint64_t payload_2nd;
  uint64_t* payload_1st_ptr;
  uint64_t* payload_2nd_ptr;
  size_t payload_length_1st;
  size_t payload_length_2nd;
  uint64_t word;
  uint64_t* word_ptr;
  uint64_t null_word;
  uint64_t* null_word_ptr;

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

    key_1st = 123;
    key_2nd = 890;
    key_unknown = 0;
    key_1st_ptr = &key_1st;
    key_2nd_ptr = &key_2nd;
    key_unknown_ptr = &key_unknown;
    key_length_1st = kWordLength;
    key_length_2nd = kWordLength;
    payload_1st = 4567;
    payload_2nd = 1234;
    payload_1st_ptr = &payload_1st;
    payload_2nd_ptr = &payload_2nd;
    payload_length_1st = kWordLength;
    payload_length_2nd = kWordLength;
    word = 1;
    word_ptr = &word;
    null_word = 0;
    null_word_ptr = &null_word;
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
  FillNode()
  {
    for (size_t i = 0; i < 9; ++i) {
      node->Write(null_word_ptr, kWordLength, null_word_ptr, kWordLength, kIndexEpoch, pool.get());
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

TEST_F(LeafNodeUInt64Fixture, Write_UIntValues_MetadataCorrectlyUpdated)
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

TEST_F(LeafNodeUInt64Fixture, Write_UIntValues_ReadWrittenValue)
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

  // read latest values
  node->Write(key_1st_ptr, key_length_1st, payload_2nd_ptr, payload_length_2nd, kIndexEpoch,
              pool.get());
  node->Write(key_2nd_ptr, key_length_2nd, payload_1st_ptr, payload_length_1st, kIndexEpoch,
              pool.get());
  std::tie(rc, u_ptr) = node->Read(key_1st_ptr, comp);
  result = GetResult();
  ASSERT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(payload_2nd, result);
  std::tie(rc, u_ptr) = node->Read(key_2nd_ptr, comp);
  result = GetResult();
  ASSERT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(payload_1st, result);
}

TEST_F(LeafNodeUInt64Fixture, Write_AlmostFilled_GetCorrectReturnCodes)
{
  FillNode();

  std::tie(rc, status) =
      node->Write(word_ptr, kWordLength, word_ptr, kWordLength, kIndexEpoch, pool.get());
  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);

  std::tie(rc, status) = node->Write(key_1st_ptr, key_length_1st, payload_1st_ptr,
                                     payload_length_1st, kIndexEpoch, pool.get());
  EXPECT_EQ(BaseNode::NodeReturnCode::kNoSpace, rc);
}

/*--------------------------------------------------------------------------------------------------
 * Insert operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(LeafNodeUInt64Fixture, Insert_UIntValues_MetadataCorrectlyUpdated)
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

TEST_F(LeafNodeUInt64Fixture, Insert_UIntValues_ReadWrittenValue)
{
  node->Insert(key_1st_ptr, key_length_1st, payload_1st_ptr, payload_length_1st, kIndexEpoch, comp,
               pool.get());
  node->Insert(key_2nd_ptr, key_length_2nd, payload_2nd_ptr, payload_length_2nd, kIndexEpoch, comp,
               pool.get());

  // abort due to inserting duplicated values
  std::tie(rc, status) = node->Insert(key_1st_ptr, key_length_1st, payload_1st_ptr,
                                      payload_length_1st, kIndexEpoch, comp, pool.get());
  ASSERT_EQ(BaseNode::NodeReturnCode::kKeyExist, rc);
  std::tie(rc, status) = node->Insert(key_2nd_ptr, key_length_2nd, payload_2nd_ptr,
                                      payload_length_2nd, kIndexEpoch, comp, pool.get());
  ASSERT_EQ(BaseNode::NodeReturnCode::kKeyExist, rc);

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

TEST_F(LeafNodeUInt64Fixture, Insert_AlmostFilled_GetCorrectReturnCodes)
{
  FillNode();

  std::tie(rc, status) =
      node->Insert(word_ptr, kWordLength, word_ptr, kWordLength, kIndexEpoch, comp, pool.get());
  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(kDefaultNodeSize, status.GetOccupiedSize());

  std::tie(rc, status) = node->Insert(key_1st_ptr, key_length_1st, payload_1st_ptr,
                                      payload_length_1st, kIndexEpoch, comp, pool.get());
  EXPECT_EQ(BaseNode::NodeReturnCode::kNoSpace, rc);

  std::tie(rc, status) =
      node->Insert(word_ptr, kWordLength, word_ptr, kWordLength, kIndexEpoch, comp, pool.get());
  EXPECT_EQ(BaseNode::NodeReturnCode::kKeyExist, rc);
}

/*--------------------------------------------------------------------------------------------------
 * Update operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(LeafNodeUInt64Fixture, Update_UIntValues_MetadataCorrectlyUpdated)
{
  std::tie(rc, status) = node->Insert(key_1st_ptr, key_length_1st, payload_1st_ptr,
                                      payload_length_1st, kIndexEpoch, comp, pool.get());
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

TEST_F(LeafNodeUInt64Fixture, Update_UIntValues_ReadWrittenValue)
{
  // abort due to update not exist keys
  std::tie(rc, status) = node->Update(key_1st_ptr, key_length_1st, payload_1st_ptr,
                                      payload_length_1st, kIndexEpoch, comp, pool.get());
  ASSERT_EQ(BaseNode::NodeReturnCode::kKeyNotExist, rc);

  // insert and update value
  node->Insert(key_1st_ptr, key_length_1st, payload_2nd_ptr, payload_length_2nd, kIndexEpoch, comp,
               pool.get());
  node->Update(key_1st_ptr, key_length_1st, payload_2nd_ptr, payload_length_2nd, kIndexEpoch, comp,
               pool.get());

  // read latest values
  std::tie(rc, u_ptr) = node->Read(key_1st_ptr, comp);
  result = GetResult();
  ASSERT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(payload_2nd, result);
}

TEST_F(LeafNodeUInt64Fixture, Update_AlmostFilled_GetCorrectReturnCodes)
{
  FillNode();

  std::tie(rc, status) = node->Update(null_word_ptr, kWordLength, word_ptr, kWordLength,
                                      kIndexEpoch, comp, pool.get());
  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(kDefaultNodeSize, status.GetOccupiedSize());

  std::tie(rc, status) = node->Update(null_word_ptr, kWordLength, payload_1st_ptr,
                                      payload_length_1st, kIndexEpoch, comp, pool.get());
  EXPECT_EQ(BaseNode::NodeReturnCode::kNoSpace, rc);

  std::tie(rc, status) =
      node->Update(word_ptr, kWordLength, word_ptr, kWordLength, kIndexEpoch, comp, pool.get());
  EXPECT_EQ(BaseNode::NodeReturnCode::kKeyNotExist, rc);
}

/*--------------------------------------------------------------------------------------------------
 * Delete operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(LeafNodeUInt64Fixture, Delete_UIntValues_MetadataCorrectlyUpdated)
{
  std::tie(rc, status) = node->Insert(key_1st_ptr, key_length_1st, payload_1st_ptr,
                                      payload_length_1st, kIndexEpoch, comp, pool.get());
  std::tie(rc, status) = node->Insert(key_2nd_ptr, key_length_2nd, payload_2nd_ptr,
                                      payload_length_2nd, kIndexEpoch, comp, pool.get());
  std::tie(rc, status) = node->Delete(key_1st_ptr, key_length_1st, comp, pool.get());

  rec_count = 1;
  block_size = key_length_1st + payload_length_1st + key_length_2nd + payload_length_2nd;
  deleted_size = key_length_1st + payload_length_1st;

  ASSERT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  ASSERT_EQ(status, node->GetStatusWord());
  EXPECT_FALSE(node->RecordIsVisible(0));
  EXPECT_TRUE(node->RecordIsDeleted(0));
  EXPECT_EQ(payload_length_2nd, node->GetPayloadLength(1));
  EXPECT_FALSE(status.IsFrozen());
  EXPECT_EQ(rec_count, status.GetRecordCount());
  EXPECT_EQ(block_size, status.GetBlockSize());
  EXPECT_EQ(deleted_size, status.GetDeletedSize());
}

TEST_F(LeafNodeUInt64Fixture, Delete_UIntValues_UnReadDeletedValue)
{
  // abort due to delete not exist keys
  std::tie(rc, status) = node->Delete(key_1st_ptr, key_length_1st, comp, pool.get());
  ASSERT_EQ(BaseNode::NodeReturnCode::kKeyNotExist, rc);

  // insert and update value
  node->Insert(key_1st_ptr, key_length_1st, payload_2nd_ptr, payload_length_2nd, kIndexEpoch, comp,
               pool.get());
  node->Delete(key_1st_ptr, key_length_1st, comp, pool.get());

  // read latest values
  std::tie(rc, u_ptr) = node->Read(key_1st_ptr, comp);
  ASSERT_EQ(BaseNode::NodeReturnCode::kKeyNotExist, rc);

  // check double-delete
  std::tie(rc, status) = node->Delete(key_1st_ptr, key_length_1st, comp, pool.get());
  ASSERT_EQ(BaseNode::NodeReturnCode::kKeyNotExist, rc);
}

TEST_F(LeafNodeUInt64Fixture, Delete_AlmostFilled_GetCorrectReturnCodes)
}

}  // namespace bztree
