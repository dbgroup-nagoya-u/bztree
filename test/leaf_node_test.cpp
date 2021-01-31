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

class LeafNodeFixture : public testing::Test
{
 protected:
  std::unique_ptr<pmwcas::DescriptorPool> pool;
  std::unique_ptr<LeafNode> node;

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
};

TEST_F(LeafNodeFixture, New_EmptyNode_CorrectlyInitialized)
{
  EXPECT_EQ(kWordLength, node->GetStatusWordOffsetForTest());
  EXPECT_EQ(kWordLength, node->GetMetadataOffsetForTest());
  EXPECT_EQ(kDefaultNodeSize, node->GetNodeSize());
  EXPECT_EQ(0, node->GetSortedCount());
}

TEST_F(LeafNodeFixture, Write_StringValues_MetadataCorrectlyUpdated)
{
  auto key = "123", payload = "4567";
  auto key_length = 4, payload_length = 5;
  auto [rc, status] = node->Write(CastToBytePtr(key), key_length, CastToBytePtr(payload),
                                  payload_length, kIndexEpoch, pool.get());
  auto rec_count = 1, index = 0, block_size = key_length + payload_length;

  ASSERT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(rec_count, node->GetRecordCount());
  EXPECT_FALSE(node->IsFrozen());
  EXPECT_TRUE(node->RecordIsVisible(index));
  EXPECT_FALSE(node->RecordIsDeleted(index));
  EXPECT_EQ(key_length, node->GetKeyLength(index));
  EXPECT_EQ(payload_length, node->GetPayloadLength(index));
  EXPECT_FALSE(status.IsFrozen());
  EXPECT_EQ(rec_count, status.GetRecordCount());
  EXPECT_EQ(block_size, status.GetBlockSize());
  EXPECT_EQ(0, status.GetDeletedSize());

  key = "test", payload = "value";
  key_length = 6, payload_length = 6;
  std::tie(rc, status) = node->Write(CastToBytePtr(key), key_length, CastToBytePtr(payload),
                                     payload_length, kIndexEpoch, pool.get());
  ++rec_count;
  ++index;
  block_size += key_length + payload_length;

  ASSERT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(rec_count, node->GetRecordCount());
  EXPECT_FALSE(node->IsFrozen());
  EXPECT_TRUE(node->RecordIsVisible(index));
  EXPECT_FALSE(node->RecordIsDeleted(index));
  EXPECT_EQ(key_length, node->GetKeyLength(index));
  EXPECT_EQ(payload_length, node->GetPayloadLength(index));
  EXPECT_FALSE(status.IsFrozen());
  EXPECT_EQ(rec_count, status.GetRecordCount());
  EXPECT_EQ(block_size, status.GetBlockSize());
  EXPECT_EQ(0, status.GetDeletedSize());
}

TEST_F(LeafNodeFixture, Write_StringValues_ReadWrittenValue)
{
  const auto first_key = "123", first_payload = "4567";
  const auto first_key_length = 4, first_payload_length = 5;
  const auto second_key = "test", second_payload = "value";
  const auto second_key_length = 6, second_payload_length = 6;

  node->Write(CastToBytePtr(first_key), first_key_length, CastToBytePtr(first_payload),
              first_payload_length, kIndexEpoch, pool.get());
  node->Write(CastToBytePtr(second_key), second_key_length, CastToBytePtr(second_payload),
              second_payload_length, kIndexEpoch, pool.get());

  auto [rc, u_ptr] = node->Read(CastToBytePtr(first_key), CompareAsCString{});
  auto result = CastToCString(u_ptr.get());

  ASSERT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_STREQ(first_payload, result);

  std::tie(rc, u_ptr) = node->Read(CastToBytePtr(second_key), CompareAsCString{});
  result = CastToCString(u_ptr.get());

  ASSERT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_STREQ(second_payload, result);

  std::tie(rc, u_ptr) = node->Read(CastToBytePtr("unknown"), CompareAsCString{});

  ASSERT_EQ(BaseNode::NodeReturnCode::kKeyNotExist, rc);
}

TEST_F(LeafNodeFixture, Write_UIntValues_MetadataCorrectlyUpdated)
{
  auto key = 123UL, payload = 4567UL;
  auto key_length = kWordLength, payload_length = kWordLength;
  auto [rc, status] = node->Write(CastToBytePtr(&key), key_length, CastToBytePtr(&payload),
                                  payload_length, kIndexEpoch, pool.get());
  auto rec_count = 1UL, index = 0UL, block_size = key_length + payload_length;

  ASSERT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(rec_count, node->GetRecordCount());
  EXPECT_FALSE(node->IsFrozen());
  EXPECT_TRUE(node->RecordIsVisible(index));
  EXPECT_FALSE(node->RecordIsDeleted(index));
  EXPECT_EQ(key_length, node->GetKeyLength(index));
  EXPECT_EQ(payload_length, node->GetPayloadLength(index));
  EXPECT_FALSE(status.IsFrozen());
  EXPECT_EQ(rec_count, status.GetRecordCount());
  EXPECT_EQ(block_size, status.GetBlockSize());
  EXPECT_EQ(0, status.GetDeletedSize());

  key = 890UL, payload = 1234UL;
  std::tie(rc, status) = node->Write(CastToBytePtr(&key), key_length, CastToBytePtr(&payload),
                                     payload_length, kIndexEpoch, pool.get());
  ++rec_count;
  ++index;
  block_size += key_length + payload_length;

  ASSERT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(rec_count, node->GetRecordCount());
  EXPECT_FALSE(node->IsFrozen());
  EXPECT_TRUE(node->RecordIsVisible(index));
  EXPECT_FALSE(node->RecordIsDeleted(index));
  EXPECT_EQ(key_length, node->GetKeyLength(index));
  EXPECT_EQ(payload_length, node->GetPayloadLength(index));
  EXPECT_FALSE(status.IsFrozen());
  EXPECT_EQ(rec_count, status.GetRecordCount());
  EXPECT_EQ(block_size, status.GetBlockSize());
  EXPECT_EQ(0, status.GetDeletedSize());
}

TEST_F(LeafNodeFixture, Write_UIntValues_ReadWrittenValue)
{
  const auto first_key = 123UL, first_payload = 4567UL;
  const auto key_length = kWordLength, payload_length = kWordLength;
  const auto second_key = 890UL, second_payload = 1234UL;

  node->Write(CastToBytePtr(&first_key), key_length, CastToBytePtr(&first_payload), payload_length,
              kIndexEpoch, pool.get());
  node->Write(CastToBytePtr(&second_key), key_length, CastToBytePtr(&second_payload),
              payload_length, kIndexEpoch, pool.get());

  auto [rc, u_ptr] = node->Read(CastToBytePtr(&first_key), CompareAsCString{});
  auto result = CastToUint64(u_ptr.get());

  ASSERT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(first_payload, result);

  std::tie(rc, u_ptr) = node->Read(CastToBytePtr(&second_key), CompareAsCString{});
  result = CastToUint64(u_ptr.get());

  ASSERT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(second_payload, result);

  const auto unkown_key = 999UL;
  std::tie(rc, u_ptr) = node->Read(CastToBytePtr(&unkown_key), CompareAsCString{});

  ASSERT_EQ(BaseNode::NodeReturnCode::kKeyNotExist, rc);
}

}  // namespace bztree
