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

TEST_F(LeafNodeFixture, New_DefaultNodeSize_GetInitializedHeader)
{
  // EXPECT_EQ(kDefaultNodeSize, node->GetNodeSize());
  // EXPECT_FALSE(node->IsFrozen());
  // EXPECT_EQ(0, node->GetRecordCount());
  // EXPECT_EQ(0, node->GetBlockSize());
  // EXPECT_EQ(0, node->GetDeletedSize());
  // EXPECT_TRUE(node->IsLeaf());
  // EXPECT_EQ(0, node->GetSortedCount());
}

TEST_F(LeafNodeFixture, Write_StringValues_MetadataCorrectlyUpdated)
{
  // auto str_key = "123", str_payload = "4567";
  // auto key_length = 4, payload_length = 5;
  // auto rc = node->Write(reinterpret_cast<const byte *>(str_key), key_length,
  //                       reinterpret_cast<const byte *>(str_payload), payload_length, kIndexEpoch,
  //                       kDefaultBlockSizeThreshold, kDefaultDeletedSizeThreshold, pool.get());

  // ASSERT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  // EXPECT_EQ(1, node->GetRecordCount());
  // EXPECT_TRUE(node->RecordIsVisible(0));
  // EXPECT_FALSE(node->RecordIsDeleted(0));
  // EXPECT_EQ(key_length, node->GetKeyLength(0));
  // EXPECT_EQ(payload_length, node->GetPayloadLength(0));

  // str_key = "test", str_payload = "value";
  // key_length = 6, payload_length = 6;
  // rc = node->Write(reinterpret_cast<const byte *>(str_key), key_length,
  //                  reinterpret_cast<const byte *>(str_payload), payload_length,  //
  //                  kIndexEpoch, kDefaultBlockSizeThreshold, kDefaultDeletedSizeThreshold,
  //                  pool.get());

  // ASSERT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  // EXPECT_EQ(2, node->GetRecordCount());
  // EXPECT_TRUE(node->RecordIsVisible(1));
  // EXPECT_FALSE(node->RecordIsDeleted(1));
  // EXPECT_EQ(key_length, node->GetKeyLength(1));
  // EXPECT_EQ(payload_length, node->GetPayloadLength(1));
}

}  // namespace bztree
