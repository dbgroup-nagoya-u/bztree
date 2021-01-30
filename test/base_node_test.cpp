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

class BaseNodeFixture : public testing::Test
{
 protected:
  std::unique_ptr<BaseNode<uint64_t, uint64_t>> node;

  void
  SetUp() override
  {
    node.reset(BaseNode<uint64_t, uint64_t>::CreateEmptyNode(kDefaultNodeSize, true));
  }

  void
  TearDown() override
  {
  }
};

TEST_F(BaseNodeFixture, New_EmptyNode_CorrectlyInitialized)
{
  EXPECT_EQ(kWordLength, node->GetStatusWordOffsetForTest());
  EXPECT_EQ(kWordLength, node->GetMetadataOffsetForTest());
  EXPECT_EQ(kDefaultNodeSize, node->GetNodeSize());
  EXPECT_EQ(0, node->GetSortedCount());
}

}  // namespace bztree
