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
  std::unique_ptr<BaseNode> node;

  void
  SetUp() override
  {
    node.reset(BaseNode::CreateEmptyNode(kDefaultNodeSize, true));
  }

  void
  TearDown() override
  {
  }
};

TEST_F(BaseNodeFixture, New_EmptyNode_CorrectlyInitialized)
{
  auto status = *BitCast<StatusWord*>(ShiftAddress(node.get(), kWordLength));

  EXPECT_EQ(kDefaultNodeSize, node->GetNodeSize());
  EXPECT_EQ(0, node->GetSortedCount());
  EXPECT_EQ(status, node->GetStatusWord());
}

}  // namespace bztree
