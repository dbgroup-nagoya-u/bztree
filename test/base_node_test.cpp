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
static constexpr size_t kDefaultMinNodeSizeThreshold = 128;
static constexpr size_t kIndexEpoch = 0;
static constexpr size_t kKeyNumForTest = 100;

class BaseNodeFixture : public testing::Test
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
  CompareAsUInt64 comp{};

 protected:
  void
  SetUp() override
  {
    pmwcas::InitLibrary(pmwcas::DefaultAllocator::Create, pmwcas::DefaultAllocator::Destroy,
                        pmwcas::LinuxEnvironment::Create, pmwcas::LinuxEnvironment::Destroy);
    pool.reset(new pmwcas::DescriptorPool{1000, 1, false});

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
  WriteNullKey(  //
      LeafNode* target_node,
      const size_t write_num)
  {
    for (size_t index = 0; index < write_num; ++index) {
      target_node->Write(key_null_ptr, key_length_null, payload_null_ptr, payload_length_null,
                         kIndexEpoch, pool.get());
    }
  }

  void
  WriteOrderedKeys(  //
      LeafNode* target_node,
      const size_t begin_index,
      const size_t end_index)
  {
    assert(end_index < kKeyNumForTest);

    for (size_t index = begin_index; index <= end_index; ++index) {
      auto key_ptr = key_ptrs[index];
      auto key_length = key_lengths[index];
      auto payload_ptr = payload_ptrs[index];
      auto payload_length = payload_lengths[index];
      target_node->Write(key_ptr, key_length, payload_ptr, payload_length, kIndexEpoch, pool.get());
    }
  }

  std::unique_ptr<InternalNode>
  CreateInternalNodeWithOrderedKeys(  //
      const size_t begin_index,
      const size_t end_index)
  {
    auto tmp_leaf_node = LeafNode::CreateEmptyNode(kDefaultNodeSize);
    WriteOrderedKeys(tmp_leaf_node, begin_index, end_index);
    auto tmp_meta = tmp_leaf_node->GatherSortedLiveMetadata(comp);
    tmp_leaf_node = LeafNode::Consolidate(tmp_leaf_node, tmp_meta);
    return std::unique_ptr<InternalNode>(BitCast<InternalNode*>(tmp_leaf_node));
  }
};

TEST_F(BaseNodeFixture, New_EmptyNode_CorrectlyInitialized)
{
  auto node = std::unique_ptr<BaseNode>(BaseNode::CreateEmptyNode(kDefaultNodeSize, true));
  auto status = *BitCast<StatusWord*>(ShiftAddress(node.get(), kWordLength));

  EXPECT_EQ(kDefaultNodeSize, node->GetNodeSize());
  EXPECT_EQ(0, node->GetSortedCount());
  EXPECT_EQ(status, node->GetStatusWord());
}

TEST_F(BaseNodeFixture, Freeze_NotFrozenNode_FreezeNode)
{
  auto node = std::unique_ptr<BaseNode>(BaseNode::CreateEmptyNode(kDefaultNodeSize, true));

  auto rc = node->Freeze(pool.get());
  auto status = node->GetStatusWord();

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_TRUE(status.IsFrozen());
}

TEST_F(BaseNodeFixture, Freeze_FrozenNode_FreezeFailed)
{
  auto node = std::unique_ptr<BaseNode>(BaseNode::CreateEmptyNode(kDefaultNodeSize, true));

  node->Freeze(pool.get());
  auto rc = node->Freeze(pool.get());
  auto status = node->GetStatusWord();

  EXPECT_EQ(BaseNode::NodeReturnCode::kFrozen, rc);
  EXPECT_TRUE(status.IsFrozen());
}

}  // namespace bztree
