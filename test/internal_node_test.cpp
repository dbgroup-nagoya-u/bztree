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

class InternalNodeFixture : public testing::Test
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

TEST_F(InternalNodeFixture, NeedSplit_EmptyNode_SplitNotRequired)
{
  auto target_node = std::unique_ptr<InternalNode>(InternalNode::CreateEmptyNode(kDefaultNodeSize));

  auto split_required = target_node->NeedSplit(key_lengths[1], payload_lengths[1]);

  EXPECT_FALSE(split_required);
}

TEST_F(InternalNodeFixture, NeedSplit_FilledNode_SplitRequired)
{
  auto target_node = CreateInternalNodeWithOrderedKeys(1, 10);

  auto split_required = target_node->NeedSplit(key_lengths[1], payload_lengths[1]);

  EXPECT_TRUE(split_required);
}

TEST_F(InternalNodeFixture, Split_TenKeys_SplitNodesHaveCorrectStatus)
{
  auto target_node = CreateInternalNodeWithOrderedKeys(1, 10);
  auto left_record_count = 5;

  auto [left_node_ptr, right_node_ptr] = InternalNode::Split(target_node.get(), left_record_count);
  auto left_node = std::unique_ptr<InternalNode>(left_node_ptr);
  auto right_node = std::unique_ptr<InternalNode>(right_node_ptr);

  auto left_status = left_node->GetStatusWord();
  auto left_block_size = (kWordLength * 2) * left_record_count;
  auto left_deleted_size = 0;
  auto right_status = right_node->GetStatusWord();
  auto right_record_count = 5;
  auto right_block_size = (kWordLength * 2) * right_record_count;
  auto right_deleted_size = 0;

  EXPECT_EQ(kDefaultNodeSize, left_node->GetNodeSize());
  EXPECT_EQ(left_record_count, left_node->GetSortedCount());
  EXPECT_EQ(left_record_count, left_status.GetRecordCount());
  EXPECT_EQ(left_block_size, left_status.GetBlockSize());
  EXPECT_EQ(left_deleted_size, left_status.GetDeletedSize());

  EXPECT_EQ(kDefaultNodeSize, right_node->GetNodeSize());
  EXPECT_EQ(right_record_count, right_node->GetSortedCount());
  EXPECT_EQ(right_record_count, right_status.GetRecordCount());
  EXPECT_EQ(right_block_size, right_status.GetBlockSize());
  EXPECT_EQ(right_deleted_size, right_status.GetDeletedSize());
}

TEST_F(InternalNodeFixture, Split_TenKeys_SplitNodesHaveCorrectKeysAndPayloads)
{
  auto target_node = CreateInternalNodeWithOrderedKeys(1, 10);
  const auto left_record_count = 5UL;
  const auto right_record_count = 5UL;

  auto [left_node_ptr, right_node_ptr] = InternalNode::Split(target_node.get(), left_record_count);
  auto left_node = std::unique_ptr<LeafNode>(BitCast<LeafNode*>(left_node_ptr));
  auto right_node = std::unique_ptr<LeafNode>(BitCast<LeafNode*>(right_node_ptr));

  size_t index = 1;
  for (size_t count = 0; count < left_record_count; ++count, ++index) {
    auto [rc, u_ptr] = left_node->Read(key_ptrs[index], comp);
    EXPECT_EQ(payloads[index], CastToValue(u_ptr.get()));
  }
  auto return_code = left_node->Read(key_ptrs[index], comp).first;
  EXPECT_EQ(BaseNode::NodeReturnCode::kKeyNotExist, return_code);

  for (size_t count = 0; count < right_record_count; ++count, ++index) {
    auto [rc, u_ptr] = right_node->Read(key_ptrs[index], comp);
    EXPECT_EQ(payloads[index], CastToValue(u_ptr.get()));
  }
  return_code = right_node->Read(key_ptrs[left_record_count - 1], comp).first;
  EXPECT_EQ(BaseNode::NodeReturnCode::kKeyNotExist, return_code);
}

TEST_F(InternalNodeFixture, NeedMerge_EmptyNode_MergeRequired)
{
  auto target_node = std::unique_ptr<InternalNode>(InternalNode::CreateEmptyNode(kDefaultNodeSize));

  auto merge_required =
      target_node->NeedMerge(key_lengths[1], payload_lengths[1], kDefaultMinNodeSizeThreshold);

  EXPECT_TRUE(merge_required);
}

TEST_F(InternalNodeFixture, NeedMerge_FilledNode_MergeNotRequired)
{
  auto target_node = CreateInternalNodeWithOrderedKeys(1, 10);

  auto merge_required =
      target_node->NeedMerge(key_lengths[1], payload_lengths[1], kDefaultMinNodeSizeThreshold);

  EXPECT_FALSE(merge_required);
}

}  // namespace bztree
