// Copyright (c) DB Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include "bztree/components/base_node.hpp"

#include <gtest/gtest.h>

#include <memory>

#include "bztree/components/leaf_node.hpp"

using std::byte;

namespace dbgroup::index::bztree
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

  CompareAsUInt64 comp{};

 protected:
  void
  SetUp() override
  {
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
                         kIndexEpoch);
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
      target_node->Write(key_ptr, key_length, payload_ptr, payload_length, kIndexEpoch);
    }
  }

  std::unique_ptr<LeafNode>
  CreateSortedLeafNodeWithOrderedKeys(  //
      const size_t begin_index,
      const size_t end_index)
  {
    auto tmp_leaf_node = LeafNode::CreateEmptyNode(kDefaultNodeSize);
    WriteOrderedKeys(tmp_leaf_node, begin_index, end_index);
    auto tmp_meta = tmp_leaf_node->GatherSortedLiveMetadata(comp);
    tmp_leaf_node = LeafNode::Consolidate(tmp_leaf_node, tmp_meta);
    return std::unique_ptr<LeafNode>(tmp_leaf_node);
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

  auto rc = node->Freeze();
  auto status = node->GetStatusWord();

  EXPECT_EQ(BaseNode::NodeReturnCode::kSuccess, rc);
  EXPECT_TRUE(status.IsFrozen());
}

TEST_F(BaseNodeFixture, Freeze_FrozenNode_FreezeFailed)
{
  auto node = std::unique_ptr<BaseNode>(BaseNode::CreateEmptyNode(kDefaultNodeSize, true));

  node->Freeze();
  auto rc = node->Freeze();
  auto status = node->GetStatusWord();

  EXPECT_EQ(BaseNode::NodeReturnCode::kFrozen, rc);
  EXPECT_TRUE(status.IsFrozen());
}

TEST_F(BaseNodeFixture, SearchSortedMeta_SearchPresentKeyWithClosedRange_FindKeyIndex)
{
  auto node = CreateSortedLeafNodeWithOrderedKeys(1, 10);
  auto target_index = 7;
  auto target_key = key_ptrs[target_index + 1];

  auto [rc, index] = node->SearchSortedMetadata(target_key, true, comp);

  EXPECT_EQ(BaseNode::KeyExistence::kExist, rc);
  EXPECT_EQ(target_index, index);

  target_index = 3;
  target_key = key_ptrs[target_index + 1];

  std::tie(rc, index) = node->SearchSortedMetadata(target_key, true, comp);

  EXPECT_EQ(BaseNode::KeyExistence::kExist, rc);
  EXPECT_EQ(target_index, index);
}

TEST_F(BaseNodeFixture, SearchSortedMeta_SearchPresentKeyWithOpenedRange_FindNextIndex)
{
  auto node = CreateSortedLeafNodeWithOrderedKeys(1, 10);
  auto target_index = 7;
  auto target_key = key_ptrs[target_index];

  auto [rc, index] = node->SearchSortedMetadata(target_key, false, comp);

  EXPECT_EQ(BaseNode::KeyExistence::kExist, rc);
  EXPECT_EQ(target_index, index);

  target_index = 3;
  target_key = key_ptrs[target_index];

  std::tie(rc, index) = node->SearchSortedMetadata(target_key, false, comp);

  EXPECT_EQ(BaseNode::KeyExistence::kExist, rc);
  EXPECT_EQ(target_index, index);
}

TEST_F(BaseNodeFixture, SearchSortedMeta_SearchNotPresentKey_FindNextIndex)
{
  // prepare a target node
  auto tmp_node = std::unique_ptr<LeafNode>(LeafNode::CreateEmptyNode(kDefaultNodeSize));
  tmp_node->Write(key_ptrs[1], key_lengths[1], payload_ptrs[1], payload_lengths[1], kIndexEpoch);
  tmp_node->Write(key_ptrs[2], key_lengths[2], payload_ptrs[2], payload_lengths[2], kIndexEpoch);
  tmp_node->Write(key_ptrs[4], key_lengths[4], payload_ptrs[4], payload_lengths[4], kIndexEpoch);
  tmp_node->Write(key_ptrs[5], key_lengths[5], payload_ptrs[5], payload_lengths[5], kIndexEpoch);
  tmp_node->Write(key_ptrs[7], key_lengths[7], payload_ptrs[7], payload_lengths[7], kIndexEpoch);
  tmp_node->Write(key_ptrs[8], key_lengths[8], payload_ptrs[8], payload_lengths[8], kIndexEpoch);
  auto tmp_meta = tmp_node->GatherSortedLiveMetadata(comp);
  auto node = std::unique_ptr<LeafNode>(LeafNode::Consolidate(tmp_node.get(), tmp_meta));

  // perform tests
  auto target_index = 2;
  auto target_key = key_ptrs[3];

  auto [rc, index] = node->SearchSortedMetadata(target_key, true, comp);

  EXPECT_EQ(BaseNode::KeyExistence::kNotExist, rc);
  EXPECT_EQ(target_index, index);

  target_index = 4;
  target_key = key_ptrs[6];

  std::tie(rc, index) = node->SearchSortedMetadata(target_key, false, comp);

  EXPECT_EQ(BaseNode::KeyExistence::kNotExist, rc);
  EXPECT_EQ(target_index, index);
}

TEST_F(BaseNodeFixture, SearchSortedMeta_SearchDeletedKey_FindKeyIndex)
{
  auto node = CreateSortedLeafNodeWithOrderedKeys(1, 10);
  auto target_index = 7;
  auto target_key = key_ptrs[target_index + 1];
  auto target_key_length = key_lengths[target_index + 1];
  node->Delete(target_key, target_key_length, comp);

  auto [rc, index] = node->SearchSortedMetadata(target_key, true, comp);

  EXPECT_EQ(BaseNode::KeyExistence::kDeleted, rc);
  EXPECT_EQ(target_index, index);

  target_index = 3;
  target_key = key_ptrs[target_index + 1];
  target_key_length = key_lengths[target_index + 1];
  node->Delete(target_key, target_key_length, comp);

  std::tie(rc, index) = node->SearchSortedMetadata(target_key, false, comp);

  EXPECT_EQ(BaseNode::KeyExistence::kDeleted, rc);
  EXPECT_EQ(target_index, index);
}

TEST_F(BaseNodeFixture, SearchSortedMeta_SearchOutOfNodeKey_FindBorderIndex)
{
  auto node = CreateSortedLeafNodeWithOrderedKeys(1, 10);
  auto target_index = 0;
  auto target_key = key_ptrs[target_index];

  auto [rc, index] = node->SearchSortedMetadata(target_key, true, comp);

  EXPECT_EQ(BaseNode::KeyExistence::kNotExist, rc);
  EXPECT_EQ(target_index, index);

  target_index = 10;
  target_key = key_ptrs[target_index + 1];

  std::tie(rc, index) = node->SearchSortedMetadata(target_key, false, comp);

  EXPECT_EQ(BaseNode::KeyExistence::kNotExist, rc);
  EXPECT_EQ(target_index, index);
}

}  // namespace dbgroup::index::bztree
