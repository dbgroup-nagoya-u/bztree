/*
 * Copyright 2021 Database Group, Nagoya University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "bztree/components/internal_node.hpp"

#include <gtest/gtest.h>

#include <memory>

#include "bztree/components/leaf_node.hpp"

using std::byte;

namespace dbgroup::index::bztree
{
using Key = uint64_t;
using Payload = uint64_t;
using Record_t = Record<Key, Payload>;
using BaseNode_t = BaseNode<Key, Payload>;
using LeafNode_t = LeafNode<Key, Payload>;
using InternalNode_t = InternalNode<Key, Payload>;
using NodeReturnCode = BaseNode<Key, Payload>::NodeReturnCode;
using KeyExistence = BaseNode<Key, Payload>::KeyExistence;

static constexpr size_t kNodeSize = 256;
static constexpr size_t kIndexEpoch = 0;
static constexpr size_t kKeyNumForTest = 10000;
static constexpr size_t kKeyLength = sizeof(Key);
static constexpr size_t kPayloadLength = sizeof(Payload);
static constexpr size_t kRecordLength = kKeyLength + kPayloadLength;
static constexpr size_t kDefaultMinNodeSizeThreshold = 128;

class InternalNodeFixture : public testing::Test
{
 public:
  Key keys[kKeyNumForTest];
  Payload payloads[kKeyNumForTest];
  Key key_null = 0;          // null key must have 8 bytes to fill a node
  Payload payload_null = 0;  // null payload must have 8 bytes to fill a node

  std::unique_ptr<BaseNode_t> node;

  size_t expected_record_count;
  size_t expected_block_size;
  size_t expected_deleted_size;

 protected:
  void
  SetUp() override
  {
    node.reset(BaseNode_t::CreateEmptyNode(kInternalFlag));

    expected_record_count = 0;
    expected_block_size = 0;
    expected_deleted_size = 0;

    for (size_t index = 0; index < kKeyNumForTest; index++) {
      keys[index] = index + 1;
      payloads[index] = index + 1;
    }
  }

  void
  TearDown() override
  {
  }

  void
  WriteNullKey(  //
      BaseNode_t* target_node,
      const size_t write_num)
  {
    for (size_t index = 0; index < write_num; ++index) {
      LeafNode_t::Write(target_node, key_null, kKeyLength, payload_null, kPayloadLength);
    }
  }

  void
  WriteOrderedKeys(  //
      BaseNode_t* target_node,
      const size_t begin_index,
      const size_t end_index)
  {
    assert(end_index < kKeyNumForTest);

    for (size_t index = begin_index; index <= end_index; ++index) {
      LeafNode_t::Write(target_node, keys[index], kKeyLength, payloads[index], kPayloadLength);
    }
  }

  BaseNode_t*
  CreateInternalNodeWithOrderedKeys(  //
      const size_t begin_index,
      const size_t end_index)
  {
    auto tmp_leaf_node = BaseNode_t::CreateEmptyNode(kLeafFlag);
    WriteOrderedKeys(tmp_leaf_node, begin_index, end_index);
    auto tmp_meta = LeafNode_t::GatherSortedLiveMetadata(tmp_leaf_node);
    return LeafNode_t::Consolidate(tmp_leaf_node, tmp_meta);
  }

  void
  VerifyStatusWord(  //
      const StatusWord status,
      const bool status_is_frozen = false)
  {
    EXPECT_EQ(status, node->GetStatusWord());
    if (status_is_frozen) {
      EXPECT_TRUE(status.IsFrozen());
    } else {
      EXPECT_FALSE(status.IsFrozen());
    }
    EXPECT_EQ(expected_record_count, status.GetRecordCount());
    EXPECT_EQ(expected_block_size, status.GetBlockSize());
    EXPECT_EQ(expected_deleted_size, status.GetDeletedSize());
  }
};

TEST_F(InternalNodeFixture, NeedSplit_EmptyNode_SplitNotRequired)
{
  EXPECT_FALSE(InternalNode_t::NeedSplit(node.get(), kKeyLength, kPayloadLength));
}

TEST_F(InternalNodeFixture, NeedSplit_FilledNode_SplitRequired)
{
  node.reset(CreateInternalNodeWithOrderedKeys(0, 9));

  EXPECT_TRUE(InternalNode_t::NeedSplit(node.get(), kKeyLength, kPayloadLength));
}

TEST_F(InternalNodeFixture, Split_TenKeys_SplitNodesHaveCorrectStatus)
{
  node.reset(CreateInternalNodeWithOrderedKeys(0, 9));
  auto left_record_count = 5;

  auto [left_node_ptr, right_node_ptr] = InternalNode_t::Split(node.get(), left_record_count);
  expected_record_count = 5;
  expected_block_size = expected_record_count * kRecordLength;

  node.reset(left_node_ptr);
  VerifyStatusWord(node->GetStatusWord());

  node.reset(right_node_ptr);
  VerifyStatusWord(node->GetStatusWord());
}

TEST_F(InternalNodeFixture, Split_TenKeys_SplitNodesHaveCorrectKeysAndPayloads)
{
  node.reset(CreateInternalNodeWithOrderedKeys(0, 9));
  expected_record_count = 5;

  auto [left_node_ptr, right_node_ptr] = InternalNode_t::Split(node.get(), expected_record_count);

  std::unique_ptr<LeafNode_t> target_node;
  NodeReturnCode return_code;

  target_node.reset(CastAddress<LeafNode_t*>(left_node_ptr));
  size_t index = 0;
  for (size_t count = 0; count < expected_record_count; ++count, ++index) {
    auto [rc, record] =
        LeafNode_t::Read(reinterpret_cast<BaseNode_t*>(target_node.get()), keys[index]);
    EXPECT_EQ(payloads[index], record->GetPayload());
  }
  return_code =
      LeafNode_t::Read(reinterpret_cast<BaseNode_t*>(target_node.get()), keys[index]).first;
  EXPECT_EQ(NodeReturnCode::kKeyNotExist, return_code);

  target_node.reset(CastAddress<LeafNode_t*>(right_node_ptr));
  for (size_t count = 0; count < expected_record_count; ++count, ++index) {
    auto [rc, record] =
        LeafNode_t::Read(reinterpret_cast<BaseNode_t*>(target_node.get()), keys[index]);
    EXPECT_EQ(payloads[index], record->GetPayload());
  }
  return_code =
      LeafNode_t::Read(reinterpret_cast<BaseNode_t*>(target_node.get()), keys[index]).first;
  EXPECT_EQ(NodeReturnCode::kKeyNotExist, return_code);
}

TEST_F(InternalNodeFixture, NeedMerge_EmptyNode_MergeRequired)
{
  EXPECT_TRUE(InternalNode_t::NeedMerge(node.get(), kKeyLength, kPayloadLength,
                                        kDefaultMinNodeSizeThreshold));
}

TEST_F(InternalNodeFixture, NeedMerge_FilledNode_MergeNotRequired)
{
  node.reset(CreateInternalNodeWithOrderedKeys(0, 9));

  EXPECT_FALSE(InternalNode_t::NeedMerge(node.get(), kKeyLength, kPayloadLength,
                                         kDefaultMinNodeSizeThreshold));
}

TEST_F(InternalNodeFixture, Merge_LeftSibling_MergedNodeHasCorrectStatus)
{
  auto target_node = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(4, 6));
  auto sibling_node = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(2, 3));

  node.reset(InternalNode_t::Merge(target_node.get(), sibling_node.get(), true));
  expected_record_count = 5;
  expected_block_size = expected_record_count * kRecordLength;

  VerifyStatusWord(node->GetStatusWord());
}

TEST_F(InternalNodeFixture, Merge_RightSibling_MergedNodeHasCorrectStatus)
{
  auto target_node = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(4, 6));
  auto sibling_node = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(7, 8));

  node.reset(InternalNode_t::Merge(target_node.get(), sibling_node.get(), false));
  expected_record_count = 5;
  expected_block_size = expected_record_count * kRecordLength;

  VerifyStatusWord(node->GetStatusWord());
}

TEST_F(InternalNodeFixture, Merge_LeftSibling_MergedNodeHasCorrectKeysAndPayloads)
{
  auto target_node = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(4, 6));
  auto sibling_node = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(2, 3));

  auto merged_node = std::unique_ptr<BaseNode_t>(
      CastAddress<BaseNode_t*>(InternalNode_t::Merge(target_node.get(), sibling_node.get(), true)));

  auto [rc, scan_results] = LeafNode_t::Scan(merged_node.get(), &keys[3], true, &keys[5], false);

  ASSERT_EQ(2, scan_results.size());
  EXPECT_EQ(keys[3], scan_results[0]->GetKey());
  EXPECT_EQ(payloads[3], scan_results[0]->GetPayload());
  EXPECT_EQ(keys[4], scan_results[1]->GetKey());
  EXPECT_EQ(payloads[4], scan_results[1]->GetPayload());
}

TEST_F(InternalNodeFixture, Merge_RightSibling_MergedNodeHasCorrectKeysAndPayloads)
{
  auto target_node = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(4, 6));
  auto sibling_node = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(7, 8));

  auto merged_node = std::unique_ptr<BaseNode_t>(CastAddress<BaseNode_t*>(
      InternalNode_t::Merge(target_node.get(), sibling_node.get(), false)));

  auto [rc, scan_results] = LeafNode_t::Scan(merged_node.get(), &keys[5], false, &keys[7], true);

  ASSERT_EQ(2, scan_results.size());
  EXPECT_EQ(keys[6], scan_results[0]->GetKey());
  EXPECT_EQ(payloads[6], scan_results[0]->GetPayload());
  EXPECT_EQ(keys[7], scan_results[1]->GetKey());
  EXPECT_EQ(payloads[7], scan_results[1]->GetPayload());
}

TEST_F(InternalNodeFixture, NewRoot_TwoChildNodes_HasCorrectStatus)
{
  auto left_node = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(0, 4));
  auto right_node = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(5, 9));

  node.reset(InternalNode_t::CreateNewRoot(left_node.get(), right_node.get()));
  expected_record_count = 2;
  expected_block_size = expected_record_count * kRecordLength;

  VerifyStatusWord(node->GetStatusWord());
}

TEST_F(InternalNodeFixture, NewRoot_TwoChildNodes_HasCorrectPointersToChildren)
{
  auto left_node = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(0, 4));
  auto right_node = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(5, 9));

  node.reset(InternalNode_t::CreateNewRoot(left_node.get(), right_node.get()));

  auto read_left_addr = InternalNode_t::GetChildNode(node.get(), 0);
  EXPECT_TRUE(HaveSameAddress(left_node.get(), read_left_addr));

  auto read_right_addr = InternalNode_t::GetChildNode(node.get(), 1);
  EXPECT_TRUE(HaveSameAddress(right_node.get(), read_right_addr));
}

TEST_F(InternalNodeFixture, NewParent_AfterSplit_HasCorrectStatus)
{
  // prepare an old parent
  auto left_node = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(1, 6));
  auto right_node = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(7, 9));
  auto old_parent =
      std::unique_ptr<BaseNode_t>(InternalNode_t::CreateNewRoot(left_node.get(), right_node.get()));

  // prepare a split node
  auto [tmp_left, tmp_right] = InternalNode_t::Split(left_node.get(), 3);
  auto split_left = std::unique_ptr<BaseNode_t>(tmp_left);
  auto split_right = std::unique_ptr<BaseNode_t>(tmp_right);
  auto new_key = keys[3];
  auto split_index = 1;

  // create a new parent node
  auto new_parent = std::unique_ptr<BaseNode_t>(InternalNode_t::NewParentForSplit(
      old_parent.get(), new_key, kKeyLength, split_left.get(), split_right.get(), split_index));

  auto status = new_parent->GetStatusWord();
  auto record_count = 3;
  auto block_size = (kWordLength * 2) * record_count;
  auto deleted_size = 0;

  EXPECT_EQ(record_count, new_parent->GetSortedCount());
  EXPECT_EQ(record_count, status.GetRecordCount());
  EXPECT_EQ(block_size, status.GetBlockSize());
  EXPECT_EQ(deleted_size, status.GetDeletedSize());
}

TEST_F(InternalNodeFixture, NewParent_AfterSplit_HasCorrectPointersToChildren)
{
  // prepare an old parent
  auto left_node = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(1, 6));
  auto right_node = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(7, 9));
  auto old_parent =
      std::unique_ptr<BaseNode_t>(InternalNode_t::CreateNewRoot(left_node.get(), right_node.get()));

  // prepare a split node
  auto [tmp_left, tmp_right] = InternalNode_t::Split(left_node.get(), 3);
  auto split_left = std::unique_ptr<BaseNode_t>(tmp_left);
  auto split_right = std::unique_ptr<BaseNode_t>(tmp_right);
  auto new_key = keys[3];
  auto split_index = 1;

  // create a new parent node
  auto new_parent = std::unique_ptr<BaseNode_t>(InternalNode_t::NewParentForSplit(
      old_parent.get(), new_key, kKeyLength, split_left.get(), split_right.get(), split_index));

  auto read_addr = InternalNode_t::GetChildNode(new_parent.get(), 0);
  EXPECT_TRUE(HaveSameAddress(left_node.get(), read_addr));

  read_addr = InternalNode_t::GetChildNode(new_parent.get(), 1);
  EXPECT_TRUE(HaveSameAddress(split_left.get(), read_addr));

  read_addr = InternalNode_t::GetChildNode(new_parent.get(), 2);
  EXPECT_TRUE(HaveSameAddress(split_right.get(), read_addr));
}

TEST_F(InternalNodeFixture, NewParent_AfterMerge_HasCorrectStatus)
{
  // prepare an old parent
  auto left_node = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(1, 6));
  auto right_node = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(7, 9));
  auto old_parent =
      std::unique_ptr<BaseNode_t>(InternalNode_t::CreateNewRoot(left_node.get(), right_node.get()));

  // prepare a merged node
  auto merged_node =
      std::unique_ptr<BaseNode_t>(InternalNode_t::Merge(left_node.get(), right_node.get(), false));
  auto deleted_index = 0;

  // create a new parent node
  node.reset(InternalNode_t::NewParentForMerge(old_parent.get(), merged_node.get(), deleted_index));
  expected_record_count = 1;
  expected_block_size = expected_record_count * kRecordLength;

  VerifyStatusWord(node->GetStatusWord());
}

TEST_F(InternalNodeFixture, NewParent_AfterMerge_HasCorrectPointersToChildren)
{
  // prepare an old parent
  auto left_node = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(1, 6));
  auto right_node = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(7, 9));
  auto old_parent =
      std::unique_ptr<BaseNode_t>(InternalNode_t::CreateNewRoot(left_node.get(), right_node.get()));

  // prepare a merged node
  auto merged_node =
      std::unique_ptr<BaseNode_t>(InternalNode_t::Merge(left_node.get(), right_node.get(), false));
  auto deleted_index = 0;

  // create a new parent node
  node.reset(InternalNode_t::NewParentForMerge(old_parent.get(), merged_node.get(), deleted_index));

  auto read_addr = InternalNode_t::GetChildNode(node.get(), 0);
  EXPECT_TRUE(HaveSameAddress(merged_node.get(), read_addr));
}

TEST_F(InternalNodeFixture, CanMergeLeftSibling_SiblingHasSufficentSpace_CanBeMerged)
{
  // prepare an old parent
  auto left_n = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(1, 3));  // 72B
  auto right_n = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(4, 9));
  node.reset(InternalNode_t::CreateNewRoot(left_n.get(), right_n.get()));

  // prepare merging information
  auto target_index = 1;
  auto target_node_size = kHeaderLength + kWordLength + kRecordLength;  // 40B
  auto max_merged_size = 128;

  EXPECT_TRUE(InternalNode_t::CanMergeLeftSibling(node.get(), target_index, target_node_size,
                                                  max_merged_size));
}

TEST_F(InternalNodeFixture, CanMergeLeftSibling_SiblingHasSmallSpace_CannotBeMerged)
{
  // prepare an old parent
  auto left_n = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(1, 6));  // 144B
  auto right_n = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(7, 9));
  node.reset(InternalNode_t::CreateNewRoot(left_n.get(), right_n.get()));

  // prepare merging information
  auto target_index = 1;
  auto target_node_size = kHeaderLength + kWordLength + kRecordLength;  // 40B
  auto max_merged_size = 128;

  EXPECT_FALSE(InternalNode_t::CanMergeLeftSibling(node.get(), target_index, target_node_size,
                                                   max_merged_size));
}

TEST_F(InternalNodeFixture, CanMergeLeftSibling_NoSibling_CannotBeMerged)
{
  // prepare an old parent
  auto left_n = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(1, 6));  // 144B
  auto right_n = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(7, 9));
  node.reset(InternalNode_t::CreateNewRoot(left_n.get(), right_n.get()));

  // prepare merging information
  auto target_index = 0;
  auto target_node_size = kHeaderLength + kWordLength + kRecordLength;  // 40B
  auto max_merged_size = 128;

  EXPECT_FALSE(InternalNode_t::CanMergeLeftSibling(node.get(), target_index, target_node_size,
                                                   max_merged_size));
}

TEST_F(InternalNodeFixture, CanMergeRightSibling_SiblingHasSufficentSpace_CanBeMerged)
{
  // prepare an old parent
  auto left_n = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(1, 6));
  auto right_n = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(7, 9));  // 72B
  node.reset(InternalNode_t::CreateNewRoot(left_n.get(), right_n.get()));

  // prepare merging information
  auto target_index = 0;
  auto target_node_size = kHeaderLength + kWordLength + kRecordLength;  // 40B
  auto max_merged_size = 128;

  EXPECT_TRUE(InternalNode_t::CanMergeRightSibling(node.get(), target_index, target_node_size,
                                                   max_merged_size));
}

TEST_F(InternalNodeFixture, CanMergeRightSibling_SiblingHasSmallSpace_CannotBeMerged)
{
  // prepare an old parent
  auto left_n = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(1, 3));
  auto right_n = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(4, 9));  // 144B
  node.reset(InternalNode_t::CreateNewRoot(left_n.get(), right_n.get()));

  // prepare merging information
  auto target_index = 0;
  auto target_node_size = kHeaderLength + kWordLength + kRecordLength;  // 40B
  auto max_merged_size = 128;

  EXPECT_FALSE(InternalNode_t::CanMergeRightSibling(node.get(), target_index, target_node_size,
                                                    max_merged_size));
}

TEST_F(InternalNodeFixture, CanMergeRightSibling_NoSibling_CannotBeMerged)
{
  // prepare an old parent
  auto left_n = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(1, 6));
  auto right_n = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(7, 9));  // 72B
  node.reset(InternalNode_t::CreateNewRoot(left_n.get(), right_n.get()));

  // prepare merging information
  auto target_index = 1;
  auto target_node_size = kHeaderLength + kWordLength + kRecordLength;  // 40B
  auto max_merged_size = 128;

  EXPECT_FALSE(InternalNode_t::CanMergeRightSibling(node.get(), target_index, target_node_size,
                                                    max_merged_size));
}

}  // namespace dbgroup::index::bztree
