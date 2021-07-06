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

#include <memory>

#include "bztree/components/leaf_node.hpp"
#include "gtest/gtest.h"

namespace dbgroup::index::bztree
{
// use a supper template to define key-payload pair templates
template <class KeyType, class PayloadType, class KeyComparator, class PayloadComparator>
struct KeyPayloadPair {
  using Key = KeyType;
  using Payload = PayloadType;
  using KeyComp = KeyComparator;
  using PayloadComp = PayloadComparator;
};

template <class KeyPayloadPair>
class InternalNodeFixture : public testing::Test
{
 protected:
  // extract key-payload types
  using Key = typename KeyPayloadPair::Key;
  using Payload = typename KeyPayloadPair::Payload;
  using KeyComp = typename KeyPayloadPair::KeyComp;
  using PayloadComp = typename KeyPayloadPair::PayloadComp;

  // define type aliases for simplicity
  using Record_t = Record<Key, Payload>;
  using BaseNode_t = BaseNode<Key, Payload, KeyComp>;
  using LeafNode_t = LeafNode<Key, Payload, KeyComp>;
  using InternalNode_t = InternalNode<Key, Payload, KeyComp>;
  using NodeReturnCode = typename BaseNode_t::NodeReturnCode;

  // constant values for testing
  static constexpr size_t kKeyNumForTest = 1024;
  static constexpr size_t kKeyLength = kWordLength;
  static constexpr size_t kPayloadLength = kWordLength;
  static constexpr size_t kDefaultMinNodeSizeThreshold = 128;
  static constexpr size_t kDummyNodeNum = 10;

  // actual keys and payloads
  size_t key_length;
  Key keys[kKeyNumForTest];

  // the length of a record and its maximum number
  size_t record_length;
  size_t max_record_num;

  // a leaf node
  std::unique_ptr<BaseNode_t> node;

  /*################################################################################################
   * Setup/Teardown
   *##############################################################################################*/

  void
  SetUp()
  {
    // initialize a leaf node and expected statistics
    node.reset(BaseNode_t::CreateEmptyNode(kInternalFlag));

    // prepare keys
    if constexpr (std::is_same_v<Key, char*>) {
      // variable-length keys
      key_length = 7;
      for (size_t index = 0; index < kKeyNumForTest; ++index) {
        auto key = new char[kKeyLength];
        snprintf(key, kKeyLength, "%06lu", index);
        keys[index] = key;
      }
    } else {
      // static-length keys
      key_length = sizeof(Key);
      for (size_t index = 0; index < kKeyNumForTest; ++index) {
        keys[index] = index;
      }
    }

    // set a record length and its maximum number
    record_length = 2 * kWordLength;
    max_record_num = (kPageSize - kHeaderLength) / (record_length + kWordLength);
  }

  void
  TearDown()
  {
    if constexpr (std::is_same_v<Key, char*>) {
      for (size_t index = 0; index < kKeyNumForTest; ++index) {
        delete[] keys[index];
      }
    }
  }

  /*################################################################################################
   * Utility functions
   *##############################################################################################*/

  BaseNode_t*
  PrepareDummyNode(const size_t child_num)
  {
    auto dummy_node = BaseNode_t::CreateEmptyNode(kInternalFlag);

    // embeds dummy childrens
    auto offset = kPageSize;
    for (size_t i = 0; i < child_num; ++i) {
      // set a key and a dummy payload
      offset = dummy_node->SetPayload(offset, i, kWordLength);
      offset = dummy_node->SetKey(offset, keys[i], key_length);

      // set a corresponding metadata
      const auto meta = Metadata{}.SetRecordInfo(offset, key_length, key_length + kWordLength);
      dummy_node->SetMetadata(i, meta);
    }

    const auto status = StatusWord{}.AddRecordInfo(child_num, child_num * record_length, 0);
    dummy_node->SetStatus(status);

    return dummy_node;
  }

  /*################################################################################################
   * Functions for verification
   *##############################################################################################*/

  void
  VerifyGetChildNode()
  {
    node.reset(PrepareDummyNode(kDummyNodeNum));

    for (size_t i = 0; i < kDummyNodeNum; ++i) {
      auto child = reinterpret_cast<uintptr_t>(InternalNode_t::GetChildNode(node.get(), i));
      EXPECT_EQ(i, child);
    }
  }
};

/*##################################################################################################
 * Preparation for typed testing
 *################################################################################################*/

using Int32Comp = std::less<int32_t>;
using Int64Comp = std::less<int64_t>;
using CStrComp = dbgroup::index::bztree::CompareAsCString;

using KeyPayloadPairs = ::testing::Types<KeyPayloadPair<int64_t, int64_t, Int64Comp, Int64Comp>,
                                         KeyPayloadPair<int32_t, int64_t, Int32Comp, Int64Comp>,
                                         KeyPayloadPair<char*, int64_t, CStrComp, Int64Comp>>;
TYPED_TEST_CASE(InternalNodeFixture, KeyPayloadPairs);

/*##################################################################################################
 * Unit test definitions
 *################################################################################################*/

TYPED_TEST(InternalNodeFixture, GetChildNode_DummyChildren_ReadDummyValues)
{
  TestFixture::VerifyGetChildNode();
}

// TYPED_TEST(InternalNodeFixture, NeedSplit_EmptyNode_SplitNotRequired)
// {
//   EXPECT_FALSE(InternalNode_t::NeedSplit(node.get(), kKeyLength, kPayloadLength));
// }

// TYPED_TEST(InternalNodeFixture, NeedSplit_FilledNode_SplitRequired)
// {
//   node.reset(CreateInternalNodeWithOrderedKeys(0, kMaxRecordNum));

//   EXPECT_TRUE(InternalNode_t::NeedSplit(node.get(), kKeyLength, kPayloadLength));
// }

// TYPED_TEST(InternalNodeFixture, Split_TenKeys_SplitNodesHaveCorrectStatus)
// {
//   node.reset(CreateInternalNodeWithOrderedKeys(0, 9));
//   auto left_record_count = 5;

//   auto [left_node_ptr, right_node_ptr] = InternalNode_t::Split(node.get(), left_record_count);
//   expected_record_count = 5;
//   expected_block_size = expected_record_count * kRecordLength;

//   node.reset(left_node_ptr);
//   VerifyStatusWord(node->GetStatusWord());

//   node.reset(right_node_ptr);
//   VerifyStatusWord(node->GetStatusWord());
// }

// TYPED_TEST(InternalNodeFixture, Split_TenKeys_SplitNodesHaveCorrectKeysAndPayloads)
// {
//   node.reset(CreateInternalNodeWithOrderedKeys(0, 9));
//   expected_record_count = 5;

//   auto [left_node_ptr, right_node_ptr] = InternalNode_t::Split(node.get(),
//   expected_record_count);

//   std::unique_ptr<BaseNode_t> target_node;
//   NodeReturnCode return_code;

//   target_node.reset(left_node_ptr);
//   size_t index = 0;
//   for (size_t count = 0; count < expected_record_count; ++count, ++index) {
//     auto [rc, payload] = LeafNode_t::Read(target_node.get(), keys[index]);
//     EXPECT_EQ(payloads[index], payload);
//   }
//   return_code = LeafNode_t::Read(target_node.get(), keys[index]).first;
//   EXPECT_EQ(NodeReturnCode::kKeyNotExist, return_code);

//   target_node.reset(right_node_ptr);
//   for (size_t count = 0; count < expected_record_count; ++count, ++index) {
//     auto [rc, payload] = LeafNode_t::Read(target_node.get(), keys[index]);
//     EXPECT_EQ(payloads[index], payload);
//   }
//   return_code = LeafNode_t::Read(target_node.get(), keys[index]).first;
//   EXPECT_EQ(NodeReturnCode::kKeyNotExist, return_code);
// }

// TYPED_TEST(InternalNodeFixture, NeedMerge_EmptyNode_MergeRequired)
// {
//   EXPECT_TRUE(InternalNode_t::NeedMerge(node.get(), kKeyLength, kPayloadLength,
//                                         kDefaultMinNodeSizeThreshold));
// }

// TYPED_TEST(InternalNodeFixture, NeedMerge_FilledNode_MergeNotRequired)
// {
//   node.reset(CreateInternalNodeWithOrderedKeys(0, 9));

//   EXPECT_FALSE(InternalNode_t::NeedMerge(node.get(), kKeyLength, kPayloadLength,
//                                          kDefaultMinNodeSizeThreshold));
// }

// TYPED_TEST(InternalNodeFixture, Merge_LeftSibling_MergedNodeHasCorrectStatus)
// {
//   auto target_node = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(4, 6));
//   auto sibling_node = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(2, 3));

//   node.reset(InternalNode_t::Merge(target_node.get(), sibling_node.get(), true));
//   expected_record_count = 5;
//   expected_block_size = expected_record_count * kRecordLength;

//   VerifyStatusWord(node->GetStatusWord());
// }

// TYPED_TEST(InternalNodeFixture, Merge_RightSibling_MergedNodeHasCorrectStatus)
// {
//   auto target_node = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(4, 6));
//   auto sibling_node = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(7, 8));

//   node.reset(InternalNode_t::Merge(target_node.get(), sibling_node.get(), false));
//   expected_record_count = 5;
//   expected_block_size = expected_record_count * kRecordLength;

//   VerifyStatusWord(node->GetStatusWord());
// }

// TYPED_TEST(InternalNodeFixture, Merge_LeftSibling_MergedNodeHasCorrectKeysAndPayloads)
// {
//   auto target_node = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(4, 6));
//   auto sibling_node = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(2, 3));

//   auto merged_node = std::unique_ptr<BaseNode_t>(
//       CastAddress<BaseNode_t*>(InternalNode_t::Merge(target_node.get(), sibling_node.get(),
//       true)));

//   auto [rc, scan_results] = LeafNode_t::Scan(merged_node.get(), &keys[3], true, &keys[5], false);

//   ASSERT_EQ(2, scan_results.size());
//   EXPECT_EQ(keys[3], scan_results[0]->GetKey());
//   EXPECT_EQ(payloads[3], scan_results[0]->GetPayload());
//   EXPECT_EQ(keys[4], scan_results[1]->GetKey());
//   EXPECT_EQ(payloads[4], scan_results[1]->GetPayload());
// }

// TYPED_TEST(InternalNodeFixture, Merge_RightSibling_MergedNodeHasCorrectKeysAndPayloads)
// {
//   auto target_node = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(4, 6));
//   auto sibling_node = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(7, 8));

//   auto merged_node = std::unique_ptr<BaseNode_t>(CastAddress<BaseNode_t*>(
//       InternalNode_t::Merge(target_node.get(), sibling_node.get(), false)));

//   auto [rc, scan_results] = LeafNode_t::Scan(merged_node.get(), &keys[5], false, &keys[7], true);

//   ASSERT_EQ(2, scan_results.size());
//   EXPECT_EQ(keys[6], scan_results[0]->GetKey());
//   EXPECT_EQ(payloads[6], scan_results[0]->GetPayload());
//   EXPECT_EQ(keys[7], scan_results[1]->GetKey());
//   EXPECT_EQ(payloads[7], scan_results[1]->GetPayload());
// }

// TYPED_TEST(InternalNodeFixture, NewRoot_TwoChildNodes_HasCorrectStatus)
// {
//   auto left_node = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(0, 4));
//   auto right_node = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(5, 9));

//   node.reset(InternalNode_t::CreateNewRoot(left_node.get(), right_node.get()));
//   expected_record_count = 2;
//   expected_block_size = expected_record_count * kRecordLength;

//   VerifyStatusWord(node->GetStatusWord());
// }

// TYPED_TEST(InternalNodeFixture, NewRoot_TwoChildNodes_HasCorrectPointersToChildren)
// {
//   auto left_node = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(0, 4));
//   auto right_node = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(5, 9));

//   node.reset(InternalNode_t::CreateNewRoot(left_node.get(), right_node.get()));

//   auto read_left_addr = InternalNode_t::GetChildNode(node.get(), 0);
//   EXPECT_TRUE(HaveSameAddress(left_node.get(), read_left_addr));

//   auto read_right_addr = InternalNode_t::GetChildNode(node.get(), 1);
//   EXPECT_TRUE(HaveSameAddress(right_node.get(), read_right_addr));
// }

// TYPED_TEST(InternalNodeFixture, NewParent_AfterSplit_HasCorrectStatus)
// {
//   // prepare an old parent
//   auto left_node = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(1, 6));
//   auto right_node = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(7, 9));
//   auto old_parent =
//       std::unique_ptr<BaseNode_t>(InternalNode_t::CreateNewRoot(left_node.get(),
//       right_node.get()));

//   // prepare a split node
//   auto [tmp_left, tmp_right] = InternalNode_t::Split(left_node.get(), 3);
//   auto split_left = std::unique_ptr<BaseNode_t>(tmp_left);
//   auto split_right = std::unique_ptr<BaseNode_t>(tmp_right);
//   auto new_key = keys[3];
//   auto split_index = 1;

//   // create a new parent node
//   auto new_parent = std::unique_ptr<BaseNode_t>(InternalNode_t::NewParentForSplit(
//       old_parent.get(), new_key, kKeyLength, split_left.get(), split_right.get(), split_index));

//   auto status = new_parent->GetStatusWord();
//   auto record_count = 3;
//   auto block_size = (kWordLength * 2) * record_count;
//   auto deleted_size = 0;

//   EXPECT_EQ(record_count, new_parent->GetSortedCount());
//   EXPECT_EQ(record_count, status.GetRecordCount());
//   EXPECT_EQ(block_size, status.GetBlockSize());
//   EXPECT_EQ(deleted_size, status.GetDeletedSize());
// }

// TYPED_TEST(InternalNodeFixture, NewParent_AfterSplit_HasCorrectPointersToChildren)
// {
//   // prepare an old parent
//   auto left_node = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(1, 6));
//   auto right_node = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(7, 9));
//   auto old_parent =
//       std::unique_ptr<BaseNode_t>(InternalNode_t::CreateNewRoot(left_node.get(),
//       right_node.get()));

//   // prepare a split node
//   auto [tmp_left, tmp_right] = InternalNode_t::Split(left_node.get(), 3);
//   auto split_left = std::unique_ptr<BaseNode_t>(tmp_left);
//   auto split_right = std::unique_ptr<BaseNode_t>(tmp_right);
//   auto new_key = keys[3];
//   auto split_index = 1;

//   // create a new parent node
//   auto new_parent = std::unique_ptr<BaseNode_t>(InternalNode_t::NewParentForSplit(
//       old_parent.get(), new_key, kKeyLength, split_left.get(), split_right.get(), split_index));

//   auto read_addr = InternalNode_t::GetChildNode(new_parent.get(), 0);
//   EXPECT_TRUE(HaveSameAddress(left_node.get(), read_addr));

//   read_addr = InternalNode_t::GetChildNode(new_parent.get(), 1);
//   EXPECT_TRUE(HaveSameAddress(split_left.get(), read_addr));

//   read_addr = InternalNode_t::GetChildNode(new_parent.get(), 2);
//   EXPECT_TRUE(HaveSameAddress(split_right.get(), read_addr));
// }

// TYPED_TEST(InternalNodeFixture, NewParent_AfterMerge_HasCorrectStatus)
// {
//   // prepare an old parent
//   auto left_node = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(1, 6));
//   auto right_node = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(7, 9));
//   auto old_parent =
//       std::unique_ptr<BaseNode_t>(InternalNode_t::CreateNewRoot(left_node.get(),
//       right_node.get()));

//   // prepare a merged node
//   auto merged_node =
//       std::unique_ptr<BaseNode_t>(InternalNode_t::Merge(left_node.get(), right_node.get(),
//       false));
//   auto deleted_index = 0;

//   // create a new parent node
//   node.reset(InternalNode_t::NewParentForMerge(old_parent.get(), merged_node.get(),
//   deleted_index)); expected_record_count = 1; expected_block_size = expected_record_count *
//   kRecordLength;

//   VerifyStatusWord(node->GetStatusWord());
// }

// TYPED_TEST(InternalNodeFixture, NewParent_AfterMerge_HasCorrectPointersToChildren)
// {
//   // prepare an old parent
//   auto left_node = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(1, 6));
//   auto right_node = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(7, 9));
//   auto old_parent =
//       std::unique_ptr<BaseNode_t>(InternalNode_t::CreateNewRoot(left_node.get(),
//       right_node.get()));

//   // prepare a merged node
//   auto merged_node =
//       std::unique_ptr<BaseNode_t>(InternalNode_t::Merge(left_node.get(), right_node.get(),
//       false));
//   auto deleted_index = 0;

//   // create a new parent node
//   node.reset(InternalNode_t::NewParentForMerge(old_parent.get(), merged_node.get(),
//   deleted_index));

//   auto read_addr = InternalNode_t::GetChildNode(node.get(), 0);
//   EXPECT_TRUE(HaveSameAddress(merged_node.get(), read_addr));
// }

// TYPED_TEST(InternalNodeFixture, CanMergeLeftSibling_SiblingHasSufficentSpace_CanBeMerged)
// {
//   // prepare an old parent
//   auto left_n = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(1, 3));  // 72B
//   auto right_n = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(4, 9));
//   node.reset(InternalNode_t::CreateNewRoot(left_n.get(), right_n.get()));

//   // prepare merging information
//   auto target_index = 1;
//   auto target_node_size = kHeaderLength + kWordLength + kRecordLength;  // 40B
//   auto max_merged_size = 128;

//   EXPECT_TRUE(InternalNode_t::CanMergeLeftSibling(node.get(), target_index, target_node_size,
//                                                   max_merged_size));
// }

// TYPED_TEST(InternalNodeFixture, CanMergeLeftSibling_SiblingHasSmallSpace_CannotBeMerged)
// {
//   // prepare an old parent
//   auto left_n = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(1, 6));  // 144B
//   auto right_n = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(7, 9));
//   node.reset(InternalNode_t::CreateNewRoot(left_n.get(), right_n.get()));

//   // prepare merging information
//   auto target_index = 1;
//   auto target_node_size = kHeaderLength + kWordLength + kRecordLength;  // 40B
//   auto max_merged_size = 128;

//   EXPECT_FALSE(InternalNode_t::CanMergeLeftSibling(node.get(), target_index, target_node_size,
//                                                    max_merged_size));
// }

// TYPED_TEST(InternalNodeFixture, CanMergeLeftSibling_NoSibling_CannotBeMerged)
// {
//   // prepare an old parent
//   auto left_n = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(1, 6));  // 144B
//   auto right_n = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(7, 9));
//   node.reset(InternalNode_t::CreateNewRoot(left_n.get(), right_n.get()));

//   // prepare merging information
//   auto target_index = 0;
//   auto target_node_size = kHeaderLength + kWordLength + kRecordLength;  // 40B
//   auto max_merged_size = 128;

//   EXPECT_FALSE(InternalNode_t::CanMergeLeftSibling(node.get(), target_index, target_node_size,
//                                                    max_merged_size));
// }

// TYPED_TEST(InternalNodeFixture, CanMergeRightSibling_SiblingHasSufficentSpace_CanBeMerged)
// {
//   // prepare an old parent
//   auto left_n = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(1, 6));
//   auto right_n = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(7, 9));  // 72B
//   node.reset(InternalNode_t::CreateNewRoot(left_n.get(), right_n.get()));

//   // prepare merging information
//   auto target_index = 0;
//   auto target_node_size = kHeaderLength + kWordLength + kRecordLength;  // 40B
//   auto max_merged_size = 128;

//   EXPECT_TRUE(InternalNode_t::CanMergeRightSibling(node.get(), target_index, target_node_size,
//                                                    max_merged_size));
// }

// TYPED_TEST(InternalNodeFixture, CanMergeRightSibling_SiblingHasSmallSpace_CannotBeMerged)
// {
//   // prepare an old parent
//   auto left_n = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(1, 3));
//   auto right_n = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(4, 9));  // 144B
//   node.reset(InternalNode_t::CreateNewRoot(left_n.get(), right_n.get()));

//   // prepare merging information
//   auto target_index = 0;
//   auto target_node_size = kHeaderLength + kWordLength + kRecordLength;  // 40B
//   auto max_merged_size = 128;

//   EXPECT_FALSE(InternalNode_t::CanMergeRightSibling(node.get(), target_index, target_node_size,
//                                                     max_merged_size));
// }

// TYPED_TEST(InternalNodeFixture, CanMergeRightSibling_NoSibling_CannotBeMerged)
// {
//   // prepare an old parent
//   auto left_n = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(1, 6));
//   auto right_n = std::unique_ptr<BaseNode_t>(CreateInternalNodeWithOrderedKeys(7, 9));  // 72B
//   node.reset(InternalNode_t::CreateNewRoot(left_n.get(), right_n.get()));

//   // prepare merging information
//   auto target_index = 1;
//   auto target_node_size = kHeaderLength + kWordLength + kRecordLength;  // 40B
//   auto max_merged_size = 128;

//   EXPECT_FALSE(InternalNode_t::CanMergeRightSibling(node.get(), target_index, target_node_size,
//                                                     max_merged_size));
// }

}  // namespace dbgroup::index::bztree
