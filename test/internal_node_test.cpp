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
  PrepareDummyNode(  //
      const size_t child_num,
      const size_t payload_begin = 0)
  {
    auto dummy_node = BaseNode_t::CreateEmptyNode(kInternalFlag);

    // embeds dummy childrens
    auto offset = kPageSize;
    for (size_t i = 0; i < child_num; ++i) {
      // set a key and a dummy payload
      offset = dummy_node->SetPayload(offset, payload_begin + i, kWordLength);
      offset = dummy_node->SetKey(offset, keys[i], key_length);

      // set a corresponding metadata
      const auto meta = Metadata{}.SetRecordInfo(offset, key_length, key_length + kWordLength);
      dummy_node->SetMetadata(i, meta);
    }

    const auto status = StatusWord{}.AddRecordInfo(child_num, child_num * record_length, 0);
    dummy_node->SetStatus(status);
    dummy_node->SetSortedCount(kDummyNodeNum);

    return dummy_node;
  }

  void
  ReleaseChildren()
  {
    for (size_t i = 0; i < node->GetSortedCount(); ++i) {
      delete InternalNode_t::GetChildNode(node.get(), i);
    }
  }

  /*################################################################################################
   * Functions for verification
   *##############################################################################################*/

  void
  VerifyInternalNode(  //
      const BaseNode_t* target_node,
      const size_t child_num)
  {
    EXPECT_FALSE(target_node->IsLeaf());
    EXPECT_EQ(child_num, target_node->GetSortedCount());
    EXPECT_FALSE(target_node->GetStatusWord().IsFrozen());
  }

  void
  VerifyChildren(  //
      const BaseNode_t* target_node,
      const size_t child_num,
      const bool child_is_leaf)
  {
    for (size_t i = 0; i < child_num; ++i) {
      auto child = InternalNode_t::GetChildNode(target_node, i);
      EXPECT_FALSE(child->IsLeaf() ^ child_is_leaf);
    }
  }

  void
  VerifyDummyChildren(  //
      const BaseNode_t* target_node,
      const size_t child_num,
      const size_t begin_payload)
  {
    for (size_t i = 0; i < child_num; ++i) {
      auto child = InternalNode_t::GetChildNode(target_node, i);
      EXPECT_EQ(begin_payload + i, reinterpret_cast<uintptr_t>(child));
    }
  }

  void
  VerifyGetChildNode()
  {
    node.reset(PrepareDummyNode(kDummyNodeNum));

    VerifyDummyChildren(node.get(), 1, 0);
  }

  void
  VerifyNeedSplit(const bool expect_split)
  {
    if (expect_split) {
      node.reset(PrepareDummyNode(max_record_num));
      EXPECT_TRUE(InternalNode_t::NeedSplit(node.get(), key_length));
    } else {
      node.reset(PrepareDummyNode(max_record_num - 1));
      EXPECT_FALSE(InternalNode_t::NeedSplit(node.get(), key_length));
    }
  }

  void
  VerifyInitialRoot()
  {
    node.reset(InternalNode_t::CreateInitialRoot());

    VerifyInternalNode(node.get(), 1);
    VerifyChildren(node.get(), 1, true);

    ReleaseChildren();
  }

  void
  VerifySplit()
  {
    node.reset(PrepareDummyNode(kDummyNodeNum));

    const size_t left_rec_count = kDummyNodeNum / 2;
    auto [left_node, right_node] = InternalNode_t::Split(node.get(), left_rec_count);

    VerifyInternalNode(left_node, left_rec_count);
    VerifyDummyChildren(left_node, left_rec_count, 0);
    VerifyInternalNode(right_node, kDummyNodeNum - left_rec_count);
    VerifyDummyChildren(right_node, kDummyNodeNum - left_rec_count, left_rec_count);

    delete left_node;
    delete right_node;
  }

  void
  VerifyMerge()
  {
    BaseNode_t *sibling_node, *merged_node;
    node.reset(PrepareDummyNode(kDummyNodeNum));
    sibling_node = PrepareDummyNode(kDummyNodeNum, kDummyNodeNum);

    merged_node = InternalNode_t::Merge(node.get(), sibling_node);

    VerifyInternalNode(merged_node, kDummyNodeNum * 2);
    VerifyDummyChildren(merged_node, kDummyNodeNum * 2, 0);

    delete sibling_node;
    delete merged_node;
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

TYPED_TEST(InternalNodeFixture, NeedSplit_HasOneRecordSpace_SplitNotRequired)
{
  TestFixture::VerifyNeedSplit(false);
}

TYPED_TEST(InternalNodeFixture, NeedSplit_FilledNode_SplitRequired)
{
  TestFixture::VerifyNeedSplit(true);
}

TYPED_TEST(InternalNodeFixture, CanMergeLeftSibling_SiblingHasSufficentSpace_CanBeMerged) {}

TYPED_TEST(InternalNodeFixture, CanMergeLeftSibling_SiblingHasSmallSpace_CannotBeMerged) {}

TYPED_TEST(InternalNodeFixture, CanMergeLeftSibling_NoSibling_CannotBeMerged) {}

TYPED_TEST(InternalNodeFixture, CanMergeRightSibling_SiblingHasSufficentSpace_CanBeMerged) {}

TYPED_TEST(InternalNodeFixture, CanMergeRightSibling_SiblingHasSmallSpace_CannotBeMerged) {}

TYPED_TEST(InternalNodeFixture, CanMergeRightSibling_NoSibling_CannotBeMerged) {}

TYPED_TEST(InternalNodeFixture, CreateInitialRoot_Default_RootHasOneLeaf)
{
  TestFixture::VerifyInitialRoot();
}

TYPED_TEST(InternalNodeFixture, Split_DummyChildren_ChildrenEquallyDivided)
{
  TestFixture::VerifySplit();
}

TYPED_TEST(InternalNodeFixture, Merge_DummyChildren_MergedNodeHasAllChildren)
{
  TestFixture::VerifyMerge();
}

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

}  // namespace dbgroup::index::bztree
