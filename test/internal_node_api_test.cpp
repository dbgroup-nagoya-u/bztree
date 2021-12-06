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

#include "bztree/component/internal_node_api.hpp"

#include <memory>

#include "bztree/component/leaf_node_api.hpp"
#include "common.hpp"
#include "gtest/gtest.h"

namespace dbgroup::index::bztree::internal::test
{
// use a supper template to define key-payload pair templates
template <class KeyType, class PayloadType, class KeyComparator, class PayloadComparator>
struct KeyPayload {
  using Key = KeyType;
  using Payload = PayloadType;
  using KeyComp = KeyComparator;
  using PayloadComp = PayloadComparator;
};

template <class KeyPayload>
class InternalNodeFixture : public testing::Test
{
 protected:
  // extract key-payload types
  using Key = typename KeyPayload::Key;
  using Payload = typename KeyPayload::Payload;
  using KeyComp = typename KeyPayload::KeyComp;
  using PayloadComp = typename KeyPayload::PayloadComp;

  // define type aliases for simplicity
  using Node_t = component::Node<Key, Payload, KeyComp>;

  // constant values for testing
  static constexpr size_t kKeyNumForTest = 1024;
  static constexpr size_t kKeyLength = kWordLength;
  static constexpr size_t kPayloadLength = kWordLength;
  static constexpr size_t kMaxNodeSize = kPageSize / 2;
  static constexpr size_t kDummyNodeNum = 10;
  static constexpr bool kInterFlag = false;

  // actual keys and payloads
  size_t key_length;
  Key keys[kKeyNumForTest];

  // the length of a record and its maximum number
  size_t record_length;
  size_t max_record_num;

  // a leaf node
  std::unique_ptr<Node_t> node;

  /*################################################################################################
   * Setup/Teardown
   *##############################################################################################*/

  void
  SetUp()
  {
    // initialize a leaf node and expected statistics
    node.reset(new Node_t{!kLeafFlag});

    // prepare keys
    key_length = (IsVariableLengthData<Key>()) ? 7 : sizeof(Key);
    PrepareTestData(keys, kKeyNumForTest, key_length);

    // set a record length and its maximum number
    record_length = 2 * kWordLength;
    max_record_num = (kPageSize - component::kHeaderLength) / (record_length + kWordLength);
  }

  void
  TearDown()
  {
    ReleaseTestData(keys, kKeyNumForTest);
  }

  /*################################################################################################
   * Utility functions
   *##############################################################################################*/

  Node_t *
  PrepareDummyNode(  //
      const size_t child_num,
      const size_t payload_begin = 0)
  {
    auto dummy_node = new Node_t{!kLeafFlag};

    // embeds dummy childrens
    auto offset = kPageSize;
    for (size_t i = 0; i < child_num; ++i) {
      // set a key and a dummy payload
      dummy_node->SetPayload(offset, payload_begin + i, kWordLength);
      dummy_node->SetKey(offset, keys[i], key_length);

      // set a corresponding metadata
      const auto meta = Metadata{offset, key_length, key_length + kWordLength};
      dummy_node->SetMetadata(i, meta);

      offset -= offset & (kWordLength - 1);
    }

    const auto status = StatusWord{child_num, kPageSize - offset};
    dummy_node->SetStatus(status);
    dummy_node->SetSortedCount(kDummyNodeNum);

    return dummy_node;
  }

  void
  ReleaseChildren()
  {
    for (size_t i = 0; i < node->GetSortedCount(); ++i) {
      delete internal::GetChildNode(node.get(), i);
    }
  }

  /*################################################################################################
   * Functions for verification
   *##############################################################################################*/

  void
  VerifyInternalNode(  //
      const Node_t *target_node,
      const size_t child_num)
  {
    EXPECT_FALSE(target_node->IsLeaf());
    EXPECT_EQ(child_num, target_node->GetSortedCount());
    EXPECT_FALSE(target_node->GetStatusWord().IsFrozen());
  }

  void
  VerifyChildren(  //
      const Node_t *target_node,
      const size_t child_num,
      const std::vector<Node_t *> *expected_children)
  {
    for (size_t i = 0; i < child_num; ++i) {
      auto child = internal::GetChildNode(target_node, i);
      if (expected_children != nullptr) {
        EXPECT_TRUE(expected_children->at(i) == child);
      }
    }
  }

  void
  VerifyDummyChildren(  //
      const Node_t *target_node,
      const size_t child_num,
      const size_t begin_payload)
  {
    for (size_t i = 0; i < child_num; ++i) {
      auto child = internal::GetChildNode(target_node, i);
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
  VerifyInitialRoot()
  {
    node.reset(internal::CreateInitialRoot<Key, Payload, KeyComp>());

    VerifyInternalNode(node.get(), 1);
    VerifyChildren(node.get(), 1, nullptr);

    ReleaseChildren();
  }

  void
  VerifySplit()
  {
    node.reset(PrepareDummyNode(kDummyNodeNum));

    const size_t left_rec_count = kDummyNodeNum / 2;
    auto *left_node = new Node_t{kInterFlag};
    auto *right_node = new Node_t{kInterFlag};
    internal::Split(left_node, right_node, node.get(), left_rec_count);

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
    Node_t *sibling_node;
    node.reset(PrepareDummyNode(kDummyNodeNum));
    sibling_node = PrepareDummyNode(kDummyNodeNum, kDummyNodeNum);

    auto *merged_node = new Node_t{kInterFlag};
    internal::Merge(merged_node, node.get(), sibling_node);

    VerifyInternalNode(merged_node, kDummyNodeNum * 2);
    VerifyDummyChildren(merged_node, kDummyNodeNum * 2, 0);

    delete sibling_node;
    delete merged_node;
  }

  void
  VerifyCreateNewRoot()
  {
    auto left_node = PrepareDummyNode(kDummyNodeNum);
    auto right_node = PrepareDummyNode(kDummyNodeNum);
    std::vector<Node_t *> expected_children = {left_node, right_node};

    auto *new_root = new Node_t{kInterFlag};
    internal::CreateNewRoot(new_root, left_node, right_node);
    node.reset(new_root);

    VerifyInternalNode(node.get(), 2);
    VerifyChildren(node.get(), 2, &expected_children);

    ReleaseChildren();
  }

  void
  VerifyNewParentForSplit()
  {
    auto init_left = PrepareDummyNode(kDummyNodeNum);
    auto init_right = PrepareDummyNode(kDummyNodeNum);
    auto left_left = PrepareDummyNode(kDummyNodeNum);
    auto left_right = PrepareDummyNode(kDummyNodeNum);
    auto right_left = PrepareDummyNode(kDummyNodeNum);
    auto right_right = PrepareDummyNode(kDummyNodeNum);

    auto *new_node = new Node_t{kInterFlag};
    internal::CreateNewRoot(new_node, init_left, init_right);
    node.reset(new_node);
    new_node = new Node_t{kInterFlag};
    internal::NewParentForSplit(new_node, node.get(), left_left, left_right, 0);
    node.reset(new_node);
    new_node = new Node_t{kInterFlag};
    internal::NewParentForSplit(new_node, node.get(), right_left, right_right, 2);
    node.reset(new_node);

    std::vector<Node_t *> expected_children = {left_left, left_right, right_left, right_right};

    VerifyInternalNode(node.get(), 4);
    VerifyChildren(node.get(), 4, &expected_children);

    ReleaseChildren();

    delete init_left;
    delete init_right;
  }

  void
  VerifyNewParentForMerge()
  {
    auto init_left = PrepareDummyNode(kDummyNodeNum);
    auto init_right = PrepareDummyNode(kDummyNodeNum);
    auto left_left = PrepareDummyNode(kDummyNodeNum);
    auto left_right = PrepareDummyNode(kDummyNodeNum);
    auto right_left = PrepareDummyNode(kDummyNodeNum);
    auto right_right = PrepareDummyNode(kDummyNodeNum);

    auto *new_node = new Node_t{kInterFlag};
    internal::CreateNewRoot(new_node, init_left, init_right);
    node.reset(new_node);
    new_node = new Node_t{kInterFlag};
    internal::NewParentForSplit(new_node, node.get(), left_left, left_right, 0);
    node.reset(new_node);
    new_node = new Node_t{kInterFlag};
    internal::NewParentForSplit(new_node, node.get(), right_left, right_right, 2);
    node.reset(new_node);
    new_node = new Node_t{kInterFlag};
    internal::NewParentForMerge(new_node, node.get(), init_left, 0);
    node.reset(new_node);
    new_node = new Node_t{kInterFlag};
    internal::NewParentForMerge(new_node, node.get(), init_right, 1);
    node.reset(new_node);

    std::vector<Node_t *> expected_children = {init_left, init_right};

    VerifyInternalNode(node.get(), 2);
    VerifyChildren(node.get(), 2, &expected_children);

    ReleaseChildren();

    delete left_left;
    delete left_right;
    delete right_left;
    delete right_right;
  }
};

/*##################################################################################################
 * Preparation for typed testing
 *################################################################################################*/

using KeyPayloadPairs = ::testing::Types<KeyPayload<uint64_t, uint64_t, UInt64Comp, UInt64Comp>,
                                         KeyPayload<uint32_t, uint64_t, UInt32Comp, UInt64Comp>,
                                         KeyPayload<char *, uint64_t, CStrComp, UInt64Comp>>;
TYPED_TEST_CASE(InternalNodeFixture, KeyPayloadPairs);

/*##################################################################################################
 * Unit test definitions
 *################################################################################################*/

TYPED_TEST(InternalNodeFixture, GetChildNode_DummyChildren_ReadDummyValues)
{
  TestFixture::VerifyGetChildNode();
}

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

TYPED_TEST(InternalNodeFixture, CreateNewRoot_DummyChildren_RootHasCorrectChild)
{
  TestFixture::VerifyCreateNewRoot();
}

TYPED_TEST(InternalNodeFixture, NewParentForSplit_DummyChildren_ParentHasCorrectChild)
{
  TestFixture::VerifyNewParentForSplit();
}

TYPED_TEST(InternalNodeFixture, NewParentForMerge_DummyChildren_ParentHasCorrectChild)
{
  TestFixture::VerifyNewParentForMerge();
}

}  // namespace dbgroup::index::bztree::internal::test
