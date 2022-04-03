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

#include "bztree/component/node.hpp"

#include <functional>
#include <memory>

#include "common.hpp"
#include "gtest/gtest.h"

namespace dbgroup::index::bztree::component::test
{
/*######################################################################################
 * Global constants
 *####################################################################################*/

constexpr size_t kMaxRecSize = 24 + sizeof(Metadata);
constexpr size_t kKeyNumForTest = 1e5;
constexpr bool kLeafFlag = true;
constexpr bool kExpectSuccess = true;
constexpr bool kExpectFailed = false;
constexpr bool kExpectKeyExist = true;
constexpr bool kExpectKeyNotExist = false;

template <class KeyType, class PayloadType>
struct KeyPayload {
  using Key = KeyType;
  using Payload = PayloadType;
};

template <class KeyPayload>
class NodeFixture : public testing::Test  // NOLINT
{
  // extract key-payload types
  using Key = typename KeyPayload::Key::Data;
  using Payload = typename KeyPayload::Payload::Data;
  using KeyComp = typename KeyPayload::Key::Comp;
  using PayloadComp = typename KeyPayload::Payload::Comp;

  // define type aliases for simplicity
  using Node_t = Node<Key, KeyComp>;

 protected:
  /*####################################################################################
   * Internal constants
   *##################################################################################*/

  static constexpr size_t kKeyLen = GetDataLength<Key>();
  static constexpr size_t kPayLen = GetDataLength<Payload>();
  static constexpr size_t kRecLen = std::get<2>(Align<Key, Payload>(kKeyLen, kPayLen));
  static constexpr size_t kRecNumInNode =
      (kPageSize - kHeaderLength) / (kRecLen + sizeof(Metadata));

  /*####################################################################################
   * Setup/Teardown
   *##################################################################################*/

  void
  SetUp() override
  {
    static_assert(kPageSize > kMaxRecSize * kMaxDeltaRecNum * 2 + kHeaderLength,
                  "The page size is too small to perform unit tests.");

    node_.reset(new Node_t{kLeafFlag, 0});

    PrepareTestData(keys_, kKeyNumForTest);
    PrepareTestData(payloads_, kKeyNumForTest);

    // set a record length and its maximum number
    if constexpr (CanCASUpdate<Payload>()) {
      const auto del_size = kRecLen + sizeof(Metadata);
      max_del_num_ = kMaxDeletedSpaceSize / del_size;
    } else {
      const auto del_size = kRecLen + kKeyLen + 2 * sizeof(Metadata);
      max_del_num_ = kMaxDeletedSpaceSize / del_size;
    }
    if (max_del_num_ > kMaxDeltaRecNum) {
      max_del_num_ = kMaxDeltaRecNum;
    }
  }

  void
  TearDown() override
  {
    ReleaseTestData(keys_, kKeyNumForTest);
    ReleaseTestData(payloads_, kKeyNumForTest);
  }

  /*####################################################################################
   * Operation wrappers
   *##################################################################################*/

  auto
  Write(  //
      const size_t key_id,
      const size_t payload_id)
  {
    return node_->Write(keys_[key_id], kKeyLen, payloads_[payload_id], kPayLen);
  }

  auto
  Insert(  //
      const size_t key_id,
      const size_t payload_id)
  {
    return node_->Insert(keys_[key_id], kKeyLen, payloads_[payload_id], kPayLen);
  }

  auto
  Update(  //
      const size_t key_id,
      const size_t payload_id)
  {
    return node_->Update(keys_[key_id], kKeyLen, payloads_[payload_id], kPayLen);
  }

  auto
  Delete(const size_t key_id)
  {
    return node_->template Delete<Payload>(keys_[key_id], kKeyLen);
  }

  void
  Consolidate()
  {
    auto *consolidated_node = new Node_t{kLeafFlag, 0};
    consolidated_node->template Consolidate<Payload>(node_.get());
    node_.reset(consolidated_node);
  }

  /*####################################################################################
   * Utility functions
   *##################################################################################*/

  void
  PrepareConsolidatedNode()
  {
    const size_t first_max_num = kMaxDeltaRecNum;
    const size_t second_max_num = 2 * first_max_num;

    // prepare consolidated node
    for (size_t i = 0; i < first_max_num; ++i) {
      Write(i, i);
    }
    Consolidate();
    for (size_t i = first_max_num; i < second_max_num; ++i) {
      Write(i, i);
    }
    Consolidate();
  }

  /*####################################################################################
   * Functions for verification
   *##################################################################################*/

  void
  VerifyRead(  //
      const size_t key_id,
      const size_t expected_id,
      const bool expect_success)
  {
    const NodeRC expected_rc = (expect_success) ? kSuccess : kKeyNotExist;

    Payload payload{};
    const auto rc = node_->Read(keys_[key_id], payload);

    EXPECT_EQ(expected_rc, rc);
    if (expect_success) {
      EXPECT_TRUE(IsEqual<PayloadComp>(payloads_[expected_id], payload));
      if constexpr (IsVariableLengthData<Payload>()) {
        ::operator delete(payload);
      }
    }
  }

  void
  VerifyWrite(  //
      const size_t key_id,
      const size_t payload_id,
      const bool expect_success)
  {
    const NodeRC expected_rc = (expect_success) ? kSuccess : kNeedConsolidation;
    auto rc = Write(key_id, payload_id);

    EXPECT_EQ(expected_rc, rc);
  }

  void
  VerifyInsert(  //
      const size_t key_id,
      const size_t payload_id,
      const bool expect_success,
      const bool expect_key_exist = false)
  {
    NodeRC expected_rc = kSuccess;
    if (!expect_success) {
      expected_rc = (expect_key_exist) ? kKeyExist : kNeedConsolidation;
    }
    auto rc = Insert(key_id, payload_id);

    EXPECT_EQ(expected_rc, rc);
  }

  void
  VerifyUpdate(  //
      const size_t key_id,
      const size_t payload_id,
      const bool expect_success,
      const bool expect_key_exist = false)
  {
    NodeRC expected_rc = kSuccess;
    if (!expect_success) {
      expected_rc = (expect_key_exist) ? kNeedConsolidation : kKeyNotExist;
    }
    auto rc = Update(key_id, payload_id);

    EXPECT_EQ(expected_rc, rc);
  }

  void
  VerifyDelete(  //
      const size_t key_id,
      const bool expect_success,
      const bool expect_key_exist = false)
  {
    NodeRC expected_rc = kSuccess;
    if (!expect_success) {
      expected_rc = (expect_key_exist) ? kNeedConsolidation : kKeyNotExist;
    }
    auto rc = Delete(key_id);

    EXPECT_EQ(expected_rc, rc);
  }

  void
  VerifySplit()
  {
    PrepareConsolidatedNode();

    auto *left_node = new Node_t{kLeafFlag, 0};
    auto *right_node = new Node_t{kLeafFlag, 0};
    node_->template Split<Payload>(left_node, right_node);

    node_.reset(left_node);
    for (size_t i = 0; i < kMaxDeltaRecNum; ++i) {
      VerifyRead(i, i, kExpectSuccess);
    }

    node_.reset(right_node);
    for (size_t i = kMaxDeltaRecNum; i < 2 * kMaxDeltaRecNum; ++i) {
      VerifyRead(i, i, kExpectSuccess);
    }
  }

  void
  VerifyMerge()
  {
    PrepareConsolidatedNode();

    auto *left_node = new Node_t{kLeafFlag, 0};
    auto *right_node = new Node_t{kLeafFlag, 0};
    node_->template Split<Payload>(left_node, right_node);
    Node_t *merged_node = left_node;
    merged_node->template Merge<Payload>(left_node, right_node);

    node_.reset(merged_node);
    for (size_t i = 0; i < 2 * kMaxDeltaRecNum; ++i) {
      VerifyRead(i, i, kExpectSuccess);
    }

    delete right_node;
  }

  void
  VerifyInitAsRoot()
  {
    PrepareConsolidatedNode();

    auto *left_node = new Node_t{kLeafFlag, 0};
    auto *right_node = new Node_t{kLeafFlag, 0};
    node_->template Split<Payload>(left_node, right_node);
    auto *root = new Node_t{!kLeafFlag, 0};
    root->InitAsRoot(left_node, right_node);

    EXPECT_EQ(left_node, root->GetChild(0));
    EXPECT_EQ(right_node, root->GetChild(1));

    delete left_node;
    delete right_node;
    delete root;
  }

  void
  VerifyInitAsSplitParent()
  {
    PrepareConsolidatedNode();

    auto *l_node = new Node_t{kLeafFlag, 0};
    auto *r_node = new Node_t{kLeafFlag, 0};
    node_->template Split<Payload>(l_node, r_node);
    auto *old_parent = new Node_t{!kLeafFlag, 0};
    old_parent->InitAsRoot(l_node, r_node);
    auto *r_l_node = new Node_t{kLeafFlag, 0};
    auto *r_r_node = new Node_t{kLeafFlag, 0};
    r_node->template Split<Payload>(r_l_node, r_r_node);
    auto *new_parent = new Node_t{!kLeafFlag, 0};
    new_parent->InitAsSplitParent(old_parent, r_node, r_r_node, 1);

    EXPECT_EQ(l_node, new_parent->GetChild(0));
    EXPECT_EQ(r_node, new_parent->GetChild(1));
    EXPECT_EQ(r_r_node, new_parent->GetChild(2));

    delete l_node;
    delete r_node;
    delete r_l_node;
    delete r_r_node;
    delete old_parent;
    delete new_parent;
  }

  void
  VerifyInitAsMergeParent()
  {
    PrepareConsolidatedNode();

    auto *l_node = new Node_t{kLeafFlag, 0};
    auto *r_node = new Node_t{kLeafFlag, 0};
    node_->template Split<Payload>(l_node, r_node);
    auto *old_parent = new Node_t{!kLeafFlag, 0};
    old_parent->InitAsRoot(l_node, r_node);
    Node_t *merged_node = l_node;
    merged_node->template Merge<Payload>(l_node, r_node);
    auto *new_parent = new Node_t{!kLeafFlag, 0};
    new_parent->InitAsMergeParent(old_parent, merged_node, 0);

    EXPECT_EQ(merged_node, new_parent->GetChild(0));

    delete l_node;
    delete r_node;
    delete old_parent;
    delete new_parent;
  }

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  // actual keys and payloads
  Key keys_[kKeyNumForTest];
  Payload payloads_[kKeyNumForTest];

  // the length of a record and its maximum number
  size_t max_del_num_{};

  std::unique_ptr<Node_t> node_{nullptr};
};

/*######################################################################################
 * Preparation for typed testing
 *####################################################################################*/

using KeyPayloadPairs = ::testing::Types<  //
    KeyPayload<UInt8, UInt8>,              // fixed keys and in-place payloads
    KeyPayload<Var, UInt8>,                // variable keys and in-place payloads
    KeyPayload<UInt8, Var>,                // fixed keys and variable payloads
    KeyPayload<Var, Var>,                  // variable keys/payloads
    KeyPayload<Ptr, Ptr>,                  // pointer keys/payloads
    KeyPayload<UInt8, Original>,           // original class payloads
    KeyPayload<UInt8, Int8>,               // fixed keys and appended payloads
    KeyPayload<Var, Int8>                  // variable keys and appended payloads
    >;
TYPED_TEST_SUITE(NodeFixture, KeyPayloadPairs);

/*######################################################################################
 * Write operation
 *####################################################################################*/

TYPED_TEST(NodeFixture, WriteWithUniqueKeysWOConsolidationReadWrittenValues)
{
  const size_t max_num = kMaxDeltaRecNum;

  // the node has capacity for new records
  for (size_t i = 0; i < max_num; ++i) {
    TestFixture::VerifyWrite(i, i, kExpectSuccess);
  }

  // the node requires consolidation now
  TestFixture::VerifyWrite(max_num, max_num, kExpectFailed);

  // the node has the written records
  for (size_t i = 0; i < max_num; ++i) {
    TestFixture::VerifyRead(i, i, kExpectSuccess);
  }
}

TYPED_TEST(NodeFixture, WriteWithUniqueKeysWithConsolidationReadWrittenValues)
{
  const size_t first_max_num = kMaxDeltaRecNum;
  const size_t second_max_num = 2 * first_max_num;

  // the node has capacity for new records
  for (size_t i = 0; i < first_max_num; ++i) {
    TestFixture::VerifyWrite(i, i, kExpectSuccess);
  }

  // perform consolidation to write records
  TestFixture::Consolidate();

  // the consolidated node has capacity for new records
  for (size_t i = first_max_num; i < second_max_num; ++i) {
    TestFixture::VerifyWrite(i, i, kExpectSuccess);
  }

  // the node requires consolidation now
  TestFixture::VerifyWrite(second_max_num, second_max_num, kExpectFailed);

  // the node has the written records
  for (size_t i = 0; i < second_max_num; ++i) {
    TestFixture::VerifyRead(i, i, kExpectSuccess);
  }
}

TYPED_TEST(NodeFixture, WriteWithDuplicateKeysWOConsolidationReadLatestValues)
{
  const size_t max_num = kMaxDeltaRecNum / 2;

  // write base records
  for (size_t i = 0; i < max_num; ++i) {
    TestFixture::VerifyWrite(i, i, kExpectSuccess);
  }

  // overwrite the records with different values
  for (size_t i = 0; i < max_num; ++i) {
    TestFixture::VerifyWrite(i, i + 1, kExpectSuccess);
  }

  // the node has the written records
  for (size_t i = 0; i < max_num; ++i) {
    TestFixture::VerifyRead(i, i + 1, kExpectSuccess);
  }
}

TYPED_TEST(NodeFixture, WriteWithDuplicateKeysWithConsolidationReadLatestValues)
{
  const size_t max_num = kMaxDeltaRecNum;

  // write base records
  for (size_t i = 0; i < max_num; ++i) {
    TestFixture::VerifyWrite(i, i, kExpectSuccess);
  }

  // perform consolidation to write records
  TestFixture::Consolidate();

  // overwrite the records with different values
  for (size_t i = 0; i < max_num; ++i) {
    TestFixture::VerifyWrite(i, i + 1, kExpectSuccess);
  }

  // the node has the written records
  for (size_t i = 0; i < max_num; ++i) {
    TestFixture::VerifyRead(i, i + 1, kExpectSuccess);
  }
}

/*--------------------------------------------------------------------------------------
 * Insert operation
 *------------------------------------------------------------------------------------*/

TYPED_TEST(NodeFixture, InsertWithUniqueKeysWOConsolidationReadWrittenValues)
{
  const size_t max_num = kMaxDeltaRecNum;

  // the node has capacity for new records
  for (size_t i = 0; i < max_num; ++i) {
    TestFixture::VerifyInsert(i, i, kExpectSuccess);
  }

  // the node requires consolidation now
  TestFixture::VerifyInsert(max_num, max_num, kExpectFailed, kExpectKeyNotExist);

  // the node has the written records
  for (size_t i = 0; i < max_num; ++i) {
    TestFixture::VerifyRead(i, i, kExpectSuccess);
  }
}

TYPED_TEST(NodeFixture, InsertWithUniqueKeysWithConsolidationReadWrittenValues)
{
  const size_t first_max_num = kMaxDeltaRecNum;
  const size_t second_max_num = 2 * first_max_num;

  // the node has capacity for new records
  for (size_t i = 0; i < first_max_num; ++i) {
    TestFixture::VerifyInsert(i, i, kExpectSuccess);
  }

  // perform consolidation to write records
  TestFixture::Consolidate();

  // the consolidated node has capacity for new records
  for (size_t i = first_max_num; i < second_max_num; ++i) {
    TestFixture::VerifyInsert(i, i, kExpectSuccess);
  }

  // the node requires consolidation now
  TestFixture::VerifyInsert(second_max_num, second_max_num, kExpectFailed, kExpectKeyNotExist);

  // the node has the written records
  for (size_t i = 0; i < second_max_num; ++i) {
    TestFixture::VerifyRead(i, i, kExpectSuccess);
  }
}

TYPED_TEST(NodeFixture, InsertWithDuplicateKeysWOConsolidationFail)
{
  const size_t max_num = kMaxDeltaRecNum / 2;

  // write base records
  for (size_t i = 0; i < max_num; ++i) {
    TestFixture::VerifyInsert(i, i, kExpectSuccess);
  }

  // insert operations will fail with inserted-keys
  for (size_t i = 0; i < max_num; ++i) {
    TestFixture::VerifyInsert(i, i + 1, kExpectFailed, kExpectKeyExist);
  }

  // the node has the written records
  for (size_t i = 0; i < max_num; ++i) {
    TestFixture::VerifyRead(i, i, kExpectSuccess);
  }
}

TYPED_TEST(NodeFixture, InsertWithDuplicateKeysWithConsolidationFail)
{
  const size_t max_num = kMaxDeltaRecNum;

  // write base records
  for (size_t i = 0; i < max_num; ++i) {
    TestFixture::VerifyInsert(i, i, kExpectSuccess);
  }

  // perform consolidation to write records
  TestFixture::Consolidate();

  // insert operations will fail with inserted-keys
  for (size_t i = 0; i < max_num; ++i) {
    TestFixture::VerifyInsert(i, i + 1, kExpectFailed, kExpectKeyExist);
  }

  // the node has the written records
  for (size_t i = 0; i < max_num; ++i) {
    TestFixture::VerifyRead(i, i, kExpectSuccess);
  }
}

/*--------------------------------------------------------------------------------------
 * Update operation
 *------------------------------------------------------------------------------------*/

TYPED_TEST(NodeFixture, UpdateWithUniqueKeysWOConsolidationFail)
{
  const size_t max_num = kMaxDeltaRecNum / 2;

  // write base records
  for (size_t i = 0; i < max_num; ++i) {
    TestFixture::Write(i, i);
  }

  // update operations will fail with not-inserted keys
  for (size_t i = max_num; i < 2 * max_num; ++i) {
    TestFixture::VerifyUpdate(i, i + 1, kExpectFailed, kExpectKeyNotExist);
  }

  // the node has the written records
  for (size_t i = 0; i < max_num; ++i) {
    TestFixture::VerifyRead(i, i, kExpectSuccess);
  }

  // the failed update operations do not modify the node
  for (size_t i = max_num; i < 2 * max_num; ++i) {
    TestFixture::VerifyRead(i, i + 1, kExpectFailed);
  }
}

TYPED_TEST(NodeFixture, UpdateWithUniqueKeysWithConsolidationFail)
{
  const size_t first_max_num = kMaxDeltaRecNum;
  const size_t second_max_num = 2 * first_max_num;

  // write base records
  for (size_t i = 0; i < first_max_num; ++i) {
    TestFixture::Write(i, i);
  }

  // perform consolidation to write records
  TestFixture::Consolidate();

  // update operations will fail with not-inserted keys
  for (size_t i = first_max_num; i < second_max_num; ++i) {
    TestFixture::VerifyUpdate(i, i + 1, kExpectFailed, kExpectKeyNotExist);
  }

  // the node has the written records
  for (size_t i = 0; i < first_max_num; ++i) {
    TestFixture::VerifyRead(i, i, kExpectSuccess);
  }

  // the node has the written records
  for (size_t i = first_max_num; i < second_max_num; ++i) {
    TestFixture::VerifyRead(i, i + 1, kExpectFailed);
  }
}

TYPED_TEST(NodeFixture, UpdateWithDuplicateKeysWOConsolidationReadLatestValues)
{
  const size_t max_num = kMaxDeltaRecNum / 2;

  // write base records
  for (size_t i = 0; i < max_num; ++i) {
    TestFixture::Write(i, i);
  }

  // overwrite the records with different values
  for (size_t i = 0; i < max_num; ++i) {
    TestFixture::VerifyUpdate(i, i + 1, kExpectSuccess);
  }

  // the node has the written records
  for (size_t i = 0; i < max_num; ++i) {
    TestFixture::VerifyRead(i, i + 1, kExpectSuccess);
  }
}

TYPED_TEST(NodeFixture, UpdateWithDuplicateKeysWithConsolidationReadLatestValues)
{
  const size_t max_num = kMaxDeltaRecNum;

  // write base records
  for (size_t i = 0; i < max_num; ++i) {
    TestFixture::Write(i, i);
  }

  // perform consolidation to write records
  TestFixture::Consolidate();

  // overwrite the records with different values
  for (size_t i = 0; i < max_num; ++i) {
    TestFixture::VerifyUpdate(i, i + 1, kExpectSuccess);
  }

  // the node has the written records
  for (size_t i = 0; i < max_num; ++i) {
    TestFixture::VerifyRead(i, i + 1, kExpectSuccess);
  }
}

/*--------------------------------------------------------------------------------------
 * Delete operation
 *------------------------------------------------------------------------------------*/

TYPED_TEST(NodeFixture, DeleteWithUniqueKeysWOConsolidationFail)
{
  const size_t max_num = kMaxDeltaRecNum / 2;

  // write base records
  for (size_t i = 0; i < max_num; ++i) {
    TestFixture::Write(i, i);
  }

  // delete operations will fail with not-inserted keys
  for (size_t i = max_num; i < 2 * max_num; ++i) {
    TestFixture::VerifyDelete(i, kExpectFailed, kExpectKeyNotExist);
  }

  // the node has the written records
  for (size_t i = 0; i < max_num; ++i) {
    TestFixture::VerifyRead(i, i, kExpectSuccess);
  }
}

TYPED_TEST(NodeFixture, DeleteWithUniqueKeysWithConsolidationFail)
{
  const size_t first_max_num = kMaxDeltaRecNum;
  const size_t second_max_num = 2 * first_max_num;

  // write base records
  for (size_t i = 0; i < first_max_num; ++i) {
    TestFixture::Write(i, i);
  }

  // perform consolidation to write records
  TestFixture::Consolidate();

  // delete operations will fail with not-inserted keys
  for (size_t i = first_max_num; i < second_max_num; ++i) {
    TestFixture::VerifyDelete(i, kExpectFailed, kExpectKeyNotExist);
  }

  // the node has the written records
  for (size_t i = 0; i < first_max_num; ++i) {
    TestFixture::VerifyRead(i, i, kExpectSuccess);
  }
}

TYPED_TEST(NodeFixture, DeleteWithDuplicateKeysWOConsolidationDeleteWrittenRecords)
{
  const size_t max_num = TestFixture::max_del_num_ / 2;

  // write base records
  for (size_t i = 0; i < max_num; ++i) {
    TestFixture::Write(i, i);
  }

  // delete inserted keys
  for (size_t i = 0; i < max_num; ++i) {
    TestFixture::VerifyDelete(i, kExpectSuccess);
  }

  // the node does not have deleted records
  for (size_t i = 0; i < max_num; ++i) {
    TestFixture::VerifyRead(i, i, kExpectFailed);
  }
}

TYPED_TEST(NodeFixture, DeleteWithDuplicateKeysWithConsolidationDeleteWrittenRecords)
{
  const size_t max_num = TestFixture::max_del_num_;

  // write base records
  for (size_t i = 0; i < max_num; ++i) {
    TestFixture::Write(i, i);
  }

  // perform consolidation to write records
  TestFixture::Consolidate();

  // delete inserted keys
  for (size_t i = 0; i < max_num; ++i) {
    TestFixture::VerifyDelete(i, kExpectSuccess);
  }

  // the node does not have deleted records
  for (size_t i = 0; i < max_num; ++i) {
    TestFixture::VerifyRead(i, i, kExpectFailed);
  }
}

/*--------------------------------------------------------------------------------------
 * SMO operation
 *------------------------------------------------------------------------------------*/

TYPED_TEST(NodeFixture, ConsolidateWithSortedAndUnsortedRecordsCreateConsolidatedNode)
{
  TestFixture::PrepareConsolidatedNode();

  // the node has the both sorted/unsorted records
  for (size_t i = 0; i < 2 * kMaxDeltaRecNum; ++i) {
    TestFixture::VerifyRead(i, i, kExpectSuccess);
  }
}

TYPED_TEST(NodeFixture, SplitDivideWrittenRecordsIntoTwoNodes)
{  //
  TestFixture::VerifySplit();
}

TYPED_TEST(NodeFixture, MergeCopyWrittenRecordsIntoSingleNode)
{  //
  TestFixture::VerifyMerge();
}

TYPED_TEST(NodeFixture, InitAsRootCreateRootWithTwoChildren)
{  //
  TestFixture::VerifyInitAsRoot();
}

TYPED_TEST(NodeFixture, InitAsSplitParentCreateParentWithChildren)
{
  TestFixture::VerifyInitAsSplitParent();
}

TYPED_TEST(NodeFixture, InitAsMergeParentCreateParentWithChild)
{
  TestFixture::VerifyInitAsMergeParent();
}

}  // namespace dbgroup::index::bztree::component::test
