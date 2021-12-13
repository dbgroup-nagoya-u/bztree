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
/*##################################################################################################
 * Global constants
 *################################################################################################*/

constexpr size_t kVarDataLength = 9;
constexpr size_t kMaxRecSize = 24 + sizeof(Metadata);
constexpr size_t kKeyNumForTest = 1e5;
constexpr bool kLeafFlag = true;
constexpr bool kExpectSuccess = true;
constexpr bool kExpectFailed = false;
constexpr bool kExpectKeyExist = true;
constexpr bool kExpectKeyNotExist = false;

template <class KeyType, class PayloadType, class KeyComparator, class PayloadComparator>
struct KeyPayload {
  using Key = KeyType;
  using Payload = PayloadType;
  using KeyComp = KeyComparator;
  using PayloadComp = PayloadComparator;
};

template <class KeyPayload>
class NodeFixture : public testing::Test
{
  // extract key-payload types
  using Key = typename KeyPayload::Key;
  using Payload = typename KeyPayload::Payload;
  using KeyComp = typename KeyPayload::KeyComp;
  using PayloadComp = typename KeyPayload::PayloadComp;

  // define type aliases for simplicity
  using Node_t = Node<Key, Payload, KeyComp>;

 protected:
  /*################################################################################################
   * Setup/Teardown
   *##############################################################################################*/

  void
  SetUp() override
  {
    static_assert(kPageSize > kMaxRecSize * kMaxUnsortedRecNum * 2 + kHeaderLength,
                  "The page size is too small to perform unit tests.");

    node.reset(new Node_t{kLeafFlag});

    // prepare keys
    key_size_ = (IsVariableLengthData<Key>()) ? kVarDataLength : sizeof(Key);
    PrepareTestData(keys_, kKeyNumForTest, key_size_);

    // prepare payloads
    pay_size_ = (IsVariableLengthData<Payload>()) ? kVarDataLength : sizeof(Payload);
    PrepareTestData(payloads_, kKeyNumForTest, pay_size_);

    // set a record length and its maximum number
    const auto rec_size = PadRecord<Key, Payload>(key_size_ + pay_size_) + sizeof(Metadata);
    max_rec_num_ = (kPageSize - kHeaderLength) / rec_size;
    if constexpr (CanCASUpdate<Payload>()) {
      const auto del_size = rec_size + sizeof(Metadata);
      max_del_num_ = kMaxDeletedSpaceSize / del_size;
    } else {
      const auto del_size = rec_size + key_size_ + 2 * sizeof(Metadata);
      max_del_num_ = kMaxDeletedSpaceSize / del_size;
    }
  }

  void
  TearDown() override
  {
    ReleaseTestData(keys_, kKeyNumForTest);
    ReleaseTestData(payloads_, kKeyNumForTest);
  }

  /*################################################################################################
   * Operation wrappers
   *##############################################################################################*/

  auto
  Write(  //
      const size_t key_id,
      const size_t payload_id)
  {
    return node->Write(keys_[key_id], key_size_, payloads_[payload_id], pay_size_);
  }

  auto
  Insert(  //
      const size_t key_id,
      const size_t payload_id)
  {
    return node->Insert(keys_[key_id], key_size_, payloads_[payload_id], pay_size_);
  }

  auto
  Update(  //
      const size_t key_id,
      const size_t payload_id)
  {
    return node->Update(keys_[key_id], key_size_, payloads_[payload_id], pay_size_);
  }

  auto
  Delete(const size_t key_id)
  {
    return node->Delete(keys_[key_id], key_size_);
  }

  void
  Consolidate()
  {
    Node_t *consolidated_node = new Node_t{kLeafFlag};
    consolidated_node->Consolidate(node.get());
    node.reset(consolidated_node);
  }

  /*################################################################################################
   * Utility functions
   *##############################################################################################*/

  void
  PrepareConsolidatedNode()
  {
    const size_t first_max_num = kMaxUnsortedRecNum;
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

  /*################################################################################################
   * Functions for verification
   *##############################################################################################*/

  void
  VerifyRead(  //
      const size_t key_id,
      const size_t expected_id,
      const bool expect_success)
  {
    const NodeReturnCode expected_rc = (expect_success) ? kSuccess : kKeyNotExist;

    Payload payload{};
    const auto rc = node->Read(keys_[key_id], payload);

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
    const NodeReturnCode expected_rc = (expect_success) ? kSuccess : kNeedConsolidation;
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
    NodeReturnCode expected_rc = kSuccess;
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
    NodeReturnCode expected_rc = kSuccess;
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
    NodeReturnCode expected_rc = kSuccess;
    if (!expect_success) {
      expected_rc = (expect_key_exist) ? kNeedConsolidation : kKeyNotExist;
    }
    auto rc = Delete(key_id);

    EXPECT_EQ(expected_rc, rc);
  }

  void
  VerifyLeafSplit()
  {
    PrepareConsolidatedNode();

    Node_t *left_node = node.get();
    Node_t *right_node = new Node_t{kLeafFlag};
    Node_t::template Split<Payload>(left_node, left_node, right_node);

    for (size_t i = 0; i < kMaxUnsortedRecNum; ++i) {
      VerifyRead(i, i, kExpectSuccess);
    }

    node.reset(right_node);
    for (size_t i = kMaxUnsortedRecNum; i < 2 * kMaxUnsortedRecNum; ++i) {
      VerifyRead(i, i, kExpectSuccess);
    }
  }

  void
  VerifyInternalSplit()
  {
    if constexpr (sizeof(Payload) == kWordLength && !std::is_pointer_v<Payload>) {
      PrepareConsolidatedNode();

      Node_t *left_node = new Node_t{kLeafFlag};
      Node_t *right_node = new Node_t{kLeafFlag};
      Node_t::template Split<Node_t *>(node.get(), left_node, right_node);

      node.reset(left_node);
      for (size_t i = 0; i < kMaxUnsortedRecNum; ++i) {
        VerifyRead(i, i, kExpectSuccess);
      }

      node.reset(right_node);
      for (size_t i = kMaxUnsortedRecNum; i < 2 * kMaxUnsortedRecNum; ++i) {
        VerifyRead(i, i, kExpectSuccess);
      }
    }
  }

  void
  VerifyLeafMerge()
  {
    PrepareConsolidatedNode();

    Node_t *left_node = node.get();
    Node_t *right_node = new Node_t{kLeafFlag};
    Node_t::template Split<Payload>(left_node, left_node, right_node);
    Node_t::template Merge<Payload>(left_node, right_node, left_node);
    delete right_node;

    for (size_t i = 0; i < 2 * kMaxUnsortedRecNum; ++i) {
      VerifyRead(i, i, kExpectSuccess);
    }
  }

  void
  VerifyInternalMerge()
  {
    if constexpr (sizeof(Payload) == kWordLength && !std::is_pointer_v<Payload>) {
      PrepareConsolidatedNode();

      Node_t *left_node = new Node_t{kLeafFlag};
      Node_t *right_node = new Node_t{kLeafFlag};
      Node_t::template Split<Node_t *>(node.get(), left_node, right_node);
      Node_t *merged_node = new Node_t{kLeafFlag};
      Node_t::template Merge<Node_t *>(left_node, right_node, merged_node);
      delete left_node;
      delete right_node;

      node.reset(merged_node);
      for (size_t i = 0; i < 2 * kMaxUnsortedRecNum; ++i) {
        VerifyRead(i, i, kExpectSuccess);
      }
    }
  }

  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  // actual keys and payloads
  size_t key_size_{};
  size_t pay_size_{};
  Key keys_[kKeyNumForTest];
  Payload payloads_[kKeyNumForTest];

  // the length of a record and its maximum number
  size_t max_rec_num_{};
  size_t max_del_num_{};

  std::unique_ptr<Node_t> node{nullptr};
};

/*##################################################################################################
 * Preparation for typed testing
 *################################################################################################*/

using KeyPayloadPairs = ::testing::Types<  //
    KeyPayload<uint64_t, uint64_t, UInt64Comp, UInt64Comp>,
    KeyPayload<char *, uint64_t, CStrComp, UInt64Comp>,
    KeyPayload<uint64_t, char *, UInt64Comp, CStrComp>,
    KeyPayload<char *, char *, CStrComp, CStrComp>,
    KeyPayload<uint32_t, uint64_t, UInt32Comp, UInt64Comp>,
    KeyPayload<uint64_t, uint64_t *, UInt64Comp, PtrComp>,
    KeyPayload<uint64_t, MyClass, UInt64Comp, MyClassComp>,
    KeyPayload<uint64_t, int64_t, UInt64Comp, Int64Comp>  //
    >;
TYPED_TEST_CASE(NodeFixture, KeyPayloadPairs);

/*##################################################################################################
 * Unit test definitions
 *################################################################################################*/

/*--------------------------------------------------------------------------------------------------
 * Write operation
 *------------------------------------------------------------------------------------------------*/

TYPED_TEST(NodeFixture, WriteWithUniqueKeysWOConsolidationReadWrittenValues)
{
  const size_t max_num = kMaxUnsortedRecNum;

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
  const size_t first_max_num = kMaxUnsortedRecNum;
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
  const size_t max_num = kMaxUnsortedRecNum / 2;

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
  const size_t max_num = kMaxUnsortedRecNum;

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

/*--------------------------------------------------------------------------------------------------
 * Insert operation
 *------------------------------------------------------------------------------------------------*/

TYPED_TEST(NodeFixture, InsertWithUniqueKeysWOConsolidationReadWrittenValues)
{
  const size_t max_num = kMaxUnsortedRecNum;

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
  const size_t first_max_num = kMaxUnsortedRecNum;
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
  const size_t max_num = kMaxUnsortedRecNum / 2;

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
  const size_t max_num = kMaxUnsortedRecNum;

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

/*--------------------------------------------------------------------------------------------------
 * Update operation
 *------------------------------------------------------------------------------------------------*/

TYPED_TEST(NodeFixture, UpdateWithUniqueKeysWOConsolidationFail)
{
  const size_t max_num = kMaxUnsortedRecNum / 2;

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
  const size_t first_max_num = kMaxUnsortedRecNum;
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
  const size_t max_num = kMaxUnsortedRecNum / 2;

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
  const size_t max_num = kMaxUnsortedRecNum;

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

/*--------------------------------------------------------------------------------------------------
 * Delete operation
 *------------------------------------------------------------------------------------------------*/

TYPED_TEST(NodeFixture, DeleteWithUniqueKeysWOConsolidationFail)
{
  const size_t max_num = kMaxUnsortedRecNum / 2;

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
  const size_t first_max_num = kMaxUnsortedRecNum;
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

/*--------------------------------------------------------------------------------------------------
 * SMO operation
 *------------------------------------------------------------------------------------------------*/

TYPED_TEST(NodeFixture, ConsolidateWithSortedAndUnsortedRecordsCreateConsolidatedNode)
{
  TestFixture::PrepareConsolidatedNode();

  // the node has the both sorted/unsorted records
  for (size_t i = 0; i < 2 * kMaxUnsortedRecNum; ++i) {
    TestFixture::VerifyRead(i, i, kExpectSuccess);
  }
}

TYPED_TEST(NodeFixture, SplitWithLeafNodeDivideWrittenRecordsIntoEachNode)
{
  TestFixture::VerifyLeafSplit();
}

TYPED_TEST(NodeFixture, SplitWithInternalNodeDivideWrittenRecordsIntoEachNode)
{
  TestFixture::VerifyInternalSplit();
}

TYPED_TEST(NodeFixture, MergeWithLeafNodesMergeWrittenRecordsIntoSingleNode)
{
  TestFixture::VerifyLeafMerge();
}

TYPED_TEST(NodeFixture, MergeWithInternalNodesMergeWrittenRecordsIntoSingleNode)
{
  TestFixture::VerifyInternalMerge();
}

}  // namespace dbgroup::index::bztree::component::test
