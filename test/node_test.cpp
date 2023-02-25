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

// C++ standard libraries
#include <functional>
#include <memory>

// external sources
#include "external/index-fixtures/common.hpp"
#include "gtest/gtest.h"

/*######################################################################################
 * Overriding for testing
 *####################################################################################*/

namespace dbgroup::atomic::mwcas
{
/**
 * @brief Specialization to enable MwCAS to swap our sample class.
 *
 */
template <>
constexpr auto
CanMwCAS<MyClass>()  //
    -> bool
{
  return true;
}

}  // namespace dbgroup::atomic::mwcas

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
  using PayComp = typename KeyPayload::Payload::Comp;

  // define type aliases for simplicity
  using Node_t = Node<Key, KeyComp>;

 protected:
  /*####################################################################################
   * Internal constants
   *##################################################################################*/

  static constexpr size_t kHeaderLen = sizeof(Node_t);
  static inline const size_t key_len = ::dbgroup::index::test::GetLength(Key{});
  static inline const size_t padded_len = Pad<Payload>(key_len + sizeof(Payload));
  static inline const size_t rec_num_in_node =
      (kPageSize - kHeaderLen) / (padded_len + sizeof(Metadata));

  /*####################################################################################
   * Setup/Teardown
   *##################################################################################*/

  void
  SetUp() override
  {
    if (kPageSize < kMaxRecSize * kMaxDeltaRecNum * 2 + kHeaderLen) GTEST_SKIP();

    auto *node = new (std::calloc(1, kPageSize)) Node_t{kLeafFlag, 0};
    node_ = std::unique_ptr<Node_t>{node};

    keys_ = ::dbgroup::index::test::PrepareTestData<Key>(kKeyNumForTest);
    payloads_ = ::dbgroup::index::test::PrepareTestData<Payload>(kKeyNumForTest);

    // set a record length and its maximum number
    if constexpr (CanCASUpdate<Payload>()) {
      const auto del_size = padded_len + sizeof(Metadata);
      max_del_num_ = kMaxDeletedSpaceSize / del_size;
    } else {
      const auto del_size = padded_len + key_len + 2 * sizeof(Metadata);
      max_del_num_ = kMaxDeletedSpaceSize / del_size;
    }
    if (max_del_num_ > kMaxDeltaRecNum) {
      max_del_num_ = kMaxDeltaRecNum;
    }
  }

  void
  TearDown() override
  {
    if (kPageSize >= kMaxRecSize * kMaxDeltaRecNum * 2 + kHeaderLen) {
      ::dbgroup::index::test::ReleaseTestData(keys_);
      ::dbgroup::index::test::ReleaseTestData(payloads_);
    }

    node_ = nullptr;
  }

  /*####################################################################################
   * Operation wrappers
   *##################################################################################*/

  auto
  Write(  //
      const size_t key_id,
      const size_t payload_id)
  {
    return node_->Write(keys_[key_id], key_len, payloads_[payload_id]);
  }

  auto
  Insert(  //
      const size_t key_id,
      const size_t payload_id)
  {
    return node_->Insert(keys_[key_id], key_len, payloads_[payload_id]);
  }

  auto
  Update(  //
      const size_t key_id,
      const size_t payload_id)
  {
    return node_->Update(keys_[key_id], key_len, payloads_[payload_id]);
  }

  auto
  Delete(const size_t key_id)
  {
    return node_->template Delete<Payload>(keys_[key_id], key_len);
  }

  void
  Consolidate()
  {
    auto *consolidated_node = new (std::calloc(1, kPageSize)) Node_t{kLeafFlag, 0};
    consolidated_node->template Consolidate<Payload>(node_.get());
    node_.reset(consolidated_node);
  }

  /*####################################################################################
   * Utility functions
   *##################################################################################*/

  void
  PrepareConsolidatedNode()
  {
    for (size_t i = 0; i < rec_num_in_node; ++i) {
      if (Write(i, i) != NodeRC::kSuccess) {
        Consolidate();
        --i;
      }
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
      EXPECT_TRUE(IsEqual<PayComp>(payloads_[expected_id], payload));
      if constexpr (IsVarLenData<Payload>()) {
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
    const auto r_count = rec_num_in_node / 2;
    const auto l_count = rec_num_in_node - r_count;

    PrepareConsolidatedNode();

    auto *left_node = new (std::calloc(1, kPageSize)) Node_t{kLeafFlag, 0};
    auto *right_node = new (std::calloc(1, kPageSize)) Node_t{kLeafFlag, 0};
    node_->template Split<Payload>(left_node, right_node);

    node_.reset(left_node);
    for (size_t i = 0; i < l_count; ++i) {
      VerifyRead(i, i, kExpectSuccess);
    }

    node_.reset(right_node);
    for (size_t i = l_count; i < rec_num_in_node; ++i) {
      VerifyRead(i, i, kExpectSuccess);
    }
  }

  void
  VerifyMerge()
  {
    PrepareConsolidatedNode();

    auto *left_node = new (std::calloc(1, kPageSize)) Node_t{kLeafFlag, 0};
    auto *right_node = new (std::calloc(1, kPageSize)) Node_t{kLeafFlag, 0};
    node_->template Split<Payload>(left_node, right_node);

    auto *merged_node = new (std::calloc(1, kPageSize)) Node_t{kLeafFlag, 0};
    merged_node->template Merge<Payload>(left_node, right_node);

    node_.reset(merged_node);
    for (size_t i = 0; i < 2 * kMaxDeltaRecNum; ++i) {
      VerifyRead(i, i, kExpectSuccess);
    }

    delete left_node;
    delete right_node;
  }

  void
  VerifyInitAsRoot()
  {
    PrepareConsolidatedNode();

    auto *left_node = new (std::calloc(1, kPageSize)) Node_t{kLeafFlag, 0};
    auto *right_node = new (std::calloc(1, kPageSize)) Node_t{kLeafFlag, 0};
    node_->template Split<Payload>(left_node, right_node);
    auto *root = new (std::calloc(1, kPageSize)) Node_t{!kLeafFlag, 0};
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

    auto *l_node = new (std::calloc(1, kPageSize)) Node_t{kLeafFlag, 0};
    auto *r_node = new (std::calloc(1, kPageSize)) Node_t{kLeafFlag, 0};
    node_->template Split<Payload>(l_node, r_node);
    auto *old_parent = new (std::calloc(1, kPageSize)) Node_t{!kLeafFlag, 0};
    old_parent->InitAsRoot(l_node, r_node);
    auto *r_l_node = new (std::calloc(1, kPageSize)) Node_t{kLeafFlag, 0};
    auto *r_r_node = new (std::calloc(1, kPageSize)) Node_t{kLeafFlag, 0};
    r_node->template Split<Payload>(r_l_node, r_r_node);
    auto *new_parent = new (std::calloc(1, kPageSize)) Node_t{!kLeafFlag, 0};
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

    auto *l_node = new (std::calloc(1, kPageSize)) Node_t{kLeafFlag, 0};
    auto *r_node = new (std::calloc(1, kPageSize)) Node_t{kLeafFlag, 0};
    node_->template Split<Payload>(l_node, r_node);
    auto *old_parent = new (std::calloc(1, kPageSize)) Node_t{!kLeafFlag, 0};
    old_parent->InitAsRoot(l_node, r_node);
    auto *merged_node = new (std::calloc(1, kPageSize)) Node_t{kLeafFlag, 0};
    merged_node->template Merge<Payload>(l_node, r_node);
    auto *new_parent = new (std::calloc(1, kPageSize)) Node_t{!kLeafFlag, 0};
    new_parent->InitAsMergeParent(old_parent, merged_node, 0);

    EXPECT_EQ(merged_node, new_parent->GetChild(0));

    delete l_node;
    delete r_node;
    delete merged_node;
    delete old_parent;
    delete new_parent;
  }

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// actual keys
  std::vector<Key> keys_{};

  /// actual payloads
  std::vector<Payload> payloads_{};

  // the length of a record and its maximum number
  size_t max_del_num_{};

  std::unique_ptr<Node_t> node_{nullptr};
};

/*######################################################################################
 * Preparation for typed testing
 *####################################################################################*/

using UInt8 = ::dbgroup::index::test::UInt8;
using UInt4 = ::dbgroup::index::test::UInt4;
using Int8 = ::dbgroup::index::test::Int8;
using Var = ::dbgroup::index::test::Var;
using Ptr = ::dbgroup::index::test::Ptr;
using Original = ::dbgroup::index::test::Original;

using KeyPayloadPairs = ::testing::Types<  //
    KeyPayload<UInt8, UInt8>,              // fixed-length keys
    KeyPayload<UInt8, Int8>,               // fixed-length keys with append-mode
    KeyPayload<UInt4, UInt8>,              // small keys
    KeyPayload<UInt4, Int8>,               // small keys with append-mode
    KeyPayload<UInt8, UInt4>,              // small payloads with append-mode
    KeyPayload<UInt4, UInt4>,              // small keys/payloads with append-mode
    KeyPayload<Ptr, Ptr>,                  // pointer keys/payloads
    KeyPayload<Original, Original>         // original class keys/payloads
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
  const size_t max_num = TestFixture::max_del_num_ / 2;

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
  const size_t max_num = TestFixture::max_del_num_;

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
