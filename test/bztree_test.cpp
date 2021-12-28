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

#include "bztree/bztree.hpp"

#include <functional>
#include <memory>
#include <thread>

#include "common.hpp"
#include "gtest/gtest.h"

namespace dbgroup::index::bztree::test
{
using component::AlignRecord;

/*######################################################################################
 * Global constants
 *####################################################################################*/

constexpr size_t kGCTime = 1000;
constexpr size_t kTestMaxRecNum = (kPageSize - kHeaderLength - kMinFreeSpaceSize) / 20;
constexpr size_t kKeyNumForTest = 2 * kTestMaxRecNum * kTestMaxRecNum + 2;
constexpr bool kExpectSuccess = true;
constexpr bool kExpectFailed = false;
constexpr bool kRangeClosed = true;
constexpr bool kRangeOpened = false;

// use a supper template to define key-payload pair templates
template <class KeyType, class PayloadType>
struct KeyPayload {
  using Key = KeyType;
  using Payload = PayloadType;
};

template <class KeyPayload>
class BzTreeFixture : public testing::Test  // NOLINT
{
  // extract key-payload types
  using Key = typename KeyPayload::Key::Data;
  using Payload = typename KeyPayload::Payload::Data;
  using KeyComp = typename KeyPayload::Key::Comp;
  using PayloadComp = typename KeyPayload::Payload::Comp;

  // define type aliases for simplicity
  using Metadata = component::Metadata;
  using Node_t = component::Node<Key, KeyComp>;
  using BzTree_t = BzTree<Key, Payload, KeyComp>;
  using LoadEntry_t = BulkloadEntry<Key, Payload>;

 protected:
  /*####################################################################################
   * Setup/Teardown
   *##################################################################################*/

  void
  SetUp() override
  {
    // prepare keys
    key_size_ = (IsVariableLengthData<Key>()) ? kVarDataLength : sizeof(Key);
    PrepareTestData(keys_, kKeyNumForTest, key_size_);

    // prepare payloads
    pay_size_ = (IsVariableLengthData<Payload>()) ? kVarDataLength : sizeof(Payload);
    PrepareTestData(payloads_, kKeyNumForTest, pay_size_);

    // set a record length and its maximum number
    auto rec_size = std::get<2>(AlignRecord<Key, Payload>(key_size_, pay_size_)) + sizeof(Metadata);
    max_leaf_rec_num_ = (kPageSize - kHeaderLength - kMinFreeSpaceSize) / rec_size;
    max_internal_rec_num_ = (kPageSize - kHeaderLength) / rec_size;
    rec_num_for_testing_ = 1.5 * max_leaf_rec_num_ * max_internal_rec_num_;  // NOLINT

    bztree_ = std::make_unique<BzTree_t>(kGCTime);
  }

  void
  TearDown() override
  {
    ReleaseTestData(keys_, kKeyNumForTest);
    ReleaseTestData(payloads_, kKeyNumForTest);
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
    const auto read_val = bztree_->Read(keys_[key_id]);
    if (expect_success) {
      EXPECT_TRUE(read_val);

      const auto expected_val = payloads_[expected_id];
      const auto actual_val = read_val.value();
      EXPECT_TRUE(component::IsEqual<PayloadComp>(expected_val, actual_val));
      if constexpr (IsVariableLengthData<Payload>()) {
        delete actual_val;
      }
    } else {
      EXPECT_FALSE(read_val);
    }
  }

  void
  VerifyScan(  //
      const std::optional<std::pair<size_t, bool>> begin_ref,
      const std::optional<std::pair<size_t, bool>> end_ref)
  {
    std::optional<std::pair<const Key &, bool>> begin_key = std::nullopt;
    size_t begin_pos = 0;
    if (begin_ref) {
      auto &&[begin_id, begin_closed] = *begin_ref;
      begin_key.emplace(keys_[begin_id], begin_closed);
      begin_pos = (begin_closed) ? begin_id : begin_id + 1;
    }

    std::optional<std::pair<const Key &, bool>> end_key = std::nullopt;
    size_t end_pos = 0;
    if (end_ref) {
      auto &&[end_id, end_closed] = *end_ref;
      end_key.emplace(keys_[end_id], end_closed);
      end_pos = (end_closed) ? end_id : end_id - 1;
    }

    auto iter = bztree_->Scan(begin_key, end_key);

    for (; iter.HasNext(); ++iter, ++begin_pos) {
      auto [key, payload] = *iter;
      EXPECT_TRUE(component::IsEqual<KeyComp>(keys_[begin_pos], key));
      EXPECT_TRUE(component::IsEqual<PayloadComp>(payloads_[begin_pos], payload));
    }
    EXPECT_FALSE(iter.HasNext());

    if (end_ref) {
      EXPECT_EQ(begin_pos, end_pos);
    }
  }

  void
  VerifyWrite(  //
      const size_t key_id,
      const size_t payload_id)
  {
    auto rc = bztree_->Write(keys_[key_id], payloads_[payload_id], key_size_, pay_size_);

    EXPECT_EQ(ReturnCode::kSuccess, rc);
  }

  void
  VerifyInsert(  //
      const size_t key_id,
      const size_t payload_id,
      const bool expect_success)
  {
    ReturnCode expected_rc = (expect_success) ? kSuccess : kKeyExist;

    auto rc = bztree_->Insert(keys_[key_id], payloads_[payload_id], key_size_, pay_size_);
    EXPECT_EQ(expected_rc, rc);
  }

  void
  VerifyUpdate(  //
      const size_t key_id,
      const size_t payload_id,
      const bool expect_success)
  {
    ReturnCode expected_rc = (expect_success) ? kSuccess : kKeyNotExist;

    auto rc = bztree_->Update(keys_[key_id], payloads_[payload_id], key_size_, pay_size_);
    EXPECT_EQ(expected_rc, rc);
  }

  void
  VerifyDelete(  //
      const size_t key_id,
      const bool expect_success)
  {
    ReturnCode expected_rc = (expect_success) ? kSuccess : kKeyNotExist;

    auto rc = bztree_->Delete(keys_[key_id], key_size_);
    EXPECT_EQ(expected_rc, rc);
  }

  void
  VerifyBulkload(  //
      const size_t begin_key_id,
      const size_t end_key_id,
      const size_t thread_num)
  {
    // prepare bulkload entries
    std::vector<LoadEntry_t> entries{};
    entries.reserve(end_key_id - begin_key_id);
    for (size_t i = begin_key_id; i < end_key_id; ++i) {
      entries.emplace_back(keys_[i], payloads_[i], key_size_, pay_size_);
    }

    // bulkload the entries
    auto rc = bztree_->Bulkload(entries, thread_num);
    EXPECT_EQ(rc, kSuccess);
  }

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  // actual keys and payloads
  size_t key_size_{};
  size_t pay_size_{};
  Key keys_[kKeyNumForTest];
  Payload payloads_[kKeyNumForTest];

  // a test target BzTree
  std::unique_ptr<BzTree_t> bztree_{nullptr};

  size_t max_leaf_rec_num_{};
  size_t max_internal_rec_num_{};
  size_t rec_num_for_testing_{};
};

/*######################################################################################
 * Preparation for typed testing
 *####################################################################################*/

using KeyPayloadPairs = ::testing::Types<  //
    KeyPayload<UInt8, UInt8>,              // fixed and same alignment
    KeyPayload<Var, UInt8>,                // variable-fixed
    KeyPayload<UInt8, Var>,                // fixed-variable
    KeyPayload<Var, Var>,                  // variable-variable
    KeyPayload<UInt4, UInt8>,              // fixed but different alignment (key < payload)
    KeyPayload<UInt8, UInt4>,              // fixed but different alignment (key > payload)
    KeyPayload<Ptr, Ptr>,                  // pointer key/payload
    KeyPayload<UInt8, Original>,           // original class payload
    KeyPayload<UInt8, Int8>                // payload that cannot use CAS
    >;
TYPED_TEST_SUITE(BzTreeFixture, KeyPayloadPairs);

/*######################################################################################
 * Unit test definitions
 *####################################################################################*/

/*--------------------------------------------------------------------------------------
 * Structure modification operations
 *------------------------------------------------------------------------------------*/

TYPED_TEST(BzTreeFixture, WriteWithoutSMOsReadWrittenValues)
{
  const size_t rec_num = kMaxUnsortedRecNum;

  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyWrite(i, i);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyRead(i, i, kExpectSuccess);
  }
}

TYPED_TEST(BzTreeFixture, WriteWithConsolidationReadWrittenValues)
{
  const size_t rec_num = TestFixture::max_leaf_rec_num_;

  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyWrite(i, i);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyRead(i, i, kExpectSuccess);
  }
}

TYPED_TEST(BzTreeFixture, WriteWithRootLeafSplitReadWrittenValues)
{
  const size_t rec_num = TestFixture::max_leaf_rec_num_ * 1.5;

  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyWrite(i, i);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyRead(i, i, kExpectSuccess);
  }
}

TYPED_TEST(BzTreeFixture, WriteWithRootInternalSplitReadWrittenValues)
{
  const size_t rec_num = TestFixture::max_leaf_rec_num_ * TestFixture::max_internal_rec_num_;

  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyWrite(i, i);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyRead(i, i, kExpectSuccess);
  }
}

TYPED_TEST(BzTreeFixture, WriteWithInternalSplitReadWrittenValues)
{
  const size_t rec_num = TestFixture::rec_num_for_testing_;

  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyWrite(i, i);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyRead(i, i, kExpectSuccess);
  }
}

TYPED_TEST(BzTreeFixture, WriteWithHalfDeletingReadRemainingValues)
{
  const size_t rec_num = TestFixture::rec_num_for_testing_;

  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyWrite(i, i);
  }
  for (size_t i = 0; i < rec_num; i += 2) {
    TestFixture::VerifyDelete(i, kExpectSuccess);
  }
  for (size_t i = 0; i < rec_num; i += 2) {
    TestFixture::VerifyRead(i, i, kExpectFailed);
  }
  for (size_t i = 1; i < rec_num; i += 2) {
    TestFixture::VerifyRead(i, i, kExpectSuccess);
  }
}

TYPED_TEST(BzTreeFixture, WriteWithAllDeletingReadFail)
{
  const size_t rec_num = TestFixture::rec_num_for_testing_;

  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyWrite(i, i);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyDelete(i, kExpectSuccess);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyRead(i, i, kExpectFailed);
  }
}

/*--------------------------------------------------------------------------------------
 * Read operation
 *------------------------------------------------------------------------------------*/

TYPED_TEST(BzTreeFixture, ReadWithNotPresentKeyFail)
{  //
  TestFixture::VerifyRead(0, 0, kExpectFailed);
}

/*--------------------------------------------------------------------------------------
 * Scan operation
 *------------------------------------------------------------------------------------*/

TYPED_TEST(BzTreeFixture, ScanWithoutKeysPerformFullScan)
{
  const size_t rec_num = TestFixture::rec_num_for_testing_;

  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyWrite(i, i);
  }

  TestFixture::VerifyScan(std::nullopt, std::nullopt);
}

TYPED_TEST(BzTreeFixture, ScanWithClosedRangeIncludeLeftRightEnd)
{
  const size_t rec_num = TestFixture::rec_num_for_testing_;

  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyWrite(i, i);
  }

  TestFixture::VerifyScan(std::make_pair(0, kRangeClosed),
                          std::make_pair(rec_num - 1, kRangeClosed));
}

TYPED_TEST(BzTreeFixture, ScanWithOpenedRangeExcludeLeftRightEnd)
{
  const size_t rec_num = TestFixture::rec_num_for_testing_;

  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyWrite(i, i);
  }

  TestFixture::VerifyScan(std::make_pair(0, kRangeOpened),
                          std::make_pair(rec_num - 1, kRangeOpened));
}

/*--------------------------------------------------------------------------------------
 * Write operation
 *------------------------------------------------------------------------------------*/

TYPED_TEST(BzTreeFixture, WriteWithDuplicateKeysReadLatestValues)
{
  const size_t rec_num = TestFixture::rec_num_for_testing_;

  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyWrite(i, i);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyWrite(i, i + 1);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyRead(i, i + 1, kExpectSuccess);
  }
}

/*--------------------------------------------------------------------------------------
 * Insert operation
 *------------------------------------------------------------------------------------*/

TYPED_TEST(BzTreeFixture, InsertWithUniqueKeysReadInsertedValues)
{
  const size_t rec_num = TestFixture::rec_num_for_testing_;

  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyInsert(i, i, kExpectSuccess);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyRead(i, i, kExpectSuccess);
  }
}

TYPED_TEST(BzTreeFixture, InsertWithDuplicateKeysFail)
{
  const size_t rec_num = TestFixture::rec_num_for_testing_;

  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyInsert(i, i, kExpectSuccess);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyInsert(i, i + 1, kExpectFailed);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyRead(i, i, kExpectSuccess);
  }
}

TYPED_TEST(BzTreeFixture, InsertWithDeletedKeysSucceed)
{
  const size_t rec_num = TestFixture::rec_num_for_testing_;

  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyInsert(i, i, kExpectSuccess);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyDelete(i, kExpectSuccess);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyInsert(i, i + 1, kExpectSuccess);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyRead(i, i + 1, kExpectSuccess);
  }
}

/*--------------------------------------------------------------------------------------
 * Update operation
 *------------------------------------------------------------------------------------*/

TYPED_TEST(BzTreeFixture, UpdateWithDuplicateKeysReadUpdatedValues)
{
  const size_t rec_num = TestFixture::rec_num_for_testing_;

  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyWrite(i, i);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyUpdate(i, i + 1, kExpectSuccess);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyRead(i, i + 1, kExpectSuccess);
  }
}

TYPED_TEST(BzTreeFixture, UpdateNotInsertedKeysFail)
{
  const size_t rec_num = TestFixture::rec_num_for_testing_;

  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyUpdate(i, i, kExpectFailed);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyRead(i, i, kExpectFailed);
  }
}

TYPED_TEST(BzTreeFixture, UpdateWithDeletedKeysFail)
{
  const size_t rec_num = TestFixture::rec_num_for_testing_;

  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyWrite(i, i);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyDelete(i, kExpectSuccess);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyUpdate(i, i, kExpectFailed);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyRead(i, i, kExpectFailed);
  }
}

/*--------------------------------------------------------------------------------------
 * Delete operation
 *------------------------------------------------------------------------------------*/

TYPED_TEST(BzTreeFixture, DeleteWithDuplicateKeysSucceed)
{
  const size_t rec_num = TestFixture::rec_num_for_testing_;

  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyWrite(i, i);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyDelete(i, kExpectSuccess);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyRead(i, i, kExpectFailed);
  }
}

TYPED_TEST(BzTreeFixture, DeleteWithNotInsertedKeysFail)
{
  const size_t rec_num = TestFixture::rec_num_for_testing_;

  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyDelete(i, kExpectFailed);
  }
}

TYPED_TEST(BzTreeFixture, DeleteWithDeletedKeysFail)
{
  const size_t rec_num = TestFixture::rec_num_for_testing_;

  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyWrite(i, i);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyDelete(i, kExpectSuccess);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyDelete(i, kExpectFailed);
  }
}

/*--------------------------------------------------------------------------------------
 * Bulkload operation
 *------------------------------------------------------------------------------------*/

TYPED_TEST(BzTreeFixture, BulkloadWithoutSMOsReadWrittenValues)
{
  const size_t rec_num = kMaxUnsortedRecNum;
  const size_t thread_num = 1;

  TestFixture::VerifyBulkload(0, rec_num, thread_num);

  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyRead(i, i, kExpectSuccess);
  }
}

TYPED_TEST(BzTreeFixture, BulkloadWithConsolidationReadWrittenValues)
{
  const size_t rec_num = TestFixture::max_leaf_rec_num_;
  const size_t thread_num = 1;

  TestFixture::VerifyBulkload(0, rec_num, thread_num);

  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyRead(i, i, kExpectSuccess);
  }
}

TYPED_TEST(BzTreeFixture, BulkloadWithRootLeafSplitReadWrittenValues)
{
  const size_t rec_num = TestFixture::max_leaf_rec_num_ * 1.5;
  const size_t thread_num = 1;

  TestFixture::VerifyBulkload(0, rec_num, thread_num);

  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyRead(i, i, kExpectSuccess);
  }
}

TYPED_TEST(BzTreeFixture, BulkloadWithRootInternalSplitReadWrittenValues)
{
  const size_t rec_num = TestFixture::max_leaf_rec_num_ * TestFixture::max_internal_rec_num_;
  const size_t thread_num = 1;

  TestFixture::VerifyBulkload(0, rec_num, thread_num);

  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyRead(i, i, kExpectSuccess);
  }
}

TYPED_TEST(BzTreeFixture, BulkloadWithInternalSplitReadWrittenValues)
{
  const size_t rec_num = TestFixture::rec_num_for_testing_;
  const size_t thread_num = 1;

  TestFixture::VerifyBulkload(0, rec_num, thread_num);

  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyRead(i, i, kExpectSuccess);
  }
}

}  // namespace dbgroup::index::bztree::test
