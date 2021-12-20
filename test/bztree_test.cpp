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

/*##################################################################################################
 * Global constants
 *################################################################################################*/

constexpr size_t kGCTime = 1000;
constexpr size_t kTestMaxRecNum = (kPageSize - kHeaderLength - kMinFreeSpaceSize) / 20;
constexpr size_t kKeyNumForTest = 2 * kTestMaxRecNum * kTestMaxRecNum + 2;
constexpr bool kExpectSuccess = true;
constexpr bool kExpectFailed = false;

// use a supper template to define key-payload pair templates
template <class KeyType, class PayloadType, class KeyComparator, class PayloadComparator>
struct KeyPayload {
  using Key = KeyType;
  using Payload = PayloadType;
  using KeyComp = KeyComparator;
  using PayloadComp = PayloadComparator;
};

template <class KeyPayload>
class BzTreeFixture : public testing::Test  // NOLINT
{
  // extract key-payload types
  using Key = typename KeyPayload::Key;
  using Payload = typename KeyPayload::Payload;
  using KeyComp = typename KeyPayload::KeyComp;
  using PayloadComp = typename KeyPayload::PayloadComp;

  // define type aliases for simplicity
  using Metadata = component::Metadata;
  using Node_t = component::Node<Key, KeyComp>;
  using BzTree_t = BzTree<Key, Payload, KeyComp>;

 protected:
  /*################################################################################################
   * Setup/Teardown
   *##############################################################################################*/

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
    max_rec_num_ = (kPageSize - kHeaderLength - kMinFreeSpaceSize) / rec_size;

    bztree_ = std::make_unique<BzTree_t>(kGCTime);
  }

  void
  TearDown() override
  {
    ReleaseTestData(keys_, kKeyNumForTest);
    ReleaseTestData(payloads_, kKeyNumForTest);
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
    ReturnCode expected_rc = (expect_success) ? kSuccess : kKeyNotExist;

    const auto [rc, actual] = bztree_->Read(keys_[key_id]);
    EXPECT_EQ(expected_rc, rc);
    if (expect_success) {
      const auto expected_val = payloads_[expected_id];
      if constexpr (IsVariableLengthData<Payload>()) {
        auto *value = actual.get();
        EXPECT_TRUE(component::IsEqual<PayloadComp>(expected_val, value));
      } else {
        EXPECT_TRUE(component::IsEqual<PayloadComp>(expected_val, actual));
      }
    }
  }

  // void
  // VerifyScan(  //
  //     const size_t begin_key_id,
  //     const bool begin_null,
  //     bool begin_closed,
  //     const size_t end_key_id,
  //     const bool end_null,
  //     const bool end_closed,
  //     const std::vector<size_t> &expected_keys,
  //     const std::vector<size_t> &expected_payloads)
  // {
  //   Key begin_key{};
  //   Key *begin_ptr = nullptr, *end_ptr = nullptr;
  //   if (!begin_null) {
  //     begin_key = keys[begin_key_id];
  //     begin_ptr = &begin_key;
  //   }
  //   if (!end_null) end_ptr = &keys[end_key_id];

  //   size_t count = 0;
  //   auto iter = bztree->Scan(begin_ptr, begin_closed, end_ptr, end_closed);
  //   for (; iter.HasNext(); ++iter, ++count) {
  //     auto [key, payload] = *iter;
  //     EXPECT_TRUE(component::IsEqual<KeyComp>(keys[expected_keys[count]], key));
  //     EXPECT_TRUE(component::IsEqual<PayloadComp>(payloads[expected_payloads[count]], payload));
  //   }

  //   EXPECT_EQ(expected_keys.size(), count);
  // }

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

  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  // actual keys and payloads
  size_t key_size_{};
  size_t pay_size_{};
  Key keys_[kKeyNumForTest];
  Payload payloads_[kKeyNumForTest];

  // a test target BzTree
  std::unique_ptr<BzTree_t> bztree_{nullptr};

  size_t max_rec_num_{};
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
TYPED_TEST_SUITE(BzTreeFixture, KeyPayloadPairs);

/*##################################################################################################
 * Unit test definitions
 *################################################################################################*/

/*--------------------------------------------------------------------------------------------------
 * Structure modification operations
 *------------------------------------------------------------------------------------------------*/

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
  const size_t rec_num = TestFixture::max_rec_num_;

  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyWrite(i, i);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyRead(i, i, kExpectSuccess);
  }
}

TYPED_TEST(BzTreeFixture, WriteWithRootLeafSplitReadWrittenValues)
{
  const size_t rec_num = TestFixture::max_rec_num_ * 1.5;

  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyWrite(i, i);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyRead(i, i, kExpectSuccess);
  }
}

TYPED_TEST(BzTreeFixture, WriteWithRootInternalSplitReadWrittenValues)
{
  const size_t rec_num = TestFixture::max_rec_num_ * TestFixture::max_rec_num_;

  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyWrite(i, i);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyRead(i, i, kExpectSuccess);
  }
}

TYPED_TEST(BzTreeFixture, WriteWithInternalSplitReadWrittenValues)
{
  const size_t rec_num = 2 * TestFixture::max_rec_num_ * TestFixture::max_rec_num_;

  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyWrite(i, i);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyRead(i, i, kExpectSuccess);
  }
}

TYPED_TEST(BzTreeFixture, WriteWithHalfDeletingReadRemainingValues)
{
  const size_t rec_num = 2 * TestFixture::max_rec_num_ * TestFixture::max_rec_num_;

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
  const size_t rec_num = 2 * TestFixture::max_rec_num_ * TestFixture::max_rec_num_;

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

/*--------------------------------------------------------------------------------------------------
 * Read operation
 *------------------------------------------------------------------------------------------------*/

TYPED_TEST(BzTreeFixture, ReadWithNotPresentKeyFail)
{  //
  TestFixture::VerifyRead(0, 0, kExpectFailed);
}

// /*--------------------------------------------------------------------------------------------------
//  * Scan operation
//  *------------------------------------------------------------------------------------------------*/

// TYPED_TEST(BzTreeFixture, Scan_EmptyNode_ScanEmptyPage)
// {  //
//   std::vector<size_t> expected_ids;
//   TestFixture::VerifyScan(0, true, true, 0, true, true, expected_ids, expected_ids);
// }

// TYPED_TEST(BzTreeFixture, Scan_UniqueKeys_ScanInsertedRecords)
// {  //
//   std::vector<size_t> expected_ids;
//   for (size_t i = 0; i < TestFixture::kKeyNumForTest; ++i) {
//     TestFixture::VerifyInsert(i, i);
//     expected_ids.emplace_back(i);
//   }

//   TestFixture::VerifyScan(0, true, true, 0, true, true, expected_ids, expected_ids);
// }

// TYPED_TEST(BzTreeFixture, Scan_DuplicateKeys_ScanUpdatedRecords)
// {  //
//   std::vector<size_t> expected_keys;
//   std::vector<size_t> expected_payloads;
//   for (size_t i = 0; i < TestFixture::kKeyNumForTest - 1; ++i) {
//     TestFixture::VerifyInsert(i, i);
//   }
//   for (size_t i = 0; i < TestFixture::kKeyNumForTest - 1; ++i) {
//     TestFixture::VerifyUpdate(i, i + 1);
//     expected_keys.emplace_back(i);
//     expected_payloads.emplace_back(i + 1);
//   }

//   TestFixture::VerifyScan(0, true, true, 0, true, true, expected_keys, expected_payloads);
// }

// TYPED_TEST(BzTreeFixture, Scan_DeletedKeys_ScanEmptyPage)
// {  //
//   std::vector<size_t> expected_ids;
//   for (size_t i = 0; i < TestFixture::kKeyNumForTest; ++i) {
//     TestFixture::VerifyInsert(i, i);
//   }
//   for (size_t i = 0; i < TestFixture::kKeyNumForTest; ++i) {
//     TestFixture::VerifyDelete(i);
//   }

//   TestFixture::VerifyScan(0, true, true, 0, true, true, expected_ids, expected_ids);
// }

/*--------------------------------------------------------------------------------------------------
 * Write operation
 *------------------------------------------------------------------------------------------------*/

TYPED_TEST(BzTreeFixture, WriteWithDuplicateKeysReadLatestValues)
{
  const size_t rec_num = 2 * TestFixture::max_rec_num_ * TestFixture::max_rec_num_;

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

/*--------------------------------------------------------------------------------------------------
 * Insert operation
 *------------------------------------------------------------------------------------------------*/

TYPED_TEST(BzTreeFixture, InsertWithUniqueKeysReadInsertedValues)
{
  const size_t rec_num = 2 * TestFixture::max_rec_num_ * TestFixture::max_rec_num_;

  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyInsert(i, i, kExpectSuccess);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyRead(i, i, kExpectSuccess);
  }
}

TYPED_TEST(BzTreeFixture, InsertWithDuplicateKeysFail)
{
  const size_t rec_num = 2 * TestFixture::max_rec_num_ * TestFixture::max_rec_num_;

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
  const size_t rec_num = 2 * TestFixture::max_rec_num_ * TestFixture::max_rec_num_;

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

/*--------------------------------------------------------------------------------------------------
 * Update operation
 *------------------------------------------------------------------------------------------------*/

TYPED_TEST(BzTreeFixture, UpdateWithDuplicateKeysReadUpdatedValues)
{
  const size_t rec_num = 2 * TestFixture::max_rec_num_ * TestFixture::max_rec_num_;

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
  const size_t rec_num = 2 * TestFixture::max_rec_num_ * TestFixture::max_rec_num_;

  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyUpdate(i, i, kExpectFailed);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyRead(i, i, kExpectFailed);
  }
}

TYPED_TEST(BzTreeFixture, UpdateWithDeletedKeysFail)
{
  const size_t rec_num = 2 * TestFixture::max_rec_num_ * TestFixture::max_rec_num_;

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

/*--------------------------------------------------------------------------------------------------
 * Delete operation
 *------------------------------------------------------------------------------------------------*/

TYPED_TEST(BzTreeFixture, DeleteWithDuplicateKeysSucceed)
{
  const size_t rec_num = 2 * TestFixture::max_rec_num_ * TestFixture::max_rec_num_;

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
  const size_t rec_num = 2 * TestFixture::max_rec_num_ * TestFixture::max_rec_num_;

  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyDelete(i, kExpectFailed);
  }
}

TYPED_TEST(BzTreeFixture, DeleteWithDeletedKeysFail)
{
  const size_t rec_num = 2 * TestFixture::max_rec_num_ * TestFixture::max_rec_num_;

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

}  // namespace dbgroup::index::bztree::test
