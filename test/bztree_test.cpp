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

#include "gtest/gtest.h"

using std::byte;

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
class BzTreeFixture : public testing::Test
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
  using BzTree_t = BzTree<Key, Payload, KeyComp>;
  using NodeReturnCode = typename BaseNode_t::NodeReturnCode;
  using KeyExistence = typename BaseNode_t::KeyExistence;

  // constant values for testing
  static constexpr size_t kIndexEpoch = 1;
  static constexpr size_t kKeyNumForTest = 1024;
  static constexpr size_t kKeyLength = kWordLength;
  static constexpr size_t kPayloadLength = kWordLength;

  static constexpr size_t kTestNodeSize = 256;          // a node can have 10 records (16 + 24 * 10)
  static constexpr size_t kTestMinNodeSize = 89;        // a node with 3 records invokes merging
  static constexpr size_t kTestMinFreeSpace = 24;       // keep free space with 1 record size
  static constexpr size_t kTestExpectedFreeSpace = 72;  // expect free space can write 3 records
  static constexpr size_t kTestMaxDeletedSize = 119;    // consolidate when 5 records are deleted
  static constexpr size_t kTestMaxMergedSize = 137;     // a merged node has space for 5 records

  // actual keys and payloads
  size_t key_length;
  size_t payload_length;
  Key keys[kKeyNumForTest];
  Payload payloads[kKeyNumForTest];

  // the length of a record and its maximum number
  size_t record_length;
  size_t max_record_num;

  BzTree_t bztree = BzTree_t{};

  ReturnCode rc;
  std::unique_ptr<Record_t> record;

  /*################################################################################################
   * Setup/Teardown
   *##############################################################################################*/

  void
  SetUp()
  {
    // prepare keys
    if constexpr (std::is_same_v<Key, char *>) {
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

    // prepare payloads
    if constexpr (std::is_same_v<Payload, char *>) {
      // variable-length payloads
      payload_length = 7;
      for (size_t index = 0; index < kKeyNumForTest; ++index) {
        auto payload = new char[kPayloadLength];
        snprintf(payload, kPayloadLength, "%06lu", index);
        payloads[index] = payload;
      }
    } else {
      // static-length payloads
      payload_length = sizeof(Payload);
      for (size_t index = 0; index < kKeyNumForTest; ++index) {
        payloads[index] = index;
      }
    }

    // set a record length and its maximum number
    if constexpr (!std::is_same_v<Payload, char *> && sizeof(Payload) == kWordLength) {
      record_length = 2 * kWordLength;
    } else {
      record_length = key_length + payload_length;
    }
    max_record_num = (kPageSize - kHeaderLength) / (record_length + kWordLength);
  }

  void
  TearDown()
  {
    if constexpr (std::is_same_v<Key, char *>) {
      for (size_t index = 0; index < kKeyNumForTest; ++index) {
        delete[] keys[index];
      }
    }
    if constexpr (std::is_same_v<Payload, char *>) {
      for (size_t index = 0; index < kKeyNumForTest; ++index) {
        delete[] payloads[index];
      }
    }
  }

  /*################################################################################################
   * Utility functions
   *##############################################################################################*/

  void
  WriteOrderedKeys(  //
      BzTree_t *bztree,
      const size_t begin_index,
      const size_t end_index)
  {
    assert(end_index < kKeyNumForTest);

    for (size_t index = begin_index; index <= end_index; ++index) {
      bztree->Write(keys[index], kKeyLength, payloads[index], kPayloadLength);
    }
  }

  void
  InsertOrderedKeys(  //
      BzTree_t *bztree,
      const size_t begin_index,
      const size_t end_index)
  {
    assert(end_index < kKeyNumForTest);

    for (size_t index = begin_index; index <= end_index; ++index) {
      bztree->Insert(keys[index], kKeyLength, payloads[index], kPayloadLength);
    }
  }

  void
  UpdateOrderedKeys(  //
      BzTree_t *bztree,
      const size_t begin_index,
      const size_t end_index)
  {
    assert(end_index + 1 < kKeyNumForTest);

    for (size_t index = begin_index; index <= end_index; ++index) {
      bztree->Update(keys[index], kKeyLength, payloads[index + 1], kPayloadLength);
    }
  }

  void
  DeleteOrderedKeys(  //
      BzTree_t *bztree,
      const size_t begin_index,
      const size_t end_index)
  {
    assert(end_index < kKeyNumForTest);

    for (size_t index = begin_index; index <= end_index; ++index) {
      bztree->Delete(keys[index], kKeyLength);
    }
  }

  /*################################################################################################
   * Functions for verification
   *##############################################################################################*/

  void
  VerifyKey(  //
      const Key expected,
      const Key actual)
  {
    EXPECT_TRUE(IsEqual<KeyComp>(expected, actual));
  }

  void
  VerifyPayload(  //
      const Payload expected,
      const Payload actual)
  {
    EXPECT_TRUE(IsEqual<PayloadComp>(expected, actual));
  }

  void
  VerifyRead(  //
      const size_t key_id,
      const size_t expected_id,
      const bool expect_fail = false)
  {
    auto [rc, actual] = bztree.Read(keys[key_id]);

    if (expect_fail) {
      EXPECT_EQ(ReturnCode::kKeyNotExist, rc);
    } else {
      EXPECT_EQ(ReturnCode::kSuccess, rc);
      if constexpr (std::is_same_v<Payload, char *>) {
        EXPECT_TRUE(IsEqual<PayloadComp>(payloads[expected_id], actual.get()));
      } else {
        EXPECT_TRUE(IsEqual<PayloadComp>(payloads[expected_id], actual));
      }
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
                                         KeyPayloadPair<char *, int64_t, CStrComp, Int64Comp>,
                                         KeyPayloadPair<int64_t, char *, Int64Comp, CStrComp>,
                                         KeyPayloadPair<int32_t, int32_t, Int32Comp, Int32Comp>,
                                         KeyPayloadPair<char *, int32_t, CStrComp, Int32Comp>,
                                         KeyPayloadPair<int32_t, char *, Int32Comp, CStrComp>,
                                         KeyPayloadPair<char *, char *, CStrComp, CStrComp>>;
TYPED_TEST_CASE(BzTreeFixture, KeyPayloadPairs);

/*##################################################################################################
 * Unit test definitions
 *################################################################################################*/

/*--------------------------------------------------------------------------------------------------
 * Read operation
 *------------------------------------------------------------------------------------------------*/

TYPED_TEST(BzTreeFixture, Read_NotPresentKey_ReadFail)
{  //
  TestFixture::VerifyRead(0, 0, true);
}

// /*--------------------------------------------------------------------------------------------------
//  * Scan operation
//  *------------------------------------------------------------------------------------------------*/

// TYPED_TEST(BzTreeFixture, Scan_EmptyNode_NoResult)
// {
//   auto [rc, scan_results] = bztree.Scan(keys[1], true, keys[10], true);

//   EXPECT_EQ(ReturnCode::kSuccess, rc);
//   EXPECT_EQ(0, scan_results.size());
// }

// TYPED_TEST(BzTreeFixture, Scan_BothClosed_ScanTargetValues)
// {
//   WriteOrderedKeys(&bztree, 1, 10);

//   auto [rc, scan_results] = bztree.Scan(keys[4], true, keys[6], true);

//   EXPECT_EQ(ReturnCode::kSuccess, rc);
//   EXPECT_EQ(3, scan_results.size());
//   VerifyKey(keys[4], scan_results[0]->GetKey());
//   VerifyPayload(payloads[4], scan_results[0]->GetPayload());
//   VerifyKey(keys[5], scan_results[1]->GetKey());
//   VerifyPayload(payloads[5], scan_results[1]->GetPayload());
//   VerifyKey(keys[6], scan_results[2]->GetKey());
//   VerifyPayload(payloads[6], scan_results[2]->GetPayload());
// }

// TYPED_TEST(BzTreeFixture, Scan_LeftClosed_ScanTargetValues)
// {
//   WriteOrderedKeys(&bztree, 1, 10);

//   auto [rc, scan_results] = bztree.Scan(keys[8], true, keys[10], false);

//   EXPECT_EQ(ReturnCode::kSuccess, rc);
//   EXPECT_EQ(2, scan_results.size());
//   VerifyKey(keys[8], scan_results[0]->GetKey());
//   VerifyPayload(payloads[8], scan_results[0]->GetPayload());
//   VerifyKey(keys[9], scan_results[1]->GetKey());
//   VerifyPayload(payloads[9], scan_results[1]->GetPayload());
// }

// TYPED_TEST(BzTreeFixture, Scan_RightClosed_ScanTargetValues)
// {
//   WriteOrderedKeys(&bztree, 1, 10);

//   auto [rc, scan_results] = bztree.Scan(keys[8], false, keys[10], true);

//   EXPECT_EQ(ReturnCode::kSuccess, rc);
//   EXPECT_EQ(2, scan_results.size());
//   VerifyKey(keys[9], scan_results[0]->GetKey());
//   VerifyPayload(payloads[9], scan_results[0]->GetPayload());
//   VerifyKey(keys[10], scan_results[1]->GetKey());
//   VerifyPayload(payloads[10], scan_results[1]->GetPayload());
// }

// TYPED_TEST(BzTreeFixture, Scan_BothOpened_ScanTargetValues)
// {
//   WriteOrderedKeys(&bztree, 1, 10);

//   auto [rc, scan_results] = bztree.Scan(keys[8], false, keys[10], false);

//   EXPECT_EQ(ReturnCode::kSuccess, rc);
//   EXPECT_EQ(1, scan_results.size());
//   VerifyKey(keys[9], scan_results[0]->GetKey());
//   VerifyPayload(payloads[9], scan_results[0]->GetPayload());
// }

// TYPED_TEST(BzTreeFixture, ScanLess_EndClosed_ScanTargetValues)
// {
//   WriteOrderedKeys(&bztree, 1, 10);

//   auto [rc, scan_results] = bztree.ScanLess(keys[2], true);

//   EXPECT_EQ(ReturnCode::kSuccess, rc);
//   EXPECT_EQ(2, scan_results.size());
//   VerifyKey(keys[1], scan_results[0]->GetKey());
//   VerifyPayload(payloads[1], scan_results[0]->GetPayload());
//   VerifyKey(keys[2], scan_results[1]->GetKey());
//   VerifyPayload(payloads[2], scan_results[1]->GetPayload());
// }

// TYPED_TEST(BzTreeFixture, ScanGreater_BeginClosed_ScanTargetValues)
// {
//   WriteOrderedKeys(&bztree, 1, 10);

//   auto [rc, scan_results] = bztree.ScanGreater(keys[9], true);

//   EXPECT_EQ(ReturnCode::kSuccess, rc);
//   EXPECT_EQ(2, scan_results.size());
//   VerifyKey(keys[9], scan_results[0]->GetKey());
//   VerifyPayload(payloads[9], scan_results[0]->GetPayload());
//   VerifyKey(keys[10], scan_results[1]->GetKey());
//   VerifyPayload(payloads[10], scan_results[1]->GetPayload());
// }

// TYPED_TEST(BzTreeFixture, ScanLess_LeftOutsideRange_NoResults)
// {
//   WriteOrderedKeys(&bztree, 5, 10);

//   auto [rc, scan_results] = bztree.ScanLess(keys[3], false);

//   EXPECT_EQ(ReturnCode::kSuccess, rc);
//   EXPECT_EQ(0, scan_results.size());
// }

// TYPED_TEST(BzTreeFixture, ScanGreater_RightOutsideRange_NoResults)
// {
//   WriteOrderedKeys(&bztree, 1, 4);

//   auto [rc, scan_results] = bztree.ScanGreater(keys[5], false);

//   EXPECT_EQ(ReturnCode::kSuccess, rc);
//   EXPECT_EQ(0, scan_results.size());
// }

// TYPED_TEST(BzTreeFixture, Scan_WithUpdateDelete_ScanLatestValues)
// {
//   WriteOrderedKeys(&bztree, 1, 5);
//   bztree.Update(keys[2], kKeyLength, payloads[0], kPayloadLength);
//   bztree.Delete(keys[3], kKeyLength);

//   auto [rc, scan_results] = bztree.Scan(keys[2], true, keys[4], true);

//   EXPECT_EQ(ReturnCode::kSuccess, rc);
//   EXPECT_EQ(2, scan_results.size());
//   VerifyKey(keys[2], scan_results[0]->GetKey());
//   VerifyPayload(payloads[0], scan_results[0]->GetPayload());
//   VerifyKey(keys[4], scan_results[1]->GetKey());
//   VerifyPayload(payloads[4], scan_results[1]->GetPayload());
// }

// /*--------------------------------------------------------------------------------------------------
//  * Write operation
//  *------------------------------------------------------------------------------------------------*/

// TYPED_TEST(BzTreeFixture, Write_TwoKeys_ReadWrittenValues)
// {
//   bztree.Write(keys[1], kKeyLength, payloads[1], kPayloadLength);
//   bztree.Write(keys[2], kKeyLength, payloads[2], kPayloadLength);

//   VerifyRead(keys[1], payloads[1]);
//   VerifyRead(keys[2], payloads[2]);
// }

// TYPED_TEST(BzTreeFixture, Write_DuplicateKey_ReadLatestValue)
// {
//   bztree.Write(keys[1], kKeyLength, payloads[1], kPayloadLength);
//   bztree.Write(keys[1], kKeyLength, payloads[2], kPayloadLength);

//   VerifyRead(keys[1], payloads[2]);
// }

// /*--------------------------------------------------------------------------------------------------
//  * Insert operation
//  *------------------------------------------------------------------------------------------------*/

// TYPED_TEST(BzTreeFixture, Insert_TwoKeys_ReadInsertedValues)
// {
//   bztree.Insert(keys[1], kKeyLength, payloads[1], kPayloadLength);
//   bztree.Insert(keys[2], kKeyLength, payloads[2], kPayloadLength);

//   VerifyRead(keys[1], payloads[1]);
//   VerifyRead(keys[2], payloads[2]);
// }

// TYPED_TEST(BzTreeFixture, Insert_DuplicateKey_InsertionFailed)
// {
//   bztree.Insert(keys[1], kKeyLength, payloads[1], kPayloadLength);

//   rc = bztree.Insert(keys[1], kKeyLength, payloads[1], kPayloadLength);

//   EXPECT_EQ(ReturnCode::kKeyExist, rc);
// }

// /*--------------------------------------------------------------------------------------------------
//  * Update operation
//  *------------------------------------------------------------------------------------------------*/

// TYPED_TEST(BzTreeFixture, Update_SingleKey_ReadUpdatedValue)
// {
//   bztree.Insert(keys[1], kKeyLength, payloads[2], kPayloadLength);
//   bztree.Update(keys[1], kKeyLength, payloads[2], kPayloadLength);

//   VerifyRead(keys[1], payloads[2]);
// }

// TYPED_TEST(BzTreeFixture, Update_NotPresentKey_UpdatedFailed)
// {
//   rc = bztree.Update(keys[1], kKeyLength, payloads[1], kPayloadLength);

//   EXPECT_EQ(ReturnCode::kKeyNotExist, rc);
// }

// TYPED_TEST(BzTreeFixture, Update_DeletedKey_UpdateFailed)
// {
//   bztree.Insert(keys[1], kKeyLength, payloads[2], kPayloadLength);
//   bztree.Delete(keys[1], kKeyLength);

//   rc = bztree.Update(keys[1], kKeyLength, payloads[2], kPayloadLength);

//   EXPECT_EQ(ReturnCode::kKeyNotExist, rc);
// }

// /*--------------------------------------------------------------------------------------------------
//  * Delete operation
//  *------------------------------------------------------------------------------------------------*/

// TYPED_TEST(BzTreeFixture, Delete_PresentKey_DeletionSucceed)
// {
//   bztree.Insert(keys[1], kKeyLength, payloads[1], kPayloadLength);

//   rc = bztree.Delete(keys[1], kKeyLength);

//   EXPECT_EQ(ReturnCode::kSuccess, rc);
// }

// TYPED_TEST(BzTreeFixture, Delete_PresentKey_ReadFailed)
// {
//   bztree.Insert(keys[1], kKeyLength, payloads[1], kPayloadLength);
//   bztree.Delete(keys[1], kKeyLength);

//   auto [rc, payload] = bztree.Read(keys[1]);

//   EXPECT_EQ(ReturnCode::kKeyNotExist, rc);
// }

// TYPED_TEST(BzTreeFixture, Delete_NotPresentKey_DeletionFailed)
// {
//   rc = bztree.Delete(keys[1], kKeyLength);

//   EXPECT_EQ(ReturnCode::kKeyNotExist, rc);
// }

// TYPED_TEST(BzTreeFixture, Delete_DeletedKey_DeletionFailed)
// {
//   bztree.Insert(keys[1], kKeyLength, payloads[1], kPayloadLength);
//   bztree.Delete(keys[1], kKeyLength);

//   rc = bztree.Delete(keys[1], kKeyLength);

//   EXPECT_EQ(ReturnCode::kKeyNotExist, rc);
// }

// /*--------------------------------------------------------------------------------------------------
//  * Split operation
//  *------------------------------------------------------------------------------------------------*/

// TYPED_TEST(BzTreeFixture, Split_OrderedKeyWrites_ReadWrittenKeys)
// {
//   const auto record_count = 1000;

//   WriteOrderedKeys(&bztree, 1, record_count);
//   // std::thread{&BzTreeFixture::WriteOrderedKeys, this, &bztree, 1, record_count}.join();

//   for (size_t index = 1; index <= record_count; ++index) {
//     VerifyRead(keys[index], payloads[index]);
//   }

//   auto [rc, scan_results] = bztree.Scan(keys[50], true, keys[100], true);
//   EXPECT_EQ(ReturnCode::kSuccess, rc);
//   auto index = 50;
//   for (auto&& record : scan_results) {
//     VerifyKey(keys[index], record->GetKey());
//     VerifyPayload(payloads[index++], record->GetPayload());
//   }
// }

// TYPED_TEST(BzTreeFixture, Split_OrderedKeyInserts_ReadInsertedKeys)
// {
//   const auto record_count = 1000;

//   InsertOrderedKeys(&bztree, 1, record_count);
//   // std::thread{&BzTreeFixture::InsertOrderedKeys, this, &bztree, 1, record_count}.join();

//   for (size_t index = 1; index <= record_count; ++index) {
//     VerifyRead(keys[index], payloads[index]);
//   }

//   auto [rc, scan_results] = bztree.Scan(keys[50], true, keys[100], true);
//   EXPECT_EQ(ReturnCode::kSuccess, rc);
//   auto index = 50UL;
//   for (auto&& record : scan_results) {
//     VerifyKey(keys[index], record->GetKey());
//     VerifyPayload(payloads[index++], record->GetPayload());
//   }
// }

// TYPED_TEST(BzTreeFixture, Split_OrderedKeyInsertsUpdates_ReadLatestKeys)
// {
//   const auto record_count = 1000;

//   InsertOrderedKeys(&bztree, 1, record_count);
//   UpdateOrderedKeys(&bztree, 1, record_count);
//   // std::thread{&BzTreeFixture::InsertOrderedKeys, this, &bztree, 1, record_count}.join();
//   // std::thread{&BzTreeFixture::UpdateOrderedKeys, this, &bztree, 1, record_count}.join();

//   for (size_t index = 1; index <= record_count; ++index) {
//     VerifyRead(keys[index], payloads[index + 1]);
//   }

//   auto [rc, scan_results] = bztree.Scan(keys[50], true, keys[100], true);
//   EXPECT_EQ(ReturnCode::kSuccess, rc);
//   auto index = 50UL;
//   for (auto&& record : scan_results) {
//     VerifyKey(keys[index], record->GetKey());
//     VerifyPayload(payloads[++index], record->GetPayload());
//   }
// }

// /*--------------------------------------------------------------------------------------------------
//  * Merge operation
//  *------------------------------------------------------------------------------------------------*/

// TYPED_TEST(BzTreeFixture, Merge_OrderedKeyWritesDeletes_ReadRemainingKey)
// {
//   const auto record_count = 1000;

//   std::thread{&BzTreeFixture::WriteOrderedKeys, this, &bztree, 1, record_count}.join();
//   std::thread{&BzTreeFixture::DeleteOrderedKeys, this, &bztree, 2, record_count}.join();

//   VerifyRead(keys[1], payloads[1]);

//   for (size_t index = 2; index <= record_count; ++index) {
//     auto [rc, payload] = bztree.Read(keys[index]);
//     EXPECT_EQ(ReturnCode::kKeyNotExist, rc);
//   }
// }

}  // namespace dbgroup::index::bztree
