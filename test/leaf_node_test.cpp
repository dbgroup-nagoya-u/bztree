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

#include "bztree/components/leaf_node.hpp"

#include <functional>
#include <memory>
#include <utility>
#include <vector>

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
class LeafNodeFixture : public testing::Test
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
  using NodeReturnCode = typename BaseNode_t::NodeReturnCode;

  // constant values for testing
  static constexpr size_t kIndexEpoch = 1;
  static constexpr size_t kKeyNumForTest = 10000;
  static constexpr size_t kKeyLength = kWordLength;
  static constexpr size_t kPayloadLength = kWordLength;

  // actual keys and payloads
  size_t key_length;
  size_t payload_length;
  Key keys[kKeyNumForTest];
  Payload payloads[kKeyNumForTest];

  // the length of a record and its maximum number
  size_t record_length;
  size_t max_record_num;

  // a leaf node and its statistics
  std::unique_ptr<BaseNode_t> node;
  size_t expected_record_count;
  size_t expected_block_size;
  size_t expected_deleted_size;
  size_t expected_occupied_size;

  /*################################################################################################
   * Setup/Teardown
   *##############################################################################################*/

  void
  SetUp()
  {
    // initialize a leaf node and expected statistics
    node.reset(BaseNode_t::CreateEmptyNode(kLeafFlag));
    expected_record_count = 0;
    expected_block_size = 0;
    expected_deleted_size = 0;
    expected_occupied_size = kHeaderLength;

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
    record_length = key_length + payload_length;
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
   * Operation wrappers
   *##############################################################################################*/

  auto
  Write(  //
      const size_t key_id,
      const size_t payload_id,
      const bool expect_full = false)
  {
    if (!expect_full) {
      expected_record_count += 1;
      expected_block_size += record_length;
      expected_occupied_size += kWordLength + record_length;
    }

    return LeafNode_t::Write(node.get(), keys[key_id], key_length, payloads[payload_id],
                             payload_length);
  }

  auto
  Insert(  //
      const size_t key_id,
      const size_t payload_id,
      const bool expect_exist = false,
      const bool expect_full = false)
  {
    if (!expect_exist && !expect_full) {
      expected_record_count += 1;
      expected_block_size += record_length;
      expected_occupied_size += kWordLength + record_length;
    }

    return LeafNode_t::Insert(node.get(), keys[key_id], key_length, payloads[payload_id],
                              payload_length);
  }

  auto
  Update(  //
      const size_t key_id,
      const size_t payload_id,
      const bool expect_not_exist = false,
      const bool expect_full = false)
  {
    if (!expect_not_exist && !expect_full) {
      expected_record_count += 1;
      expected_block_size += record_length;
      expected_deleted_size += kWordLength + record_length;
      expected_occupied_size += kWordLength + record_length;
    }

    return LeafNode_t::Update(node.get(), keys[key_id], key_length, payloads[payload_id],
                              payload_length);
  }

  auto
  Delete(  //
      const size_t key_id,
      const bool expect_not_exist = false,
      const bool expect_full = false)
  {
    if (!expect_not_exist && !expect_full) {
      expected_record_count += 1;
      expected_block_size += key_length;
      expected_deleted_size += kWordLength + record_length;
      expected_occupied_size += kWordLength + key_length;
    }

    return LeafNode_t::Delete(node.get(), keys[key_id], key_length);
  }

  void
  WriteNullKey(const size_t write_num)
  {
    for (size_t i = 0; i < write_num; ++i) {
      Write(0, 0);
    }
  }

  std::vector<Key>
  WriteOrderedKeys(  //
      const size_t begin_index,
      const size_t end_index)
  {
    assert(end_index < kKeyNumForTest);

    std::vector<Key> written_keys;
    for (size_t index = begin_index; index <= end_index; ++index) {
      Write(index, index);
      written_keys.emplace_back(keys[index]);
    }
    return written_keys;
  }

  void
  PrepareConsolidatedNode(  //
      const size_t begin_index,
      const size_t end_index)
  {
    WriteOrderedKeys(begin_index, end_index);
    auto meta_vec = LeafNode_t::GatherSortedLiveMetadata(node.get());
    node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));
  }

  /*################################################################################################
   * Functions for verification
   *##############################################################################################*/

  void
  VerifyMetadata(  //
      const Metadata meta,
      const bool record_is_visible = true)
  {
    if (record_is_visible) {
      EXPECT_TRUE(meta.IsVisible());
      EXPECT_FALSE(meta.IsDeleted());
      EXPECT_EQ(payload_length, meta.GetPayloadLength());
    } else {
      EXPECT_FALSE(meta.IsVisible());
      EXPECT_TRUE(meta.IsDeleted());
      EXPECT_EQ(0, meta.GetPayloadLength());
    }
    EXPECT_EQ(key_length, meta.GetKeyLength());
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
    auto [rc, actual] = LeafNode_t::Read(node.get(), keys[key_id]);

    if (expect_fail) {
      EXPECT_EQ(NodeReturnCode::kKeyNotExist, rc);
    } else {
      EXPECT_EQ(NodeReturnCode::kSuccess, rc);
      const auto expected = payloads[expected_id];
      if constexpr (std::is_same_v<Payload, char *>) {
        EXPECT_TRUE(IsEqual<PayloadComp>(expected, actual.get()));
      } else {
        EXPECT_TRUE(IsEqual<PayloadComp>(expected, actual));
      }
    }
  }

  void
  VerifyWrite(  //
      const size_t key_id,
      const size_t payload_id,
      const bool expect_full = false)
  {
    auto [rc, status] = Write(key_id, payload_id, expect_full);

    if (expect_full) {
      EXPECT_EQ(NodeReturnCode::kNoSpace, rc);
    } else {
      EXPECT_EQ(NodeReturnCode::kSuccess, rc);
      VerifyMetadata(node->GetMetadata(expected_record_count - 1));
      VerifyStatusWord(status);
    }
  }

  void
  VerifyInsert(  //
      const size_t key_id,
      const size_t payload_id,
      const bool expect_exist = false,
      const bool expect_full = false)
  {
    auto [rc, status] = Insert(key_id, payload_id, expect_exist, expect_full);

    if (expect_full) {
      EXPECT_EQ(NodeReturnCode::kNoSpace, rc);
    } else if (expect_exist) {
      EXPECT_EQ(NodeReturnCode::kKeyExist, rc);
    } else {
      EXPECT_EQ(NodeReturnCode::kSuccess, rc);
      VerifyMetadata(node->GetMetadata(expected_record_count - 1));
      VerifyStatusWord(status);
    }
  }

  void
  VerifyUpdate(  //
      const size_t key_id,
      const size_t payload_id,
      const bool expect_not_exist = false,
      const bool expect_full = false)
  {
    auto [rc, status] = Update(key_id, payload_id, expect_not_exist, expect_full);

    if (expect_full) {
      EXPECT_EQ(NodeReturnCode::kNoSpace, rc);
    } else if (expect_not_exist) {
      EXPECT_EQ(NodeReturnCode::kKeyNotExist, rc);
    } else {
      EXPECT_EQ(NodeReturnCode::kSuccess, rc);
      VerifyMetadata(node->GetMetadata(expected_record_count - 1));
      VerifyStatusWord(status);
    }
  }

  void
  VerifyDelete(  //
      const size_t key_id,
      const bool expect_not_exist = false,
      const bool expect_full = false)
  {
    auto [rc, status] = Delete(key_id, expect_not_exist, expect_full);

    if (expect_full) {
      EXPECT_EQ(NodeReturnCode::kNoSpace, rc);
    } else if (expect_not_exist) {
      EXPECT_EQ(NodeReturnCode::kKeyNotExist, rc);
    } else {
      EXPECT_EQ(NodeReturnCode::kSuccess, rc);
      VerifyMetadata(node->GetMetadata(expected_record_count - 1), false);
      VerifyStatusWord(status);
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
TYPED_TEST_CASE(LeafNodeFixture, KeyPayloadPairs);

/*##################################################################################################
 * Unit test definitions
 *################################################################################################*/

/*--------------------------------------------------------------------------------------------------
 * Read operation
 *------------------------------------------------------------------------------------------------*/

TYPED_TEST(LeafNodeFixture, Read_NotInsertedKey_ReadFail)
{  //
  TestFixture::VerifyRead(1, 1, true);
}

TYPED_TEST(LeafNodeFixture, Read_DeletedKey_ReadFail)
{
  TestFixture::Insert(1, 1);
  TestFixture::Delete(1);

  TestFixture::VerifyRead(1, 1, true);
}

// /*--------------------------------------------------------------------------------------------------
//  * Scan operation
//  *------------------------------------------------------------------------------------------------*/

// TYPED_TEST(LeafNodeFixture, Scan_EmptyNode_NoResult)
// {
// //   auto [rc, scan_results] = LeafNode_t::Scan(node.get(), &keys[0], true, &keys[9], true);

//   EXPECT_EQ(NodeReturnCode::kSuccess, rc);
//   EXPECT_EQ(0, scan_results.size());
// }

// TYPED_TEST(LeafNodeFixture, Scan_BothClosed_ScanTargetValues)
// {
// //   TestFixture::WriteOrderedKeys(4, 9);
//   TestFixture::WriteOrderedKeys(0, 3);

//   auto [rc, scan_results] = LeafNode_t::Scan(node.get(), &keys[4], true, &keys[6], true);

//   EXPECT_EQ(NodeReturnCode::kSuccess, rc);
//   EXPECT_EQ(3, scan_results.size());
//   VerifyKey(keys[4], scan_results[0]->GetKey());
//   VerifyPayload(payloads[4], scan_results[0]->GetPayload());
//   VerifyKey(keys[5], scan_results[1]->GetKey());
//   VerifyPayload(payloads[5], scan_results[1]->GetPayload());
//   VerifyKey(keys[6], scan_results[2]->GetKey());
//   VerifyPayload(payloads[6], scan_results[2]->GetPayload());
// }

// TYPED_TEST(LeafNodeFixture, Scan_LeftClosed_ScanTargetValues)
// {
// //   TestFixture::WriteOrderedKeys(4, 9);
//   TestFixture::WriteOrderedKeys(0, 3);

//   auto [rc, scan_results] = LeafNode_t::Scan(node.get(), &keys[7], true, &keys[9], false);

//   EXPECT_EQ(NodeReturnCode::kSuccess, rc);
//   EXPECT_EQ(2, scan_results.size());
//   VerifyKey(keys[7], scan_results[0]->GetKey());
//   VerifyPayload(payloads[7], scan_results[0]->GetPayload());
//   VerifyKey(keys[8], scan_results[1]->GetKey());
//   VerifyPayload(payloads[8], scan_results[1]->GetPayload());
// }

// TYPED_TEST(LeafNodeFixture, Scan_RightClosed_ScanTargetValues)
// {
// //   TestFixture::WriteOrderedKeys(4, 9);
//   TestFixture::WriteOrderedKeys(0, 3);

//   auto [rc, scan_results] = LeafNode_t::Scan(node.get(), &keys[7], false, &keys[9], true);

//   EXPECT_EQ(NodeReturnCode::kSuccess, rc);
//   EXPECT_EQ(2, scan_results.size());
//   VerifyKey(keys[8], scan_results[0]->GetKey());
//   VerifyPayload(payloads[8], scan_results[0]->GetPayload());
//   VerifyKey(keys[9], scan_results[1]->GetKey());
//   VerifyPayload(payloads[9], scan_results[1]->GetPayload());
// }

// TYPED_TEST(LeafNodeFixture, Scan_BothOpened_ScanTargetValues)
// {
// //   TestFixture::WriteOrderedKeys(4, 9);
//   TestFixture::WriteOrderedKeys(0, 3);

//   auto [rc, scan_results] = LeafNode_t::Scan(node.get(), &keys[7], false, &keys[9], false);

//   EXPECT_EQ(NodeReturnCode::kSuccess, rc);
//   EXPECT_EQ(1, scan_results.size());
//   VerifyKey(keys[8], scan_results[0]->GetKey());
//   VerifyPayload(payloads[8], scan_results[0]->GetPayload());
// }

// TYPED_TEST(LeafNodeFixture, Scan_LeftInfinity_ScanTargetValues)
// {
// //   TestFixture::WriteOrderedKeys(4, 9);
//   TestFixture::WriteOrderedKeys(0, 3);

//   auto [rc, scan_results] = LeafNode_t::Scan(node.get(), nullptr, false, &keys[1], true);

//   EXPECT_EQ(NodeReturnCode::kSuccess, rc);
//   EXPECT_EQ(2, scan_results.size());
//   VerifyKey(keys[0], scan_results[0]->GetKey());
//   VerifyPayload(payloads[0], scan_results[0]->GetPayload());
//   VerifyKey(keys[1], scan_results[1]->GetKey());
//   VerifyPayload(payloads[1], scan_results[1]->GetPayload());
// }

// TYPED_TEST(LeafNodeFixture, Scan_RightInfinity_ScanTargetValues)
// {
// //   TestFixture::WriteOrderedKeys(4, 9);
//   TestFixture::WriteOrderedKeys(0, 3);

//   auto [rc, scan_results] = LeafNode_t::Scan(node.get(), &keys[8], true, nullptr, false);

//   EXPECT_EQ(NodeReturnCode::kSuccess, rc);
//   EXPECT_EQ(2, scan_results.size());
//   VerifyKey(keys[8], scan_results[0]->GetKey());
//   VerifyPayload(payloads[8], scan_results[0]->GetPayload());
//   VerifyKey(keys[9], scan_results[1]->GetKey());
//   VerifyPayload(payloads[9], scan_results[1]->GetPayload());
// }

// TYPED_TEST(LeafNodeFixture, Scan_LeftOutsideRange_NoResults)
// {
// //   TestFixture::WriteOrderedKeys(4, 9);

//   auto [rc, scan_results] = LeafNode_t::Scan(node.get(), nullptr, false, &keys[3], false);

//   EXPECT_EQ(NodeReturnCode::kSuccess, rc);
//   EXPECT_EQ(0, scan_results.size());
// }

// TYPED_TEST(LeafNodeFixture, Scan_RightOutsideRange_NoResults)
// {
// //   TestFixture::WriteOrderedKeys(0, 3);

//   auto [rc, scan_results] = LeafNode_t::Scan(node.get(), &keys[5], false, nullptr, false);

//   EXPECT_EQ(NodeReturnCode::kSuccess, rc);
//   EXPECT_EQ(0, scan_results.size());
// }

// TYPED_TEST(LeafNodeFixture, Scan_WithUpdateDelete_ScanLatestValues)
// {
// //   TestFixture::WriteOrderedKeys(0, 4);
//   LeafNode_t::Update(node.get(), keys[2], kKeyLength, payloads[0], kPayloadLength);
//   LeafNode_t::Delete(node.get(), keys[3], kKeyLength);

//   auto [rc, scan_results] = LeafNode_t::Scan(node.get(), &keys[2], true, &keys[4], true);

//   EXPECT_EQ(NodeReturnCode::kSuccess, rc);
//   EXPECT_EQ(2, scan_results.size());
//   VerifyKey(keys[2], scan_results[0]->GetKey());
//   VerifyPayload(payloads[0], scan_results[0]->GetPayload());
//   VerifyKey(keys[4], scan_results[1]->GetKey());
//   VerifyPayload(payloads[4], scan_results[1]->GetPayload());
// }

// TYPED_TEST(LeafNodeFixture, Scan_ConsolidatedNodeWithinRange_ScanTargetValues)
// {
// //   // prepare a consolidated node
//   TestFixture::WriteOrderedKeys(3, 7);
//   auto meta_vec = LeafNode_t::GatherSortedLiveMetadata(node.get());
//   node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));

//   auto [rc, scan_results] = LeafNode_t::Scan(node.get(), &keys[4], true, &keys[6], true);

//   EXPECT_EQ(NodeReturnCode::kSuccess, rc);
//   EXPECT_EQ(3, scan_results.size());
//   VerifyKey(keys[4], scan_results[0]->GetKey());
//   VerifyPayload(payloads[4], scan_results[0]->GetPayload());
//   VerifyKey(keys[5], scan_results[1]->GetKey());
//   VerifyPayload(payloads[5], scan_results[1]->GetPayload());
//   VerifyKey(keys[6], scan_results[2]->GetKey());
//   VerifyPayload(payloads[6], scan_results[2]->GetPayload());
// }

// TYPED_TEST(LeafNodeFixture, Scan_ConsolidatedNodeWithLeftInfinity_ScanTargetValues)
// {
// //   // prepare a consolidated node
//   TestFixture::WriteOrderedKeys(3, 7);
//   auto meta_vec = LeafNode_t::GatherSortedLiveMetadata(node.get());
//   node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));

//   auto [rc, scan_results] = LeafNode_t::Scan(node.get(), nullptr, true, &keys[3], true);

//   EXPECT_EQ(NodeReturnCode::kSuccess, rc);
//   EXPECT_EQ(1, scan_results.size());
//   VerifyKey(keys[3], scan_results[0]->GetKey());
//   VerifyPayload(payloads[3], scan_results[0]->GetPayload());
// }

// TYPED_TEST(LeafNodeFixture, Scan_ConsolidatedNodeWithRightInfinity_ScanTargetValues)
// {
// //   // prepare a consolidated node
//   TestFixture::WriteOrderedKeys(3, 7);
//   auto meta_vec = LeafNode_t::GatherSortedLiveMetadata(node.get());
//   node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));

//   auto [rc, scan_results] = LeafNode_t::Scan(node.get(), &keys[7], true, nullptr, true);

//   EXPECT_EQ(NodeReturnCode::kSuccess, rc);
//   EXPECT_EQ(1, scan_results.size());
//   VerifyKey(keys[7], scan_results[0]->GetKey());
//   VerifyPayload(payloads[7], scan_results[0]->GetPayload());
// }

// TYPED_TEST(LeafNodeFixture, Scan_ConsolidatedNodeWithUpdateDelete_ScanLatestValues)
// {
// //   // prepare a consolidated node
//   TestFixture::WriteOrderedKeys(3, 7);
//   auto meta_vec = LeafNode_t::GatherSortedLiveMetadata(node.get());
//   node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));
//   LeafNode_t::Update(node.get(), keys[5], kKeyLength, payloads[0], kPayloadLength);
//   LeafNode_t::Delete(node.get(), keys[7], kKeyLength);

//   auto [rc, scan_results] = LeafNode_t::Scan(node.get(), &keys[5], true, &keys[7], true);

//   EXPECT_EQ(NodeReturnCode::kSuccess, rc);
//   EXPECT_EQ(2, scan_results.size());
//   VerifyKey(keys[5], scan_results[0]->GetKey());
//   VerifyPayload(payloads[0], scan_results[0]->GetPayload());
//   VerifyKey(keys[6], scan_results[1]->GetKey());
//   VerifyPayload(payloads[6], scan_results[1]->GetPayload());
// }

/*--------------------------------------------------------------------------------------------------
 * Write operation
 *------------------------------------------------------------------------------------------------*/

TYPED_TEST(LeafNodeFixture, Write_UniqueKeys_ReadWrittenValues)
{
  for (size_t i = 1; i <= TestFixture::max_record_num; ++i) {
    TestFixture::VerifyWrite(i, i);
  }
}

TYPED_TEST(LeafNodeFixture, Write_DuplicateKey_ReadLatestValue)
{
  TestFixture::Write(1, 1);
  TestFixture::Write(1, 2);

  TestFixture::VerifyRead(1, 2);
}

TYPED_TEST(LeafNodeFixture, Write_FilledNode_GetCorrectReturnCodes)
{
  TestFixture::WriteNullKey(TestFixture::max_record_num - 1);

  TestFixture::VerifyWrite(1, 1);
  TestFixture::VerifyWrite(2, 2, true);
}

TYPED_TEST(LeafNodeFixture, Write_ConsolidatedNode_ReadWrittenValue)
{
  TestFixture::PrepareConsolidatedNode(1, 5);

  TestFixture::VerifyWrite(6, 6);
}

/*--------------------------------------------------------------------------------------------------
 * Insert operation
 *------------------------------------------------------------------------------------------------*/

TYPED_TEST(LeafNodeFixture, Insert_UniqueKeys_ReadInsertedValues)
{
  for (size_t i = 1; i <= TestFixture::max_record_num; ++i) {
    TestFixture::VerifyInsert(i, i);
  }
}

TYPED_TEST(LeafNodeFixture, Insert_DuplicateKey_InsertionFail)
{
  TestFixture::Insert(1, 1);

  TestFixture::VerifyInsert(1, 2, true);
  TestFixture::VerifyRead(1, 1);
}

TYPED_TEST(LeafNodeFixture, Insert_FilledNode_GetCorrectReturnCodes)
{
  TestFixture::WriteNullKey(TestFixture::max_record_num - 1);

  TestFixture::VerifyInsert(1, 1);
  TestFixture::VerifyInsert(2, 2, false, true);
}

TYPED_TEST(LeafNodeFixture, Insert_ConsolidatedNode_ReadInsertedValue)
{
  TestFixture::PrepareConsolidatedNode(1, 5);

  TestFixture::VerifyInsert(6, 6);
}

TYPED_TEST(LeafNodeFixture, Insert_ConsolidatedNodeWithDuplicateKey_InsertionFail)
{
  TestFixture::PrepareConsolidatedNode(1, 5);

  TestFixture::VerifyInsert(1, 2, true);
}

/*--------------------------------------------------------------------------------------------------
 * Update operation
 *------------------------------------------------------------------------------------------------*/

TYPED_TEST(LeafNodeFixture, Update_DuplicateKey_ReadUpdatedValue)
{
  TestFixture::Insert(1, 1);

  TestFixture::VerifyUpdate(1, 2);
  TestFixture::VerifyUpdate(1, 3);
}

TYPED_TEST(LeafNodeFixture, Update_NotInsertedKey_UpdatedFail)
{
  TestFixture::VerifyUpdate(1, 2, true);
}

TYPED_TEST(LeafNodeFixture, Update_DeletedKey_UpdateFail)
{
  TestFixture::Insert(1, 1);
  TestFixture::Delete(1);

  TestFixture::VerifyUpdate(1, 2, true);
}

TYPED_TEST(LeafNodeFixture, Update_FilledNode_GetCorrectReturnCodes)
{
  TestFixture::WriteOrderedKeys(1, TestFixture::max_record_num - 1);

  TestFixture::VerifyUpdate(1, 2);
  TestFixture::VerifyUpdate(2, 3, false, true);
}

TYPED_TEST(LeafNodeFixture, Update_ConsolidatedNode_ReadUpdatedValue)
{
  TestFixture::PrepareConsolidatedNode(1, 5);

  TestFixture::VerifyUpdate(1, 2);
  TestFixture::VerifyUpdate(1, 3);
}

TYPED_TEST(LeafNodeFixture, Update_ConsolidatedNodeWithNotInsertedKey_UpdatedFail)
{
  TestFixture::PrepareConsolidatedNode(1, 5);

  TestFixture::VerifyUpdate(6, 1, true);
}

TYPED_TEST(LeafNodeFixture, Update_ConsolidatedNodeWithDeletedKey_UpdatedFail)
{
  TestFixture::PrepareConsolidatedNode(1, 5);
  TestFixture::Delete(1);

  TestFixture::VerifyUpdate(1, 2, true);
}

/*--------------------------------------------------------------------------------------------------
 * Delete operation
 *------------------------------------------------------------------------------------------------*/

TYPED_TEST(LeafNodeFixture, Delete_InsertedKey_DeleteSucceed)
{
  TestFixture::Insert(1, 1);

  TestFixture::VerifyDelete(1);
}

TYPED_TEST(LeafNodeFixture, Delete_NotInsertedKey_DeleteFail)
{
  TestFixture::VerifyDelete(1, true);
}

TYPED_TEST(LeafNodeFixture, Delete_DeletedKey_DeleteFail)
{
  TestFixture::Insert(1, 1);
  TestFixture::Delete(1);

  TestFixture::VerifyDelete(1, true);
}

TYPED_TEST(LeafNodeFixture, Delete_FilledNode_GetCorrectReturnCodes)
{
  TestFixture::WriteOrderedKeys(1, TestFixture::max_record_num);

  for (size_t index = 1; index <= 5; ++index) {
    const bool is_full =
        kPageSize - TestFixture::expected_occupied_size < kWordLength + TestFixture::key_length;
    TestFixture::VerifyDelete(index, false, is_full);
  }
}

TYPED_TEST(LeafNodeFixture, Delete_ConsolidatedNode_DeleteSucceed)
{
  TestFixture::PrepareConsolidatedNode(1, 5);

  TestFixture::VerifyDelete(1);
}

TYPED_TEST(LeafNodeFixture, Delete_ConsolidatedNodeWithNotInsertedKey_DeleteFail)
{
  TestFixture::PrepareConsolidatedNode(1, 5);

  TestFixture::VerifyDelete(6, true);
}

TYPED_TEST(LeafNodeFixture, Delete_ConsolidatedNodeWithDeletedKey_DeleteFail)
{
  TestFixture::PrepareConsolidatedNode(1, 5);
  TestFixture::Delete(1);

  TestFixture::VerifyDelete(1, true);
}

// /*--------------------------------------------------------------------------------------------------
//  * Consolide operation
//  *------------------------------------------------------------------------------------------------*/

// TYPED_TEST(LeafNodeFixture, Consolidate_SortedTenKeys_GatherSortedLiveMetadata)
// {
//   // fill a node with ordered keys
//   auto written_keys = TestFixture::WriteOrderedKeys(0, 9);

//   // gather live metadata and check equality
//   auto meta_vec = LeafNode_t::GatherSortedLiveMetadata(node.get());

//   EXPECT_EQ(expected_record_count, meta_vec.size());
//   for (size_t index = 0; index < meta_vec.size(); index++) {
//     auto [key, meta] = meta_vec[index];
//     VerifyKey(written_keys[index], key);
//     EXPECT_TRUE(meta.IsVisible());
//   }
// }

// TYPED_TEST(LeafNodeFixture, Consolidate_SortedTenKeysWithDelete_GatherSortedLiveMetadata)
// {
//   // fill a node with ordered keys
//   auto written_keys = TestFixture::WriteOrderedKeys(0, 8);

//   // delete a key
//   LeafNode_t::Delete(node.get(), keys[2], kKeyLength);
//   --expected_record_count;
//   written_keys.erase(std::find(written_keys.begin(), written_keys.end(), keys[2]));

//   // gather live metadata and check equality
//   auto meta_vec = LeafNode_t::GatherSortedLiveMetadata(node.get());

//   EXPECT_EQ(expected_record_count, meta_vec.size());
//   for (size_t index = 0; index < meta_vec.size(); index++) {
//     auto [key, meta] = meta_vec[index];
//     VerifyKey(written_keys[index], key);
//     EXPECT_TRUE(meta.IsVisible());
//   }
// }

// TYPED_TEST(LeafNodeFixture, Consolidate_SortedTenKeysWithUpdate_GatherSortedLiveMetadata)
// {
//   // fill a node with ordered keys
//   auto written_keys = TestFixture::WriteOrderedKeys(0, 8);
//   LeafNode_t::Update(node.get(), keys[2], kKeyLength, payload_null, kNullPayloadLength);

//   // gather live metadata and check equality
//   auto meta_vec = LeafNode_t::GatherSortedLiveMetadata(node.get());

//   EXPECT_EQ(expected_record_count, meta_vec.size());
//   for (size_t index = 0; index < meta_vec.size(); index++) {
//     auto [key, meta] = meta_vec[index];
//     VerifyKey(written_keys[index], key);
//     EXPECT_TRUE(meta.IsVisible());
//   }
// }

// TYPED_TEST(LeafNodeFixture, Consolidate_UnsortedTenKeys_GatherSortedLiveMetadata)
// {
//   // fill a node with ordered keys
//   auto tmp_keys = TestFixture::WriteOrderedKeys(4, 9);
//   auto written_keys = TestFixture::WriteOrderedKeys(0, 3);
//   written_keys.insert(written_keys.end(), tmp_keys.begin(), tmp_keys.end());

//   // gather live metadata and check equality
//   auto meta_vec = LeafNode_t::GatherSortedLiveMetadata(node.get());

//   EXPECT_EQ(expected_record_count, meta_vec.size());
//   for (size_t index = 0; index < meta_vec.size(); index++) {
//     auto [key, meta] = meta_vec[index];
//     VerifyKey(written_keys[index], key);
//     EXPECT_TRUE(meta.IsVisible());
//   }
// }

// TYPED_TEST(LeafNodeFixture, Consolidate_SortedTenKeys_NodeHasCorrectStatus)
// {
//   // prepare a consolidated node
//   TestFixture::WriteOrderedKeys(0, 9);
//   auto meta_vec = LeafNode_t::GatherSortedLiveMetadata(node.get());
//   node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));

//   VerifyStatusWord(node->GetStatusWord());
// }

// TYPED_TEST(LeafNodeFixture, Consolidate_SortedTenKeysWithDelete_NodeHasCorrectStatus)
// {
//   // prepare a consolidated node
//   TestFixture::WriteOrderedKeys(0, 8);
//   LeafNode_t::Delete(node.get(), keys[2], kKeyLength);
//   expected_record_count -= 1;
//   expected_block_size -= kRecordLength;

//   auto meta_vec = LeafNode_t::GatherSortedLiveMetadata(node.get());
//   node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));

//   VerifyStatusWord(node->GetStatusWord());
// }

// TYPED_TEST(LeafNodeFixture, Consolidate_SortedTenKeysWithUpdate_NodeHasCorrectStatus)
// {
//   // prepare a consolidated node
//   TestFixture::WriteOrderedKeys(0, 8);
//   LeafNode_t::Update(node.get(), keys[2], kKeyLength, payload_null, kNullPayloadLength);
//   expected_block_size += kNullPayloadLength - kPayloadLength;

//   auto meta_vec = LeafNode_t::GatherSortedLiveMetadata(node.get());
//   node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));

//   VerifyStatusWord(node->GetStatusWord());
// }

// TYPED_TEST(LeafNodeFixture, Consolidate_UnsortedTenKeys_NodeHasCorrectStatus)
// {
//   // prepare a consolidated node
//   TestFixture::WriteOrderedKeys(4, 9);
//   TestFixture::WriteOrderedKeys(0, 3);

//   auto meta_vec = LeafNode_t::GatherSortedLiveMetadata(node.get());
//   node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));

//   VerifyStatusWord(node->GetStatusWord());
// }

// /*--------------------------------------------------------------------------------------------------
//  * Split operation
//  *------------------------------------------------------------------------------------------------*/

// TYPED_TEST(LeafNodeFixture, Split_EquallyDivided_NodesHaveCorrectStatus)
// {
//   // prepare split nodes
//   TestFixture::WriteOrderedKeys(0, 9);
//   const auto left_record_count = 5;
//   auto meta_vec = LeafNode_t::GatherSortedLiveMetadata(node.get());

//   auto [left_node, right_node] = LeafNode_t::Split(node.get(), meta_vec, left_record_count);
//   expected_record_count = 5;
//   expected_block_size = expected_record_count * kRecordLength;

//   node.reset(left_node);
//   VerifyStatusWord(node->GetStatusWord());

//   node.reset(right_node);
//   VerifyStatusWord(node->GetStatusWord());
// }

// TYPED_TEST(LeafNodeFixture, Split_EquallyDivided_NodesHaveCorrectKeyPayloads)
// {
//   // prepare split nodes
//   TestFixture::WriteOrderedKeys(0, 9);
//   const auto left_record_count = 5;
//   auto meta_vec = LeafNode_t::GatherSortedLiveMetadata(node.get());
//   auto [left_node, right_node] = LeafNode_t::Split(node.get(), meta_vec, left_record_count);

//   // check a split left node
//   size_t index = 0;
//   for (; index < left_record_count; ++index) {
//     VerifyRead(left_node, keys[index], payloads[index]);
//   }

//   // check a split right node
//   for (; index < expected_record_count; ++index) {
//     VerifyRead(right_node, keys[index], payloads[index]);
//   }
// }

// /*--------------------------------------------------------------------------------------------------
//  * Merge operation
//  *------------------------------------------------------------------------------------------------*/

// TYPED_TEST(LeafNodeFixture, Merge_LeftSiblingNode_NodeHasCorrectStatus)
// {
//   // prepare a merged node
//   auto target = std::unique_ptr<BaseNode_t>(BaseNode_t::CreateEmptyNode(kLeafFlag));
//   LeafNode_t::Write(target.get(), keys[4], kKeyLength, payloads[4], kPayloadLength);
//   auto target_meta = LeafNode_t::GatherSortedLiveMetadata(target.get());

//   auto sibling = std::unique_ptr<BaseNode_t>(BaseNode_t::CreateEmptyNode(kLeafFlag));
//   LeafNode_t::Write(sibling.get(), keys[3], kKeyLength, payloads[3], kPayloadLength);
//   auto sibling_meta = LeafNode_t::GatherSortedLiveMetadata(sibling.get());

//   node.reset(LeafNode_t::Merge(target.get(), target_meta, sibling.get(), sibling_meta, true));
//   expected_record_count = 2;
//   expected_block_size = expected_record_count * kRecordLength;

//   VerifyStatusWord(node->GetStatusWord());
// }

// TYPED_TEST(LeafNodeFixture, Merge_RightSiblingNode_NodeHasCorrectStatus)
// {
//   // prepare a merged node
//   auto target = std::unique_ptr<BaseNode_t>(BaseNode_t::CreateEmptyNode(kLeafFlag));
//   LeafNode_t::Write(target.get(), keys[2], kKeyLength, payloads[2], kPayloadLength);
//   auto target_meta = LeafNode_t::GatherSortedLiveMetadata(target.get());

//   auto sibling = std::unique_ptr<BaseNode_t>(BaseNode_t::CreateEmptyNode(kLeafFlag));
//   LeafNode_t::Write(sibling.get(), keys[3], kKeyLength, payloads[3], kPayloadLength);
//   auto sibling_meta = LeafNode_t::GatherSortedLiveMetadata(sibling.get());

//   node.reset(LeafNode_t::Merge(target.get(), target_meta, sibling.get(), sibling_meta, false));
//   expected_record_count = 2;
//   expected_block_size = expected_record_count * kRecordLength;

//   VerifyStatusWord(node->GetStatusWord());
// }

// TYPED_TEST(LeafNodeFixture, Merge_LeftSiblingNode_NodeHasCorrectKeyPayloads)
// {
//   // prepare a merged node
//   auto target = std::unique_ptr<BaseNode_t>(BaseNode_t::CreateEmptyNode(kLeafFlag));
//   LeafNode_t::Write(target.get(), keys[4], kKeyLength, payloads[4], kPayloadLength);
//   auto target_meta = LeafNode_t::GatherSortedLiveMetadata(target.get());

//   auto sibling = std::unique_ptr<BaseNode_t>(BaseNode_t::CreateEmptyNode(kLeafFlag));
//   LeafNode_t::Write(sibling.get(), keys[3], kKeyLength, payloads[3], kPayloadLength);
//   auto sibling_meta = LeafNode_t::GatherSortedLiveMetadata(sibling.get());

//   node.reset(LeafNode_t::Merge(target.get(), target_meta, sibling.get(), sibling_meta, true));

//   // check keys and payloads
//   for (size_t index = 3; index <= 4; ++index) {
//     VerifyRead(node.get(), keys[index], payloads[index]);
//   }
// }

// TYPED_TEST(LeafNodeFixture, Merge_RightSiblingNode_NodeHasCorrectKeyPayloads)
// {
//   // prepare a merged node
//   auto target = std::unique_ptr<BaseNode_t>(BaseNode_t::CreateEmptyNode(kLeafFlag));
//   LeafNode_t::Write(target.get(), keys[2], kKeyLength, payloads[2], kPayloadLength);
//   auto target_meta = LeafNode_t::GatherSortedLiveMetadata(target.get());

//   auto sibling = std::unique_ptr<BaseNode_t>(BaseNode_t::CreateEmptyNode(kLeafFlag));
//   LeafNode_t::Write(sibling.get(), keys[3], kKeyLength, payloads[3], kPayloadLength);
//   auto sibling_meta = LeafNode_t::GatherSortedLiveMetadata(sibling.get());

//   node.reset(LeafNode_t::Merge(target.get(), target_meta, sibling.get(), sibling_meta, false));

//   // check keys and payloads
//   for (size_t index = 2; index <= 3; ++index) {
//     VerifyRead(node.get(), keys[index], payloads[index]);
//   }
// }

}  // namespace dbgroup::index::bztree
