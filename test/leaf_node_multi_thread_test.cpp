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

#include <future>
#include <memory>
#include <random>
#include <shared_mutex>
#include <thread>
#include <utility>
#include <vector>

#include "bztree/component/leaf_node.hpp"
#include "gtest/gtest.h"

namespace dbgroup::index::bztree::component::test
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
  using Node_t = Node<Key, Payload, KeyComp>;
  using LeafNode_t = LeafNode<Key, Payload, KeyComp>;

  enum WriteType
  {
    kWrite,
    kInsert,
    kUpdate,
    kDelete,
    kMixed
  };

  struct Operation {
    WriteType w_type;
    size_t key_id;
    size_t payload_id;
  };

  // constant values for testing
  static constexpr size_t kIndexEpoch = 1;
  static constexpr size_t kKeyNumForTest = 1024;
  static constexpr size_t kKeyLength = kWordLength;
  static constexpr size_t kPayloadLength = kWordLength;
  static constexpr size_t kRandomSeed = 10;
  static constexpr size_t kRepeatNum = 10;
#ifdef BZTREE_TEST_THREAD_NUM
  static constexpr size_t kThreadNum = BZTREE_TEST_THREAD_NUM;
#else
  static constexpr size_t kThreadNum = 8;
#endif

  // actual keys and payloads
  size_t key_length;
  size_t payload_length;
  Key keys[kKeyNumForTest];
  Payload payloads[kKeyNumForTest];

  // the length of a record and its maximum number
  size_t record_length;
  size_t max_record_num;

  // a leaf node
  std::unique_ptr<Node_t> node;

  std::uniform_int_distribution<size_t> id_dist{0, kKeyNumForTest - 2};

  std::shared_mutex main_lock;

  std::shared_mutex worker_lock;

  /*################################################################################################
   * Setup/Teardown
   *##############################################################################################*/

  void
  SetUp()
  {
    // initialize a leaf node and expected statistics
    node.reset(Node_t::CreateEmptyNode(kLeafFlag));

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

    // prepare payloads
    if constexpr (std::is_same_v<Payload, char*>) {
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
    if constexpr (!std::is_same_v<Payload, char*> && sizeof(Payload) == kWordLength) {
      record_length = 2 * kWordLength;
    } else {
      record_length = key_length + payload_length;
    }
    max_record_num = kMaxUnsortedRecNum;
  }

  void
  TearDown()
  {
    if constexpr (std::is_same_v<Key, char*>) {
      for (size_t index = 0; index < kKeyNumForTest; ++index) {
        delete[] keys[index];
      }
    }
    if constexpr (std::is_same_v<Payload, char*>) {
      for (size_t index = 0; index < kKeyNumForTest; ++index) {
        delete[] payloads[index];
      }
    }
  }

  /*################################################################################################
   * Utility functions
   *##############################################################################################*/

  NodeReturnCode
  PerformWriteOperation(const Operation& ops)
  {
    const auto key = keys[ops.key_id];
    const auto payload = payloads[ops.payload_id];

    switch (ops.w_type) {
      case kInsert:
        return LeafNode_t::Insert(node.get(), key, key_length, payload, payload_length);
      case kUpdate:
        return LeafNode_t::Update(node.get(), key, key_length, payload, payload_length);
      case kDelete:
        return LeafNode_t::Delete(node.get(), key, kKeyLength);
      case kWrite:
      case kMixed:
        break;
    }
    return LeafNode_t::Write(node.get(), key, key_length, payload, payload_length);
  }

  Operation
  PrepareOperation(  //
      const WriteType w_type,
      std::mt19937_64& rand_engine)
  {
    const auto id = id_dist(rand_engine);

    switch (w_type) {
      case kWrite:
      case kInsert:
      case kDelete:
        break;
      case kUpdate:
        return Operation{w_type, id, id + 1};
      case kMixed:
        const auto rand_val = rand_engine() % 3;
        if (rand_val == 0) {
          return Operation{WriteType::kInsert, 0, 0};
        } else if (rand_val == 1) {
          return Operation{WriteType::kUpdate, 0, 1};
        } else {
          return Operation{WriteType::kDelete, 0, 0};
        }
    }

    return Operation{w_type, id, id};
  }

  void
  WriteRandomKeys(  //
      const size_t write_num,
      const WriteType w_type,
      const size_t rand_seed,
      std::promise<std::vector<size_t>> p)
  {
    std::vector<Operation> operations;
    std::vector<size_t> written_ids;
    operations.reserve(write_num);
    written_ids.reserve(write_num);

    {  // create a lock to prevent a main thread
      const std::shared_lock<std::shared_mutex> guard{main_lock};

      // prepare operations to be executed
      std::mt19937_64 rand_engine{rand_seed};
      for (size_t i = 0; i < write_num; ++i) {
        operations.emplace_back(PrepareOperation(w_type, rand_engine));
      }
    }

    {  // wait for a main thread to release a lock
      const std::shared_lock<std::shared_mutex> lock{worker_lock};

      // perform and gather results
      for (auto&& ops : operations) {
        if (PerformWriteOperation(ops) == NodeReturnCode::kSuccess) {
          written_ids.emplace_back(ops.key_id);
        }
      }
    }

    // return results via promise
    p.set_value(std::move(written_ids));
  }

  std::vector<size_t>
  RunOverMultiThread(  //
      const size_t write_num,
      const size_t thread_num,
      const WriteType w_type)
  {
    std::vector<std::future<std::vector<size_t>>> futures;

    {  // create a lock to prevent workers from executing
      const std::unique_lock<std::shared_mutex> guard{worker_lock};

      // run a function over multi-threads with promise
      std::mt19937_64 rand_engine(kRandomSeed);
      for (size_t i = 0; i < thread_num; ++i) {
        std::promise<std::vector<size_t>> p;
        futures.emplace_back(p.get_future());
        const auto rand_seed = rand_engine();
        std::thread{
            &LeafNodeFixture::WriteRandomKeys, this, write_num, w_type, rand_seed, std::move(p)}
            .detach();
      }

      // wait for all workers to finish initialization
      const std::unique_lock<std::shared_mutex> lock{main_lock};
    }

    // gather results via promise-future
    std::vector<size_t> written_ids;
    written_ids.reserve(write_num * thread_num);
    for (auto&& future : futures) {
      auto tmp_written = future.get();
      written_ids.insert(written_ids.end(), tmp_written.begin(), tmp_written.end());
    }

    return written_ids;
  }

  auto
  GetPayload(const Metadata meta)
  {
    if constexpr (std::is_same_v<Payload, char*>) {
      return static_cast<char*>(node->GetPayloadAddr(meta));
    } else {
      Payload payload;
      memcpy(&payload, node->GetPayloadAddr(meta), sizeof(Payload));
      return payload;
    }
  }

  /*################################################################################################
   * Functions for verification
   *##############################################################################################*/

  void
  VerifyRead(  //
      const size_t key_id,
      const size_t expected_id,
      const bool expect_fail = false)
  {
    Payload payload{};

    const auto rc = LeafNode_t::Read(node.get(), keys[key_id], payload);

    if (expect_fail) {
      EXPECT_EQ(NodeReturnCode::kKeyNotExist, rc);
    } else {
      EXPECT_EQ(NodeReturnCode::kSuccess, rc);
      EXPECT_TRUE(IsEqual<PayloadComp>(payloads[expected_id], payload));
      if constexpr (std::is_same_v<Payload, char*>) {
        free(payload);
      }
    }
  }

  void
  VerifyWrite()
  {
    const size_t write_num_per_thread = max_record_num / kThreadNum;

    for (size_t i = 0; i < kRepeatNum; ++i) {
      // initialize a leaf node and expected statistics
      node.reset(Node_t::CreateEmptyNode(kLeafFlag));

      auto written_ids = RunOverMultiThread(write_num_per_thread, kThreadNum, WriteType::kWrite);
      for (auto&& id : written_ids) {
        VerifyRead(id, id);
      }
    }
  }

  void
  VerifyInsert()
  {
    const size_t write_num_per_thread = max_record_num / kThreadNum;

    for (size_t i = 0; i < kRepeatNum; ++i) {
      // initialize a leaf node and expected statistics
      node.reset(Node_t::CreateEmptyNode(kLeafFlag));

      auto inserted_ids = RunOverMultiThread(write_num_per_thread, kThreadNum, WriteType::kInsert);
      for (auto&& id : inserted_ids) {
        VerifyRead(id, id);
      }
    }
  }

  void
  VerifyUpdate()
  {
    const size_t write_num_per_thread = max_record_num / kThreadNum / 2;

    for (size_t i = 0; i < kRepeatNum; ++i) {
      // initialize a leaf node and expected statistics
      node.reset(Node_t::CreateEmptyNode(kLeafFlag));

      auto inserted_ids = RunOverMultiThread(write_num_per_thread, kThreadNum, WriteType::kInsert);
      auto updated_ids = RunOverMultiThread(write_num_per_thread, kThreadNum, WriteType::kUpdate);

      // remove duplicated updates
      std::sort(updated_ids.begin(), updated_ids.end());
      updated_ids.erase(std::unique(updated_ids.begin(), updated_ids.end()), updated_ids.end());

      EXPECT_EQ(inserted_ids.size(), updated_ids.size());
      for (auto&& id : updated_ids) {
        VerifyRead(id, id + 1);
      }
    }
  }

  void
  VerifyDelete()
  {
    const size_t write_num_per_thread = max_record_num / kThreadNum / 2;

    for (size_t i = 0; i < kRepeatNum; ++i) {
      // initialize a leaf node and expected statistics
      node.reset(Node_t::CreateEmptyNode(kLeafFlag));

      auto inserted_ids = RunOverMultiThread(write_num_per_thread, kThreadNum, WriteType::kInsert);
      auto deleted_ids = RunOverMultiThread(write_num_per_thread, kThreadNum, WriteType::kDelete);
      for (auto&& id : deleted_ids) {
        VerifyRead(id, id, true);
      }
    }
  }

  void
  VerifyConcurrentInsertUpdateDelete()
  {
    const size_t write_num_per_thread = max_record_num / kThreadNum / 2;

    for (size_t i = 0; i < kRepeatNum; ++i) {
      // initialize a leaf node and expected statistics
      node.reset(Node_t::CreateEmptyNode(kLeafFlag));

      // insert/update/delete the same key by multi-threads
      RunOverMultiThread(write_num_per_thread, kThreadNum, WriteType::kMixed);

      bool previous_is_update = false;
      bool concurrency_is_corrupted = false;

      const auto rec_count = node->GetStatusWord().GetRecordCount();
      // check inserted/updated/deleted records linearly
      for (int64_t index = rec_count - 1; index >= 0; --index) {
        const auto meta = node->GetMetadata(index);
        if (meta.IsVisible()) {
          // an inserted or updated record
          const auto payload = GetPayload(meta);
          if (IsEqual<PayloadComp>(payloads[1], payload)) {
            // 1 is an updated value
            previous_is_update = true;
          } else {
            // 0 is an inserted value
            previous_is_update = false;
          }
        } else if (meta.IsDeleted()) {
          // a deleted record
          if (previous_is_update) {
            // updating a deleted value is invalid
            concurrency_is_corrupted = true;
          }
          previous_is_update = false;
        }
      }

      EXPECT_FALSE(concurrency_is_corrupted);
      if (concurrency_is_corrupted) break;
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
                                         KeyPayloadPair<char*, int64_t, CStrComp, Int64Comp>,
                                         KeyPayloadPair<int64_t, char*, Int64Comp, CStrComp>,
                                         KeyPayloadPair<int32_t, int32_t, Int32Comp, Int32Comp>,
                                         KeyPayloadPair<char*, int32_t, CStrComp, Int32Comp>,
                                         KeyPayloadPair<int32_t, char*, Int32Comp, CStrComp>,
                                         KeyPayloadPair<char*, char*, CStrComp, CStrComp>>;
TYPED_TEST_CASE(LeafNodeFixture, KeyPayloadPairs);

/*##################################################################################################
 * Unit test definitions
 *################################################################################################*/

TYPED_TEST(LeafNodeFixture, Write_MultiThreads_ReadWrittenPayloads)
{  //
  TestFixture::VerifyWrite();
}

TYPED_TEST(LeafNodeFixture, Insert_MultiThreads_ReadInsertedPayloads)
{  //
  TestFixture::VerifyInsert();
}

TYPED_TEST(LeafNodeFixture, Update_MultiThreads_ReadUpdatedPayloads)
{  //
  TestFixture::VerifyUpdate();
}

TYPED_TEST(LeafNodeFixture, Delete_MultiThreads_ReadFailWithDeletedPayloads)
{  //
  TestFixture::VerifyDelete();
}

TYPED_TEST(LeafNodeFixture, InsertUpdateDelete_MultiThreads_WrittenValuesLinearized)
{  //
  TestFixture::VerifyConcurrentInsertUpdateDelete();
}

}  // namespace dbgroup::index::bztree::component::test
