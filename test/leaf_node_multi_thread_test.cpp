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

#include "bztree/components/leaf_node.hpp"
#include "gtest/gtest.h"

namespace dbgroup::index::bztree
{
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

  using RunResult = std::pair<std::vector<size_t>, std::vector<size_t>>;

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
  std::unique_ptr<BaseNode_t> node;

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
    node.reset(BaseNode_t::CreateEmptyNode(kLeafFlag));

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
    record_length = key_length + payload_length;
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
    if constexpr (std::is_same_v<Payload, char*>) {
      for (size_t index = 0; index < kKeyNumForTest; ++index) {
        delete[] payloads[index];
      }
    }
  }

  /*################################################################################################
   * Utility functions
   *##############################################################################################*/

  std::pair<NodeReturnCode, StatusWord>
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
          return PrepareOperation(WriteType::kInsert, rand_engine);
        } else if (rand_val == 1) {
          return PrepareOperation(WriteType::kUpdate, rand_engine);
        } else {
          return PrepareOperation(WriteType::kDelete, rand_engine);
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
        if (PerformWriteOperation(ops).first == NodeReturnCode::kSuccess) {
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
      if constexpr (std::is_same_v<Payload, char*>) {
        EXPECT_TRUE(IsEqual<PayloadComp>(payloads[expected_id], actual.get()));
      } else {
        EXPECT_TRUE(IsEqual<PayloadComp>(payloads[expected_id], actual));
      }
    }
  }

  void
  VerifyWrite()
  {
    const size_t write_num_per_thread = max_record_num / kThreadNum;
    auto written_ids = RunOverMultiThread(write_num_per_thread, kThreadNum, WriteType::kWrite);

    for (auto&& id : written_ids) {
      VerifyRead(id, id);
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

// TYPED_TEST(LeafNodeFixture, Insert_MultiThreads_ReadWrittenPayloads)
// {
//   auto [written_indexes, failed_indexes] = RunOverMultiThread(
//       kWriteNumPerThread, kThreadNum, kInsert, &LeafNodeFixture::WriteRandomKeys);

//   EXPECT_LE(written_indexes.size(), kWriteNumPerThread);
//   EXPECT_EQ(kWriteNumPerThread * kThreadNum, written_indexes.size() + failed_indexes.size());
//   for (auto&& index : written_indexes) {
//     auto [rc, payload] = LeafNode_t::Read(node.get(), keys[index]);
//     EXPECT_EQ(NodeReturnCode::kSuccess, rc);
//     VerifyPayload(payloads[index], payload.get());
//   }
//   for (auto&& index : failed_indexes) {
//     auto [rc, payload] = LeafNode_t::Read(node.get(), keys[index]);
//     EXPECT_EQ(NodeReturnCode::kSuccess, rc);
//     VerifyPayload(payloads[index], payload.get());
//   }
// }

// TYPED_TEST(LeafNodeFixture, Update_MultiThreads_ReadWrittenPayloads)
// {
//   constexpr size_t kWriteNumHalf = kWriteNumPerThread * 0.5;

//   RunOverMultiThread(kWriteNumHalf, kThreadNum, kWrite, &LeafNodeFixture::WriteRandomKeys);
//   auto [written_indexes, failed_indexes] =
//       RunOverMultiThread(kWriteNumHalf, kThreadNum, kUpdate,
//       &LeafNodeFixture::WriteRandomKeys);

//   EXPECT_EQ(kWriteNumHalf * kThreadNum, written_indexes.size());
//   for (auto&& index : written_indexes) {
//     auto [rc, payload] = LeafNode_t::Read(node.get(), keys[index]);
//     EXPECT_EQ(NodeReturnCode::kSuccess, rc);
//     VerifyPayload(payloads[index + 1], payload.get());
//   }
// }

// TYPED_TEST(LeafNodeFixture, Delete_MultiThreads_KeysDeleted)
// {
//   constexpr size_t kWriteNumHalf = kWriteNumPerThread * 0.5;

//   RunOverMultiThread(kWriteNumHalf, kThreadNum, kWrite, &LeafNodeFixture::WriteRandomKeys);
//   auto [written_indexes, failed_indexes] =
//       RunOverMultiThread(kWriteNumHalf, kThreadNum, kDelete,
//       &LeafNodeFixture::WriteRandomKeys);

//   EXPECT_EQ(kWriteNumHalf * kThreadNum, written_indexes.size() + failed_indexes.size());
//   for (auto&& index : written_indexes) {
//     auto [rc, record] = LeafNode_t::Read(reinterpret_cast<BaseNode_t*>(node.get()),
//     keys[index]); EXPECT_EQ(NodeReturnCode::kKeyNotExist, rc);
//   }
// }

// TYPED_TEST(LeafNodeFixture, InsertUpdateDelete_MultiThreads_ConcurrencyControlCorrupted)
// {
//   constexpr size_t kWriteNumForTwoThreads = kMaxRecordNum / 2;
//   for (size_t i = 0; i < 100; ++i) {
//     // insert/update/delete the same key by multi-threads
//     node.reset(BaseNode_t::CreateEmptyNode(kLeafFlag));
//     RunOverMultiThread(kWriteNumForTwoThreads, 2, kMixed, &LeafNodeFixture::WriteRandomKeys);
//     bool previous_is_update = false;
//     bool concurrency_is_corrupted = false;

//     const auto status = node->GetStatusWord();
//     // check inserted/updated/deleted records linearly
//     for (int64_t index = status.GetRecordCount() - 1; index >= 0; --index) {
//       const auto meta = node->GetMetadata(index);
//       if (meta.IsVisible()) {
//         // an inserted or updated record
//         const auto record = node->GetRecord(meta);
//         if (IsEqual<PayloadComparator>(payloads[1], record->GetPayload())) {
//           // 1 is an updated value
//           previous_is_update = true;
//         } else {
//           // 0 is an inserted value
//           previous_is_update = false;
//         }
//       } else if (meta.IsDeleted()) {
//         // a deleted record
//         if (previous_is_update) {
//           // updating a deleted value is invalid
//           concurrency_is_corrupted = true;
//         }
//         previous_is_update = false;
//       }
//     }

//     EXPECT_FALSE(concurrency_is_corrupted);
//     if (concurrency_is_corrupted) break;
//   }
// }

}  // namespace dbgroup::index::bztree
