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

#include <functional>
#include <future>
#include <memory>
#include <random>
#include <shared_mutex>
#include <thread>
#include <utility>
#include <vector>

#include "bztree/bztree.hpp"
#include "gtest/gtest.h"

namespace dbgroup::index::bztree::test
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
  using Node_t = component::Node<Key, Payload, KeyComp>;
  using BzTree_t = BzTree<Key, Payload, KeyComp>;

  enum WriteType
  {
    kWrite,
    kInsert,
    kUpdate,
    kDelete
  };

  struct Operation {
    WriteType w_type;
    size_t key_id;
    size_t payload_id;
  };

  // constant values for testing
  static constexpr size_t kIndexEpoch = 1;
  static constexpr size_t kKeyNumForTest = 8192 * 10;
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

  // a test target BzTree
  BzTree_t bztree = BzTree_t{10000};

  std::uniform_int_distribution<size_t> id_dist{0, kKeyNumForTest - 2};

  std::shared_mutex main_lock;

  std::shared_mutex worker_lock;

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

  ReturnCode
  PerformWriteOperation(const Operation &ops)
  {
    const auto key = keys[ops.key_id];
    const auto payload = payloads[ops.payload_id];

    switch (ops.w_type) {
      case kInsert:
        return bztree.Insert(key, payload, key_length, payload_length);
      case kUpdate:
        return bztree.Update(key, payload, key_length, payload_length);
      case kDelete:
        return bztree.Delete(key, key_length);
      case kWrite:
        break;
    }
    return bztree.Write(key, payload, key_length, payload_length);
  }

  Operation
  PrepareOperation(  //
      const WriteType w_type,
      std::mt19937_64 &rand_engine)
  {
    const auto id = id_dist(rand_engine);

    switch (w_type) {
      case kWrite:
      case kInsert:
      case kDelete:
        break;
      case kUpdate:
        return Operation{w_type, id, id + 1};
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
      for (auto &&ops : operations) {
        if (PerformWriteOperation(ops) == ReturnCode::kSuccess) {
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
            &BzTreeFixture::WriteRandomKeys, this, write_num, w_type, rand_seed, std::move(p)}
            .detach();
      }

      // wait for all workers to finish initialization
      const std::unique_lock<std::shared_mutex> lock{main_lock};
    }

    // gather results via promise-future
    std::vector<size_t> written_ids;
    written_ids.reserve(write_num * thread_num);
    for (auto &&future : futures) {
      auto tmp_written = future.get();
      written_ids.insert(written_ids.end(), tmp_written.begin(), tmp_written.end());
    }

    return written_ids;
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
    const auto [rc, actual] = bztree.Read(keys[key_id]);

    if (expect_fail) {
      EXPECT_EQ(ReturnCode::kKeyNotExist, rc);
    } else {
      EXPECT_EQ(ReturnCode::kSuccess, rc);
      if constexpr (std::is_same_v<Payload, char *>) {
        EXPECT_TRUE(component::IsEqual<PayloadComp>(payloads[expected_id], actual.get()));
      } else {
        EXPECT_TRUE(component::IsEqual<PayloadComp>(payloads[expected_id], actual));
      }
    }
  }

  void
  VerifyWrite()
  {
    const size_t write_num = (kKeyNumForTest - 1) / kThreadNum;

    auto written_ids = RunOverMultiThread(write_num, kThreadNum, WriteType::kWrite);
    for (auto &&id : written_ids) {
      VerifyRead(id, id);
    }
  }

  void
  VerifyInsert()
  {
    const size_t write_num = (kKeyNumForTest - 1) / kThreadNum;

    auto written_ids = RunOverMultiThread(write_num, kThreadNum, WriteType::kInsert);
    for (auto &&id : written_ids) {
      VerifyRead(id, id);
    }

    written_ids = RunOverMultiThread(write_num, kThreadNum, WriteType::kInsert);
    EXPECT_EQ(0, written_ids.size());
  }

  void
  VerifyUpdate()
  {
    const size_t write_num = (kKeyNumForTest - 1) / kThreadNum;

    auto written_ids = RunOverMultiThread(write_num, kThreadNum, WriteType::kUpdate);
    EXPECT_EQ(0, written_ids.size());

    written_ids = RunOverMultiThread(write_num, kThreadNum, WriteType::kInsert);
    auto updated_ids = RunOverMultiThread(write_num, kThreadNum, WriteType::kUpdate);

    std::sort(updated_ids.begin(), updated_ids.end());
    updated_ids.erase(std::unique(updated_ids.begin(), updated_ids.end()), updated_ids.end());

    EXPECT_EQ(written_ids.size(), updated_ids.size());
    for (auto &&id : updated_ids) {
      VerifyRead(id, id + 1);
    }
  }

  void
  VerifyDelete()
  {
    const size_t write_num = (kKeyNumForTest - 1) / kThreadNum;

    auto written_ids = RunOverMultiThread(write_num, kThreadNum, WriteType::kDelete);
    EXPECT_EQ(0, written_ids.size());

    written_ids = RunOverMultiThread(write_num, kThreadNum, WriteType::kInsert);
    auto deleted_ids = RunOverMultiThread(write_num, kThreadNum, WriteType::kDelete);

    EXPECT_EQ(written_ids.size(), deleted_ids.size());
    for (auto &&id : deleted_ids) {
      VerifyRead(id, id, true);
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

TYPED_TEST(BzTreeFixture, Write_MultiThreads_ReadWrittenPayloads)
{  //
  TestFixture::VerifyWrite();
}

TYPED_TEST(BzTreeFixture, Insert_MultiThreads_ReadInsertedPayloads)
{  //
  TestFixture::VerifyInsert();
}

TYPED_TEST(BzTreeFixture, Update_MultiThreads_ReadUpdatedPayloads)
{  //
  TestFixture::VerifyUpdate();
}

TYPED_TEST(BzTreeFixture, Delete_MultiThreads_ReadFailWithDeletedKeys)
{  //
  TestFixture::VerifyDelete();
}

}  // namespace dbgroup::index::bztree::test
