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
#include "common.hpp"
#include "gtest/gtest.h"

namespace dbgroup::index::bztree::test
{
/*##################################################################################################
 * Global constants
 *################################################################################################*/

constexpr size_t kGCTime = 1000;
#ifdef BZTREE_TEST_THREAD_NUM
constexpr size_t kThreadNum = BZTREE_TEST_THREAD_NUM;
#else
constexpr size_t kThreadNum = 8;
#endif
constexpr size_t kKeyNumForTest = 8 * 8192 * kThreadNum;
constexpr size_t kRandomSeed = 10;

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
  using Node_t = component::Node<Key, KeyComp>;
  using BzTree_t = BzTree<Key, Payload, KeyComp>;

 protected:
  /*################################################################################################
   * Internal constants
   *##############################################################################################*/

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
  }

  void
  TearDown() override
  {
    ReleaseTestData(keys_, kKeyNumForTest);
    ReleaseTestData(payloads_, kKeyNumForTest);
  }

  /*################################################################################################
   * Utility functions
   *##############################################################################################*/

  ReturnCode
  PerformWriteOperation(const Operation &ops)
  {
    const auto key = keys_[ops.key_id];
    const auto payload = payloads_[ops.payload_id];

    switch (ops.w_type) {
      case kInsert:
        return bztree_.Insert(key, payload, key_size_, pay_size_);
      case kUpdate:
        return bztree_.Update(key, payload, key_size_, pay_size_);
      case kDelete:
        return bztree_.Delete(key, key_size_);
      case kWrite:
        break;
    }
    return bztree_.Write(key, payload, key_size_, pay_size_);
  }

  Operation
  PrepareOperation(  //
      const WriteType w_type,
      std::mt19937_64 &rand_engine)
  {
    const auto id = id_dist_(rand_engine);

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
      const std::shared_lock<std::shared_mutex> guard{main_lock_};

      // prepare operations to be executed
      std::mt19937_64 rand_engine{rand_seed};
      for (size_t i = 0; i < write_num; ++i) {
        operations.emplace_back(PrepareOperation(w_type, rand_engine));
      }
    }

    {  // wait for a main thread to release a lock
      const std::shared_lock<std::shared_mutex> lock{worker_lock_};

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
      const std::unique_lock<std::shared_mutex> guard{worker_lock_};

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
      const std::unique_lock<std::shared_mutex> lock{main_lock_};
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
    const auto [rc, actual] = bztree_.Read(keys_[key_id]);

    if (expect_fail) {
      EXPECT_EQ(ReturnCode::kKeyNotExist, rc);
    } else {
      EXPECT_EQ(ReturnCode::kSuccess, rc);
      if constexpr (IsVariableLengthData<Payload>()) {
        EXPECT_TRUE(component::IsEqual<PayloadComp>(payloads_[expected_id], actual.get()));
      } else {
        EXPECT_TRUE(component::IsEqual<PayloadComp>(payloads_[expected_id], actual));
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

  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  // actual keys and payloads
  size_t key_size_;
  size_t pay_size_;
  Key keys_[kKeyNumForTest];
  Payload payloads_[kKeyNumForTest];

  // a test target BzTree
  BzTree_t bztree_ = BzTree_t{kGCTime};

  std::uniform_int_distribution<size_t> id_dist_{0, kKeyNumForTest - 2};

  std::shared_mutex main_lock_;

  std::shared_mutex worker_lock_;
};

/*##################################################################################################
 * Preparation for typed testing
 *################################################################################################*/

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

/*##################################################################################################
 * Unit test definitions
 *################################################################################################*/

TYPED_TEST(BzTreeFixture, WriteWithMultiThreadsReadWrittenPayloads)
{  //
  TestFixture::VerifyWrite();
}

TYPED_TEST(BzTreeFixture, InsertWithMultiThreadsReadInsertedPayloads)
{  //
  TestFixture::VerifyInsert();
}

TYPED_TEST(BzTreeFixture, UpdateWithMultiThreadsReadUpdatedPayloads)
{  //
  TestFixture::VerifyUpdate();
}

TYPED_TEST(BzTreeFixture, DeleteWithMultiThreadsReadFailWithDeletedKeys)
{  //
  TestFixture::VerifyDelete();
}

}  // namespace dbgroup::index::bztree::test
