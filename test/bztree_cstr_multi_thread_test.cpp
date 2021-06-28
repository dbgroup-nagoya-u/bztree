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

#include "bztree/components/common.hpp"

using Key = char*;
using Payload = char*;
using Compare = dbgroup::index::bztree::CompareAsCString;
using PayloadComparator = dbgroup::index::bztree::CompareAsCString;
constexpr size_t kKeyLength = 7;
constexpr size_t kPayloadLength = 7;

#include "bztree_multi_thread_test.hpp"

namespace dbgroup::index::bztree
{
void
BzTreeFixture::SetUp()
{
  for (size_t index = 0; index < kKeyNumForTest; ++index) {
    auto key = new char[kKeyLength];
    auto payload = new char[kPayloadLength];
    snprintf(key, kKeyLength, "%06lu", index + 1);
    snprintf(payload, kPayloadLength, "%06lu", index + 1);

    keys[index] = key;
    payloads[index] = payload;
  }
}

void
BzTreeFixture::TearDown()
{
  for (size_t index = 0; index < kKeyNumForTest; ++index) {
    delete[] keys[index];
    delete[] payloads[index];
  }
}

}  // namespace dbgroup::index::bztree
