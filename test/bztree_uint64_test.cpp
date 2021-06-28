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

using Key = uint64_t;
using Payload = uint64_t;
using Compare = std::less<Key>;
using PayloadComparator = std::less<Payload>;
constexpr size_t kKeyLength = sizeof(Key);
constexpr size_t kPayloadLength = sizeof(Payload);

#include "bztree_test.hpp"

namespace dbgroup::index::bztree
{
void
BzTreeFixture::SetUp()
{
  for (size_t index = 0; index < kKeyNumForTest; ++index) {
    keys[index] = index + 1;
    payloads[index] = index + 1;
  }
  key_null = 0;
  payload_null = 0;
}

void
BzTreeFixture::TearDown()
{
}

}  // namespace dbgroup::index::bztree
