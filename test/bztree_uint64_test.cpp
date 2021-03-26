// Copyright (c) DB Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

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
