// Copyright (c) DB Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include <cstddef>
#include <cstdint>

using Key = uint64_t;
using Payload = uint64_t;
constexpr size_t kKeyLength = sizeof(Key);
constexpr size_t kPayloadLength = sizeof(Payload);

#include "leaf_node_test.hpp"

namespace dbgroup::index::bztree
{
void
LeafNodeFixture::SetUp()
{
  for (size_t index = 0; index < kKeyNumForTest; index++) {
    keys[index] = index + 1;
    payloads[index] = index + 1;
  }
  key_null = 0;
  payload_null = 0;
}

void
LeafNodeFixture::TearDown()
{
}

}  // namespace dbgroup::index::bztree
