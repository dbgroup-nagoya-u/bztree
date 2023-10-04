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

// external sources
#include "index_fixture_multi_thread.hpp"

namespace dbgroup::atomic::mwcas
{
/**
 * @brief Specialization to enable MwCAS to swap our sample class.
 *
 */
template <>
constexpr auto
CanMwCAS<MyClass>()  //
    -> bool
{
  return true;
}

}  // namespace dbgroup::atomic::mwcas

namespace dbgroup::index::test
{
/*######################################################################################
 * Preparation for typed testing
 *####################################################################################*/

template <class K, class V, class C>
using BzTree = ::dbgroup::index::bztree::BzTree<K, V, C>;

using TestTargets = ::testing::Types<  //
    IndexInfo<BzTree, UInt8, Int8>     // fixed-length keys
    >;
TYPED_TEST_SUITE(IndexMultiThreadFixture, TestTargets);

/*######################################################################################
 * Unit test definitions
 *####################################################################################*/

TYPED_TEST(IndexMultiThreadFixture, ReverseWriteWithUniqueKeysSucceed)
{
  TestFixture::VerifyWritesWith(!kWriteTwice, !kWithDelete, kReverse);
}

TYPED_TEST(IndexMultiThreadFixture, ReverseWriteWithDuplicateKeysSucceed)
{
  TestFixture::VerifyWritesWith(kWriteTwice, !kWithDelete, kReverse);
}

TYPED_TEST(IndexMultiThreadFixture, ReverseWriteWithDeletedKeysSucceed)
{
  TestFixture::VerifyWritesWith(kWriteTwice, kWithDelete, kReverse);
}

TYPED_TEST(IndexMultiThreadFixture, ReverseInsertWithUniqueKeysSucceed)
{
  TestFixture::VerifyInsertsWith(!kWriteTwice, !kWithDelete, kReverse);
}

TYPED_TEST(IndexMultiThreadFixture, ReverseInsertWithDuplicateKeysFail)
{
  TestFixture::VerifyInsertsWith(kWriteTwice, !kWithDelete, kReverse);
}

TYPED_TEST(IndexMultiThreadFixture, ReverseInsertWithDeletedKeysSucceed)
{
  TestFixture::VerifyInsertsWith(kWriteTwice, kWithDelete, kReverse);
}

TYPED_TEST(IndexMultiThreadFixture, ReverseUpdateWithDuplicateKeysSucceed)
{
  TestFixture::VerifyUpdatesWith(kWithWrite, !kWithDelete, kReverse);
}

TYPED_TEST(IndexMultiThreadFixture, ReverseUpdateWithNotInsertedKeysFail)
{
  TestFixture::VerifyUpdatesWith(!kWithWrite, !kWithDelete, kReverse);
}

TYPED_TEST(IndexMultiThreadFixture, ReverseUpdateWithDeletedKeysFail)
{
  TestFixture::VerifyUpdatesWith(kWithWrite, kWithDelete, kReverse);
}

TYPED_TEST(IndexMultiThreadFixture, ReverseDeleteWithDuplicateKeysSucceed)
{
  TestFixture::VerifyDeletesWith(kWithWrite, !kWithDelete, kReverse);
}

TYPED_TEST(IndexMultiThreadFixture, ReverseDeleteWithNotInsertedKeysFail)
{
  TestFixture::VerifyDeletesWith(!kWithWrite, !kWithDelete, kReverse);
}

TYPED_TEST(IndexMultiThreadFixture, ReverseDeleteWithDeletedKeysFail)
{
  TestFixture::VerifyDeletesWith(kWithWrite, kWithDelete, kReverse);
}

}  // namespace dbgroup::index::test
