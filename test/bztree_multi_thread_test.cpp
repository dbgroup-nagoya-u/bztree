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
#include "external/index-fixtures/index_fixture_multi_thread.hpp"

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

using TestTargets = ::testing::Types<      //
    IndexInfo<BzTree, UInt8, UInt8>,       // fixed-length keys
    IndexInfo<BzTree, UInt8, Int8>,        // fixed-length keys with append-mode
    IndexInfo<BzTree, UInt4, UInt8>,       // small keys
    IndexInfo<BzTree, UInt4, Int8>,        // small keys with append-mode
    IndexInfo<BzTree, UInt8, UInt4>,       // small payloads with append-mode
    IndexInfo<BzTree, UInt4, UInt4>,       // small keys/payloads with append-mode
    IndexInfo<BzTree, Var, UInt8>,         // variable-length keys
    IndexInfo<BzTree, Var, Int8>,          // variable-length keys with append-mode
    IndexInfo<BzTree, Ptr, Ptr>,           // pointer keys/payloads
    IndexInfo<BzTree, Original, Original>  // original class keys/payloads
    >;
TYPED_TEST_SUITE(IndexMultiThreadFixture, TestTargets);

/*######################################################################################
 * Unit test definitions
 *####################################################################################*/

#include "external/index-fixtures/index_fixture_multi_thread_test_definitions.hpp"

}  // namespace dbgroup::index::test
