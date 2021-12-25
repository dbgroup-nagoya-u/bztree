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

#ifndef BZTREE_UTILITY_HPP
#define BZTREE_UTILITY_HPP

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <type_traits>

#include "mwcas/utility.hpp"

namespace dbgroup::index::bztree
{
/*######################################################################################
 * Utility enum and classes
 *####################################################################################*/

/**
 * @brief Return codes for BzTree.
 *
 */
enum ReturnCode
{
  kSuccess = 0,
  kKeyNotExist = -2,
  kKeyExist
};

/**
 * @brief Compare binary keys as CString. The end of every key must be '\\0'.
 *
 */
struct CompareAsCString {
  constexpr auto
  operator()(const void *a, const void *b) const noexcept  //
      -> bool
  {
    if (a == nullptr) return false;
    if (b == nullptr) return true;
    return strcmp(static_cast<const char *>(a), static_cast<const char *>(b)) < 0;
  }
};

/**
 * @tparam T a target class.
 * @retval true if a target class is variable-length data.
 * @retval false if a target class is static-length data.
 */
template <class T>
constexpr auto
IsVariableLengthData()  //
    -> bool
{
  static_assert(std::is_trivially_copyable_v<T>);
  return false;
}

/*######################################################################################
 * Tuning parameters for BzTree
 *####################################################################################*/

/// Assumes that one word is represented by 8 bytes
constexpr size_t kWordLength = 8;

/// Header length in bytes.
constexpr size_t kHeaderLength = 2 * kWordLength;

/// the maximum alignment of keys/payloads assumed in this library
constexpr size_t kMaxAlignment = 16;

#ifdef BZTREE_PAGE_SIZE
/// The page size of each node
constexpr size_t kPageSize = BZTREE_PAGE_SIZE;
#else
constexpr size_t kPageSize = 8192;
#endif

/// check whether the specified page size is valid
static_assert(kPageSize % kWordLength == 0);

#ifdef BZTREE_MAX_UNSORTED_REC_NUM
/// Invoking consolidation if the number of unsorted records exceeds this threshold
constexpr size_t kMaxUnsortedRecNum = BZTREE_MAX_UNSORTED_REC_NUM;
#else
/// Invoking consolidation if the number of unsorted records exceeds this threshold
constexpr size_t kMaxUnsortedRecNum = kPageSize >> 7UL;
#endif

#ifdef BZTREE_MAX_DELETED_SPACE_SIZE
/// Invoking consolidation if the size of deleted records exceeds this threshold
constexpr size_t kMaxDeletedSpaceSize = BZTREE_MAX_DELETED_SPACE_SIZE;
#else
/// Invoking consolidation if the size of deleted records exceeds this threshold
constexpr size_t kMaxDeletedSpaceSize = 0.25 * kPageSize;
#endif

#ifdef BZTREE_MIN_FREE_SPACE_SIZE
/// Invoking a split if the size of free space in a node exceeds this threshold
constexpr size_t kMinFreeSpaceSize = BZTREE_MIN_FREE_SPACE_SIZE;
#else
/// Invoking a split if the size of free space in a node exceeds this threshold
constexpr size_t kMinFreeSpaceSize = kMaxUnsortedRecNum * (3 * kWordLength);
#endif

#ifdef BZTREE_MIN_CONSOLIDATED_SIZE
/// Invoking merging if the node size falls below this threshold
constexpr size_t kMinConsolidatedSize = BZTREE_MIN_CONSOLIDATED_SIZE;
#else
/// Invoking merging if the node size falls below this threshold
constexpr size_t kMinConsolidatedSize = kHeaderLength + kMinFreeSpaceSize;
#endif

#ifdef BZTREE_MAX_MERGED_SIZE
/// Canceling merging if the size of a merged node exceeds this threshold
constexpr size_t kMaxMergedSize = BZTREE_MAX_MERGED_SIZE;
#else
/// Canceling merging if the size of a merged node exceeds this threshold
constexpr size_t kMaxMergedSize = kPageSize - (2 * kMinFreeSpaceSize);
#endif

}  // namespace dbgroup::index::bztree

#endif  // BZTREE_UTILITY_HPP
