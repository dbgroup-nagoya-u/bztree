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

#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>

namespace dbgroup::index::bztree
{
/**
 * @brief Return codes for BzTree.
 *
 */
enum ReturnCode
{
  kSuccess = 0,
  kScanInProgress,
  kKeyNotExist,
  kKeyExist
};

/**
 * @brief Compare binary keys as CString. The end of every key must be '\\0'.
 *
 */
struct CompareAsCString {
  constexpr bool
  operator()(const void *a, const void *b) const noexcept
  {
    if (a == nullptr) {
      return false;
    } else if (b == nullptr) {
      return true;
    } else {
      return strcmp(static_cast<const char *>(a), static_cast<const char *>(b)) < 0;
    }
  }
};

/// Assumes that one word is represented by 8 bytes
constexpr size_t kWordLength = 8;

/// Assumes that one word is represented by 8 bytes
constexpr size_t kCacheLineSize = 64;

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
constexpr size_t kMaxUnsortedRecNum = 64;
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

#ifdef BZTREE_MIN_SORTED_REC_NUM
/// Invoking merging if the number of sorted records falls below this threshold
constexpr size_t kMinSortedRecNum = BZTREE_MIN_SORTED_REC_NUM;
#else
/// Invoking merging if the number of sorted records falls below this threshold
constexpr size_t kMinSortedRecNum = 2 * kMaxUnsortedRecNum;
#endif

#ifdef BZTREE_MAX_MERGED_SIZE
/// Canceling merging if the size of a merged node exceeds this threshold
constexpr size_t kMaxMergedSize = BZTREE_MAX_MERGED_SIZE;
#else
/// Canceling merging if the size of a merged node exceeds this threshold
constexpr size_t kMaxMergedSize = kPageSize - (2 * kMinFreeSpaceSize);
#endif

}  // namespace dbgroup::index::bztree
