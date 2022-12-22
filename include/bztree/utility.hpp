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
 * Global constants
 *####################################################################################*/

/// the default time interval for garbage collection [us].
constexpr size_t kDefaultGCTime = 10000;

/// the default number of worker threads for garbage collection.
constexpr size_t kDefaultGCThreadNum = 1;

/// a flag for indicating closed intervals
constexpr bool kClosed = true;

/// a flag for indicating closed intervals
constexpr bool kOpen = false;

/*######################################################################################
 * Utility enum and classes
 *####################################################################################*/

/**
 * @brief Return codes for BzTree.
 *
 */
enum ReturnCode {
  kSuccess = 0,
  kKeyNotExist = -3,
  kKeyExist,
  kNodeNotExist
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
IsVarLenData()  //
    -> bool
{
  if constexpr (std::is_same_v<T, char *> || std::is_same_v<T, std::byte *>) {
    return true;
  } else {
    return false;
  }
}

/*######################################################################################
 * Tuning parameters for BzTree
 *####################################################################################*/

/// The page size of each node.
constexpr size_t kPageSize = BZTREE_PAGE_SIZE;

/// Invoking consolidation if the number of delta records exceeds this threshold.
constexpr size_t kMaxDeltaRecNum = BZTREE_MAX_DELTA_RECORD_NUM;

/// Invoking consolidation if the size of deleted space exceeds this threshold.
constexpr size_t kMaxDeletedSpaceSize = BZTREE_MAX_DELETED_SPACE_SIZE;

/// Invoking a split-operation if the size of free space falls below this threshold.
constexpr size_t kMinFreeSpaceSize = BZTREE_MIN_FREE_SPACE_SIZE;

/// Invoking a merge-operation if the size of a consolidated node falls below this threshold.
constexpr size_t kMinNodeSize = BZTREE_MIN_NODE_SIZE;

/// Canceling a merge-operation if the size of a merged node exceeds this threshold.
constexpr size_t kMaxMergedSize = BZTREE_MAX_MERGED_SIZE;

/// the maximun size of variable-length data.
constexpr size_t kMaxVarDataSize = BZTREE_MAX_VARIABLE_DATA_SIZE;

}  // namespace dbgroup::index::bztree

#endif  // BZTREE_UTILITY_HPP
