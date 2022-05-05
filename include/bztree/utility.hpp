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

/// Assumes that one word is represented by 8 bytes
constexpr size_t kWordSize = sizeof(uintptr_t);

/*######################################################################################
 * Tuning parameters for BzTree
 *####################################################################################*/

/// Header length in bytes.
constexpr size_t kHeaderLength = 3 * kWordSize;

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

// Check whether the specified page size is valid
static_assert(kPageSize % kWordSize == 0);
static_assert(kMaxVarDataSize * 2 < kPageSize);

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

/**
 * @brief A class to represent an entry for bulkloading.
 *
 * Note that if you use bulk-load API with variable-length keys/values, you may need to
 * implement a specialized one to embed your class in this. If variable-length data are
 * given as arguments, this class only retains the references to a given key/payload.
 *
 * @tparam Key a target key class.
 * @tparam Payload a target payload class.
 */
template <class Key, class Payload>
class BulkloadEntry
{
 public:
  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  constexpr BulkloadEntry(  //
      const Key key,
      const Payload payload,
      const size_t key_length = sizeof(Key),
      const size_t payload_length = sizeof(Payload))
      : key_{key}, payload_{payload}, key_length_{key_length}, payload_length_{payload_length}
  {
  }

  constexpr BulkloadEntry(const BulkloadEntry &) = default;
  constexpr auto operator=(const BulkloadEntry &) -> BulkloadEntry & = default;
  constexpr BulkloadEntry(BulkloadEntry &&) noexcept = default;
  constexpr auto operator=(BulkloadEntry &&) noexcept -> BulkloadEntry & = default;

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

  ~BulkloadEntry() = default;

  /*####################################################################################
   * Public getters
   *##################################################################################*/

  [[nodiscard]] constexpr auto
  GetKey() const  //
      -> const Key &
  {
    return key_;
  }

  [[nodiscard]] constexpr auto
  GetPayload() const  //
      -> const Payload &
  {
    return payload_;
  }

  [[nodiscard]] constexpr auto
  GetKeyLength() const  //
      -> size_t
  {
    return key_length_;
  }

  [[nodiscard]] constexpr auto
  GetPayloadLength() const  //
      -> size_t
  {
    return payload_length_;
  }

 private:
  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  Key key_{};

  Payload payload_{};

  size_t key_length_{};

  size_t payload_length_{};
};

}  // namespace dbgroup::index::bztree

#endif  // BZTREE_UTILITY_HPP
