// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <mwcas/mwcas.h>

#include <cassert>
#include <cstring>
#include <memory>
#include <sstream>

namespace bztree
{
/*--------------------------------------------------------------------------------------------------
 * public utility: divide `utility.h` when reconstruct directory structure
 *------------------------------------------------------------------------------------------------*/

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

constexpr uintptr_t
PayloadToPtr(const void *payload)
{
  return static_cast<uintptr_t>(*static_cast<uint64_t *>(const_cast<void *>(payload)));
}

template <class T>
constexpr T *
CastPayload(const void *payload)
{
  return static_cast<T *>(reinterpret_cast<void *>(PayloadToPtr(payload)));
}

template <class To, class From>
constexpr To
BitCast(const From *obj)
{
  return static_cast<To>(static_cast<void *>(const_cast<From *>(obj)));
}

/**
 * @brief Compare binary keys as C_String. The end of every key must be '\\0'.
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

/**
 * @brief Compare binary keys as `uint64_t key`. The length of every key must be 8 bytes.
 *
 */
struct CompareAsUInt64 {
  constexpr bool
  operator()(const void *a, const void *b) const noexcept
  {
    if (a == nullptr) {
      return false;
    } else if (b == nullptr) {
      return true;
    } else {
      return *static_cast<const uint64_t *>(a) < *static_cast<const uint64_t *>(b);
    }
  }
};

/**
 * @brief Compare binary keys as `int64_t key`. The length of every key must be 8 bytes.
 *
 */
struct CompareAsInt64 {
  constexpr bool
  operator()(const void *a, const void *b) const noexcept
  {
    if (a == nullptr) {
      return false;
    } else if (b == nullptr) {
      return true;
    } else {
      return *static_cast<const int64_t *>(a) < *static_cast<const int64_t *>(b);
    }
  }
};

/*--------------------------------------------------------------------------------------------------
 * Common constants and utility functions
 *------------------------------------------------------------------------------------------------*/

// this code assumes that one word is represented by 8 bytes.
constexpr size_t kWordLength = 8;

// header length in bytes
constexpr size_t kHeaderLength = 2 * kWordLength;

/**
 * @brief
 *
 * @tparam Compare
 * @param obj_1
 * @param obj_2
 * @param comp comparator
 * @return true if a specified objects are equivalent according to `comp` comparator
 * @return false otherwise
 */
template <class Compare>
constexpr bool
IsEqual(const void *obj_1, const void *obj_2, const Compare &comp)
{
  return !(comp(obj_1, obj_2) || comp(obj_2, obj_1));
}

/**
 * @brief
 *
 * @tparam Compare
 * @param key
 * @param begin_key
 * @param begin_is_closed
 * @param end_key
 * @param end_is_closed
 * @param comp
 * @return true if a specfied key is in an input interval
 * @return false
 */
template <class Compare>
constexpr bool
IsInRange(const void *key,
          const void *begin_key,
          const bool begin_is_closed,
          const void *end_key,
          const bool end_is_closed,
          const Compare &comp)
{
  if (begin_key != nullptr) {
    return (comp(begin_key, key) && comp(key, end_key))
           || (begin_is_closed && IsEqual(key, begin_key, comp))
           || (end_is_closed && IsEqual(key, end_key, comp));
  } else {
    return comp(key, end_key) || (end_is_closed && IsEqual(key, end_key, comp));
  }
}

/**
 * @brief Shift a memory address by byte offsets.
 *
 * @tparam T
 * @param ptr original address
 * @param offset
 * @return byte* shifted address
 */
constexpr void *
ShiftAddress(const void *ptr, const size_t offset)
{
  return static_cast<void *>(static_cast<std::byte *>(const_cast<void *>(ptr)) + offset);
}

constexpr bool
HaveSameAddress(const void *a, const void *b)
{
  return a == b;
}

struct PtrPayload {
  uintptr_t value : 61 = 0;

  constexpr explicit PtrPayload(const void *ptr) : value{reinterpret_cast<uintptr_t>(ptr)} {}

 private:
  uint64_t control : 3 = 0;
};

union PayloadUnion {
  PtrPayload payload;
  uint64_t int_payload;
  pmwcas::MwcTargetField<uint64_t> target_field;

  constexpr explicit PayloadUnion() : int_payload{0} {}
  constexpr explicit PayloadUnion(const uint64_t int_payload) : int_payload{int_payload} {}
  constexpr explicit PayloadUnion(const PtrPayload payload) : payload{payload} {}
  constexpr explicit PayloadUnion(const void *ptr) : payload{PtrPayload{ptr}} {}
};

}  // namespace bztree
