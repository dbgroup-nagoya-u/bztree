// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <memory>
#include <utility>

#include "common.hpp"

namespace dbgroup::index::bztree
{
template <class Key, class Payload>
class Record
{
 private:
  /*################################################################################################
   * Internal constructors
   *##############################################################################################*/

  Key key_;

  Payload payload_;

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  explicit Record(const void* src_addr)
  {
    static_assert(std::is_trivially_copyable_v<Key>);
    static_assert(std::is_trivially_copyable_v<Payload>);
    static_assert(!std::is_pointer_v<Key>);
    static_assert(!std::is_pointer_v<Payload>);

    memcpy(this, src_addr, GetKeyLength() + GetPayloadLength());
  }

  ~Record() = default;
  constexpr Record(const Record&) = default;
  constexpr Record& operator=(const Record&) = default;
  constexpr Record(Record&&) = default;
  constexpr Record& operator=(Record&&) = default;

  /*################################################################################################
   * Public builders
   *##############################################################################################*/

  static std::unique_ptr<Record>
  Create(  //
      const void* src_addr,
      [[maybe_unused]] const size_t key_length,
      [[maybe_unused]] const size_t payload_length)
  {
    return std::make_unique<Record>(src_addr);
  }

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  constexpr Key
  GetKey() const
  {
    return key_;
  }

  constexpr Payload
  GetPayload() const
  {
    return payload_;
  }

  constexpr size_t
  GetKeyLength() const
  {
    return sizeof(Key);
  }

  constexpr size_t
  GetPayloadLength() const
  {
    return sizeof(Payload);
  }
};

template <class Key, class Payload>
class Record<Key*, Payload>
{
 private:
  /*################################################################################################
   * Internal constructors
   *##############################################################################################*/

  const size_t key_length_;

  std::byte record_[];

  /*################################################################################################
   * Internal constructors
   *##############################################################################################*/

  Record(  //
      const void* src_addr,
      const size_t key_length)
      : key_length_{key_length}
  {
    static_assert(std::is_trivially_copyable_v<Key>);
    static_assert(std::is_trivially_copyable_v<Payload>);
    static_assert(!std::is_pointer_v<Payload>);

    memcpy(record_, src_addr, GetKeyLength() + GetPayloadLength());
  }

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  ~Record() = default;
  constexpr Record(const Record&) = default;
  constexpr Record& operator=(const Record&) = default;
  constexpr Record(Record&&) = default;
  constexpr Record& operator=(Record&&) = default;

  /*################################################################################################
   * Public builders
   *##############################################################################################*/

  static std::unique_ptr<Record>
  Create(  //
      const void* src_addr,
      const size_t key_length,
      [[maybe_unused]] const size_t payload_length)
  {
    auto page = malloc(key_length + sizeof(Payload));
    return std::unique_ptr<Record>(new (page) Record{src_addr, key_length});
  }

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  constexpr Key*
  GetKey() const
  {
    return CastTarget<Key*>(record_);
  }

  constexpr Payload
  GetPayload() const
  {
    return *CastTarget<Payload*>(record_ + GetKeyLength());
  }

  constexpr size_t
  GetKeyLength() const
  {
    return key_length_;
  }

  constexpr size_t
  GetPayloadLength() const
  {
    return sizeof(Payload);
  }
};

template <class Key, class Payload>
class Record<Key, Payload*>
{
 private:
  /*################################################################################################
   * Internal constructors
   *##############################################################################################*/

  const size_t payload_length_;

  std::byte record_[];

  /*################################################################################################
   * Internal constructors
   *##############################################################################################*/

  Record(  //
      const void* src_addr,
      const size_t payload_length)
      : payload_length_{payload_length}
  {
    static_assert(std::is_trivially_copyable_v<Key>);
    static_assert(std::is_trivially_copyable_v<Payload>);
    static_assert(!std::is_pointer_v<Key>);

    memcpy(record_, src_addr, GetKeyLength() + GetPayloadLength());
  }

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  ~Record() = default;
  constexpr Record(const Record&) = default;
  constexpr Record& operator=(const Record&) = default;
  constexpr Record(Record&&) = default;
  constexpr Record& operator=(Record&&) = default;

  /*################################################################################################
   * Public builders
   *##############################################################################################*/

  static std::unique_ptr<Record>
  Create(  //
      const void* src_addr,
      [[maybe_unused]] const size_t key_length,
      const size_t payload_length)
  {
    auto page = malloc(sizeof(Key) + payload_length);
    return std::unique_ptr<Record>(new (page) Record{src_addr, payload_length});
  }

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  constexpr Key
  GetKey() const
  {
    return *CastTarget<Key*>(record_);
  }

  constexpr Payload*
  GetPayload() const
  {
    return CastTarget<Payload*>(record_ + GetKeyLength());
  }

  constexpr size_t
  GetKeyLength() const
  {
    return sizeof(Key);
  }

  constexpr size_t
  GetPayloadLength() const
  {
    return payload_length_;
  }
};

template <class Key, class Payload>
class Record<Key*, Payload*>
{
 private:
  /*################################################################################################
   * Internal constructors
   *##############################################################################################*/

  const size_t key_length_;

  const size_t payload_length_;

  std::byte record_[];

  /*################################################################################################
   * Internal constructors
   *##############################################################################################*/

  Record(  //
      const void* src_addr,
      const size_t key_length,
      const size_t payload_length)
      : key_length_{key_length}, payload_length_{payload_length}
  {
    static_assert(std::is_trivially_copyable_v<Key>);
    static_assert(std::is_trivially_copyable_v<Payload>);

    memcpy(record_, src_addr, GetKeyLength() + GetPayloadLength());
  }

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  ~Record() = default;
  constexpr Record(const Record&) = default;
  constexpr Record& operator=(const Record&) = default;
  constexpr Record(Record&&) = default;
  constexpr Record& operator=(Record&&) = default;

  /*################################################################################################
   * Public builders
   *##############################################################################################*/

  static std::unique_ptr<Record>
  Create(  //
      const void* src_addr,
      const size_t key_length,
      const size_t payload_length)
  {
    auto page = malloc(key_length + payload_length);
    auto record = new (page) Record{src_addr, key_length, payload_length};
    return std::unique_ptr<Record>(record);
  }

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  constexpr Key*
  GetKey() const
  {
    return CastTarget<Key*>(record_);
  }

  constexpr Payload*
  GetPayload() const
  {
    return CastTarget<Payload*>(record_ + GetKeyLength());
  }

  constexpr size_t
  GetKeyLength() const
  {
    return key_length_;
  }

  constexpr size_t
  GetPayloadLength() const
  {
    return payload_length_;
  }
};

}  // namespace dbgroup::index::bztree
