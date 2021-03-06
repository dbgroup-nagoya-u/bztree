// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include "common.hpp"

namespace dbgroup::index::bztree
{
/**
 * @brief A class to represent returning records of BzTree API.
 *
 * This class template deals with fixed-length keys and payloads as targets to be stored.
 *
 * @tparam Key a fixed-length key
 * @tparam Payload a fixed-length payload
 */
template <class Key, class Payload>
class Record
{
 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  /// a key stored in this record
  Key key_;

  /// a payload stored in this record
  Payload payload_;

  /*################################################################################################
   * Internal constructors
   *##############################################################################################*/

  /**
   * @brief Construct a new \c Record object.
   *
   * @param src_addr a source address to copy record data
   */
  explicit Record(const void* src_addr)
  {
    // keys and payloads must be trivially copyable
    static_assert(std::is_trivially_copyable_v<Key>);
    static_assert(std::is_trivially_copyable_v<Payload>);

    // this template deals with fixed-length keys and payloads
    static_assert(!std::is_pointer_v<Key>);
    static_assert(!std::is_pointer_v<Payload>);

    // a record itself must be trivially copyable
    static_assert(std::is_trivially_copyable_v<Record>);

    memcpy(this, src_addr, GetKeyLength() + GetPayloadLength());
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

  /**
   * @brief A public builder API to create a new \c Record object.
   *
   * @param src_addr a source address to copy the key and payload of a record
   * @param key_length the length of a key
   * @param payload_length the length of a payload
   * @return a \c std::unique_ptr of Record
   */
  static Record*
  Create(  //
      const void* src_addr,
      [[maybe_unused]] const size_t key_length,
      [[maybe_unused]] const size_t payload_length)
  {
    return new Record{src_addr};
  }

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  /**
   * @return a stored key
   */
  constexpr Key
  GetKey() const
  {
    return key_;
  }

  /**
   * @return a stored payload
   */
  constexpr Payload
  GetPayload() const
  {
    return payload_;
  }

  /**
   * @return the length of a stored kay
   */
  constexpr size_t
  GetKeyLength() const
  {
    return sizeof(Key);
  }

  /**
   * @return the length of a stored payload
   */
  constexpr size_t
  GetPayloadLength() const
  {
    return sizeof(Payload);
  }
};

/**
 * @brief A class to represent returning records of BzTree API.
 *
 * This class template deals with variable-length keys and fixed-length payloads as
 * targets to be stored.
 *
 * @tparam Key a variable-length key
 * @tparam Payload a fixed-length payload
 */
template <class Key, class Payload>
class Record<Key*, Payload>
{
 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  /// the length of a stored key
  const size_t key_length_;

  /// a record in binary format
  std::byte record_[];

  /*################################################################################################
   * Internal constructors
   *##############################################################################################*/

  /**
   * @brief Construct a new \c Record object.
   *
   * @param src_addr a source address to copy record data
   */
  Record(  //
      const void* src_addr,
      const size_t key_length)
      : key_length_{key_length}
  {
    // keys and payloads must be trivially copyable
    static_assert(std::is_trivially_copyable_v<Key>);
    static_assert(std::is_trivially_copyable_v<Payload>);

    // this template deals with fixed-length payloads
    static_assert(!std::is_pointer_v<Payload>);

    // a record itself must be trivially copyable
    static_assert(std::is_trivially_copyable_v<Record>);

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

  /**
   * @brief A public builder API to create a new \c Record object.
   *
   * @param src_addr a source address to copy the key and payload of a record
   * @param key_length the length of a key
   * @param payload_length the length of a payload
   * @return a \c std::unique_ptr of Record
   */
  static Record*
  Create(  //
      const void* src_addr,
      const size_t key_length,
      [[maybe_unused]] const size_t payload_length)
  {
    auto page = malloc(key_length + sizeof(Payload));
    return new (page) Record{src_addr, key_length};
  }

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  /**
   * @return an address of a stored key
   */
  constexpr Key*
  GetKey() const
  {
    return CastTarget<Key*>(record_);
  }

  /**
   * @return a stored payload
   */
  constexpr Payload
  GetPayload() const
  {
    return *CastTarget<Payload*>(record_ + GetKeyLength());
  }

  /**
   * @return the length of a stored kay
   */
  constexpr size_t
  GetKeyLength() const
  {
    return key_length_;
  }

  /**
   * @return the length of a stored payload
   */
  constexpr size_t
  GetPayloadLength() const
  {
    return sizeof(Payload);
  }
};

/**
 * @brief A class to represent returning records of BzTree API.
 *
 * This class template deals with fixed-length keys and variable-length payloads as
 * targets to be stored.
 *
 * @tparam Key a fixed-length key
 * @tparam Payload a variable-length payload
 */
template <class Key, class Payload>
class Record<Key, Payload*>
{
 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  /// the length of a stored payload
  const size_t payload_length_;

  /// a record in binary format
  std::byte record_[];

  /*################################################################################################
   * Internal constructors
   *##############################################################################################*/

  /**
   * @brief Construct a new \c Record object.
   *
   * @param src_addr a source address to copy record data
   */
  Record(  //
      const void* src_addr,
      const size_t payload_length)
      : payload_length_{payload_length}
  {
    // keys and payloads must be trivially copyable
    static_assert(std::is_trivially_copyable_v<Key>);
    static_assert(std::is_trivially_copyable_v<Payload>);

    // this template deals with fixed-length keys
    static_assert(!std::is_pointer_v<Key>);

    // a record itself must be trivially copyable
    static_assert(std::is_trivially_copyable_v<Record>);

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

  /**
   * @brief A public builder API to create a new \c Record object.
   *
   * @param src_addr a source address to copy the key and payload of a record
   * @param key_length the length of a key
   * @param payload_length the length of a payload
   * @return a \c std::unique_ptr of Record
   */
  static Record*
  Create(  //
      const void* src_addr,
      [[maybe_unused]] const size_t key_length,
      const size_t payload_length)
  {
    auto page = malloc(sizeof(Key) + payload_length);
    return new (page) Record{src_addr, payload_length};
  }

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  /**
   * @return a stored key
   */
  constexpr Key
  GetKey() const
  {
    return *CastTarget<Key*>(record_);
  }

  /**
   * @return an address of a stored payload
   */
  constexpr Payload*
  GetPayload() const
  {
    return CastTarget<Payload*>(record_ + GetKeyLength());
  }

  /**
   * @return the length of a stored kay
   */
  constexpr size_t
  GetKeyLength() const
  {
    return sizeof(Key);
  }

  /**
   * @return the length of a stored payload
   */
  constexpr size_t
  GetPayloadLength() const
  {
    return payload_length_;
  }
};

/**
 * @brief A class to represent returning records of BzTree API.
 *
 * This class template deals with variable-length keys and variable-length payloads as
 * targets to be stored.
 *
 * @tparam Key a variable-length key
 * @tparam Payload a variable-length payload
 */
template <class Key, class Payload>
class Record<Key*, Payload*>
{
 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  /// the length of a stored key
  const size_t key_length_;

  /// the length of a stored payload
  const size_t payload_length_;

  /// a record in binary format
  std::byte record_[];

  /*################################################################################################
   * Internal constructors
   *##############################################################################################*/

  /**
   * @brief Construct a new \c Record object.
   *
   * @param src_addr a source address to copy record data
   */
  Record(  //
      const void* src_addr,
      const size_t key_length,
      const size_t payload_length)
      : key_length_{key_length}, payload_length_{payload_length}
  {
    // keys and payloads must be trivially copyable
    static_assert(std::is_trivially_copyable_v<Key>);
    static_assert(std::is_trivially_copyable_v<Payload>);

    // a record itself must be trivially copyable
    static_assert(std::is_trivially_copyable_v<Record>);

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

  /**
   * @brief A public builder API to create a new \c Record object.
   *
   * @param src_addr a source address to copy the key and payload of a record
   * @param key_length the length of a key
   * @param payload_length the length of a payload
   * @return a \c std::unique_ptr of Record
   */
  static Record*
  Create(  //
      const void* src_addr,
      const size_t key_length,
      const size_t payload_length)
  {
    auto page = malloc(key_length + payload_length);
    return new (page) Record{src_addr, key_length, payload_length};
  }

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  /**
   * @return an address of a stored key
   */
  constexpr Key*
  GetKey() const
  {
    return CastTarget<Key*>(record_);
  }

  /**
   * @return an address of a stored payload
   */
  constexpr Payload*
  GetPayload() const
  {
    return CastTarget<Payload*>(record_ + GetKeyLength());
  }

  /**
   * @return the length of a stored kay
   */
  constexpr size_t
  GetKeyLength() const
  {
    return key_length_;
  }

  /**
   * @return the length of a stored payload
   */
  constexpr size_t
  GetPayloadLength() const
  {
    return payload_length_;
  }
};

}  // namespace dbgroup::index::bztree
