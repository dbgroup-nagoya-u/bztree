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

#include <memory>

#include "common.hpp"

namespace dbgroup::index::bztree
{
/**
 * @brief A class to represent return records of BzTree API.
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

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  /**
   * @brief Construct a new object.
   *
   * @param src_addr a source address to copy record data
   */
  constexpr explicit Record(const void* src_addr)
  {
    // keys and payloads must be trivially copyable
    static_assert(std::is_trivially_copyable_v<Key>);
    static_assert(std::is_trivially_copyable_v<Payload>);

    // this record is trivially copyable because keys and payloads have fixed-length
    static_assert(std::is_trivially_copyable_v<Record>);

    memcpy(&key_, src_addr, GetKeyLength());
    memcpy(&payload_, static_cast<char*>(const_cast<void*>(src_addr)) + GetKeyLength(),
           GetPayloadLength());
  }

  ~Record() = default;
  constexpr Record(const Record&) = default;
  constexpr Record& operator=(const Record&) = default;
  constexpr Record(Record&&) = default;
  constexpr Record& operator=(Record&&) = default;

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
 * @brief A class to represent return records of BzTree API.
 *
 * This class template deals with variable-length keys and fixed-length payloads as
 * targets to be stored.
 *
 * @tparam Payload a fixed-length payload
 */
template <class Payload>
class VarKeyRecord
{
 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  /// the length of a stored key
  const size_t key_length_;

  /// a payload stored in this record
  Payload payload_;

  /// a key stored in this record
  char key_[];

  /*################################################################################################
   * Internal constructors
   *##############################################################################################*/

  /**
   * @brief Construct a new object.
   *
   * @param src_addr a source address to copy record data
   * @param key_length the length of a key
   */
  constexpr VarKeyRecord(  //
      const void* src_addr,
      const size_t key_length)
      : key_length_{key_length}
  {
    // payloads must be trivially copyable
    static_assert(std::is_trivially_copyable_v<Payload>);

    memcpy(&key_, src_addr, GetKeyLength());
    memcpy(&payload_, static_cast<char*>(const_cast<void*>(src_addr)) + GetKeyLength(),
           GetPayloadLength());
  }

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  ~VarKeyRecord() = default;
  VarKeyRecord(const VarKeyRecord&) = delete;
  VarKeyRecord& operator=(const VarKeyRecord&) = delete;
  VarKeyRecord(VarKeyRecord&&) = delete;
  VarKeyRecord& operator=(VarKeyRecord&&) = delete;

  /*################################################################################################
   * Public builders
   *##############################################################################################*/

  /**
   * @brief A public builder API to create a new \c VarKeyRecord object.
   *
   * @param src_addr a source address to copy the key and payload of a record
   * @param key_length the length of a key
   * @return a \c std::unique_ptr of \c VarKeyRecord
   */
  static constexpr std::unique_ptr<VarKeyRecord>
  Create(  //
      const void* src_addr,
      const size_t key_length)
  {
    auto page = malloc(key_length + sizeof(Payload) + sizeof(size_t));
    return std::unique_ptr<VarKeyRecord>(new (page) VarKeyRecord{src_addr, key_length});
  }

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  /**
   * @return a stored key
   */
  constexpr char*
  GetKey()
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
 * @brief A class to represent return records of BzTree API.
 *
 * This class template deals with fixed-length keys and variable-length payloads as
 * targets to be stored.
 *
 * @tparam Key a fixed-length key
 */
template <class Key>
class VarPayloadRecord
{
 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  /// the length of a stored payload
  const size_t payload_length_;

  /// a key stored in this record
  Key key_;

  /// a payload stored in this record
  char payload_[];

  /*################################################################################################
   * Internal constructors
   *##############################################################################################*/

  /**
   * @brief Construct a new object.
   *
   * @param src_addr a source address to copy record data
   * @param payload_length the length of a payload
   */
  constexpr VarPayloadRecord(  //
      const void* src_addr,
      const size_t payload_length)
      : payload_length_{payload_length}
  {
    // keys must be trivially copyable
    static_assert(std::is_trivially_copyable_v<Key>);

    memcpy(&key_, src_addr, GetKeyLength());
    memcpy(&payload_, static_cast<char*>(const_cast<void*>(src_addr)) + GetKeyLength(),
           GetPayloadLength());
  }

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  ~VarPayloadRecord() = default;
  VarPayloadRecord(const VarPayloadRecord&) = delete;
  VarPayloadRecord& operator=(const VarPayloadRecord&) = delete;
  VarPayloadRecord(VarPayloadRecord&&) = delete;
  VarPayloadRecord& operator=(VarPayloadRecord&&) = delete;

  /*################################################################################################
   * Public builders
   *##############################################################################################*/

  /**
   * @brief A public builder API to create a new \c VarPayloadRecord object.
   *
   * @param src_addr a source address to copy the key and payload of a record
   * @param payload_length the length of a payload
   * @return a \c std::unique_ptr of \c VarPayloadRecord
   */
  static constexpr std::unique_ptr<VarPayloadRecord>
  Create(  //
      const void* src_addr,
      const size_t payload_length)
  {
    auto page = malloc(sizeof(Key) + payload_length + sizeof(size_t));
    return std::unique_ptr<VarPayloadRecord>(new (page) VarPayloadRecord{src_addr, payload_length});
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
  constexpr char*
  GetPayload()
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
    return payload_length_;
  }
};

/**
 * @brief A class to represent return records of BzTree API.
 *
 * This class deals with variable-length keys and variable-length payloads as targets to
 * be stored.
 */
class VarRecord
{
 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  /// the length of a stored key
  const size_t key_length_;

  /// the length of a stored payload
  const size_t payload_length_;

  /// a key and payload pair
  char record_[];

  /*################################################################################################
   * Internal constructors
   *##############################################################################################*/

  /**
   * @brief Construct a new object.
   *
   * @param src_addr a source address to copy record data
   * @param key_length the length of a key
   * @param payload_length the length of a payload
   */
  VarRecord(  //
      const void* src_addr,
      const size_t key_length,
      const size_t payload_length)
      : key_length_{key_length}, payload_length_{payload_length}
  {
    memcpy(record_, src_addr, GetKeyLength() + GetPayloadLength());
  }

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  ~VarRecord() = default;
  VarRecord(const VarRecord&) = delete;
  VarRecord& operator=(const VarRecord&) = delete;
  VarRecord(VarRecord&&) = delete;
  VarRecord& operator=(VarRecord&&) = delete;

  /*################################################################################################
   * Public builders
   *##############################################################################################*/

  /**
   * @brief A public builder API to create a new \c VarRecord object.
   *
   * @param src_addr a source address to copy the key and payload of a record
   * @param key_length the length of a key
   * @param payload_length the length of a payload
   * @return a \c std::unique_ptr of \c VarRecord
   */
  static std::unique_ptr<VarRecord>
  Create(  //
      const void* src_addr,
      const size_t key_length,
      const size_t payload_length)
  {
    constexpr size_t header_length = 2 * sizeof(size_t);
    auto page = malloc(key_length + payload_length + header_length);
    return std::unique_ptr<VarRecord>(new (page) VarRecord{src_addr, key_length, payload_length});
  }

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  /**
   * @return a stored key
   */
  constexpr char*
  GetKey()
  {
    return record_;
  }

  /**
   * @return a stored payload
   */
  constexpr char*
  GetPayload()
  {
    return record_ + key_length_;
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
