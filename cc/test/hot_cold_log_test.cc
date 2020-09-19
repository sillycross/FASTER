// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include <cstdint>
#include <cstring>
#include <deque>
#include <thread>
#include <utility>
#include "gtest/gtest.h"

#include "core/faster.h"
#include "device/null_disk.h"

#include "test_types.h"

using namespace FASTER::core;
using FASTER::test::NonMovable;
using FASTER::test::NonCopyable;
using FASTER::test::FixedSizeKey;
using FASTER::test::SimpleAtomicValue;

namespace {

class Key : NonCopyable, NonMovable {
public:
    static uint32_t size(uint32_t key_length) {
      return static_cast<uint32_t>(sizeof(Key) + key_length);
    }

    static void Create(Key* dst, uint32_t key_length, uint8_t* key_data) {
      dst->key_length_ = key_length;
      memcpy(dst->buffer(), key_data, key_length);
    }

    /// Methods and operators required by the (implicit) interface:
    inline uint32_t size() const {
      return static_cast<uint32_t>(sizeof(Key) + key_length_);
    }
    inline KeyHash GetHash() const {
      return KeyHash(Utility::HashBytesUint8(buffer(), key_length_));
    }

    /// Comparison operators.
    inline bool operator==(const Key& other) const {
      if (this->key_length_ != other.key_length_) return false;
      return memcmp(buffer(), other.buffer(), key_length_) == 0;
    }
    inline bool operator!=(const Key& other) const {
      return !(*this == other);
    }

    uint32_t key_length_;

    inline const uint8_t* buffer() const {
      return reinterpret_cast<const uint8_t*>(this + 1);
    }
    inline uint8_t* buffer() {
      return reinterpret_cast<uint8_t*>(this + 1);
    }
};

class ShallowKey {
public:
    ShallowKey(uint8_t* key_data, uint32_t key_length)
        : key_length_(key_length), key_data_(key_data)
    { }

    inline uint32_t size() const {
      return Key::size(key_length_);
    }
    inline KeyHash GetHash() const {
      return KeyHash(Utility::HashBytesUint8(key_data_, key_length_));
    }
    inline void write_deep_key_at(Key* dst) const {
      Key::Create(dst, key_length_, key_data_);
    }
    /// Comparison operators.
    inline bool operator==(const Key& other) const {
      if (this->key_length_ != other.key_length_) return false;
      return memcmp(key_data_, other.buffer(), key_length_) == 0;
    }
    inline bool operator!=(const Key& other) const {
      return !(*this == other);
    }

    uint32_t key_length_;
    uint8_t* key_data_;
};

using Value = SimpleAtomicValue<uint8_t>;

class UpsertContext : public IAsyncContext {
public:
    typedef Key key_t;
    typedef Value value_t;

    UpsertContext(uint8_t* key, uint32_t key_length, uint8_t valueToStore)
            : key_{ key, key_length }, valueToStore_(valueToStore) {
    }

    /// Copy (and deep-copy) constructor.
    UpsertContext(const UpsertContext& other)
        : key_{ other.key_ }, valueToStore_(other.valueToStore_) {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const ShallowKey& key() const {
      return key_;
    }
    inline static constexpr uint32_t value_size() {
      return sizeof(value_t);
    }
    /// Non-atomic and atomic Put() methods.
    inline void Put(Value& value) {
      value.value = valueToStore_;
    }
    inline bool PutAtomic(Value& value) {
      value.atomic_value.store(valueToStore_);
      return true;
    }

protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      // In this particular test, the key content is always on the heap and always available,
      // so we don't need to copy the key content. If the key content were on the stack,
      // we would need to copy the key content to the heap as well
      //
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

private:
    ShallowKey key_;
    uint8_t valueToStore_;
};

class ReadContext : public IAsyncContext {
public:
    typedef Key key_t;
    typedef Value value_t;

    ReadContext(uint8_t* key, uint32_t key_length)
            : key_{ key, key_length } {
    }

    /// Copy (and deep-copy) constructor.
    ReadContext(const ReadContext& other)
            : key_{ other.key_ } {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const ShallowKey& key() const {
      return key_;
    }

    inline void Get(const Value& value) {
      output = value.value;
    }
    inline void GetAtomic(const Value& value) {
      output = value.atomic_value.load();
    }

protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

private:
    ShallowKey key_;
public:
    uint8_t output;
};

class DeleteContext : public IAsyncContext {
public:
    typedef Key key_t;
    typedef Value value_t;

    DeleteContext(uint8_t* key, uint32_t key_length)
            : key_{ key, key_length } {
    }

    /// Copy (and deep-copy) constructor.
    DeleteContext(const DeleteContext& other)
            : key_{ other.key_ } {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const ShallowKey& key() const {
      return key_;
    }

    inline static constexpr uint32_t value_size() {
      return Value::size();
    }

protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

private:
    ShallowKey key_;
public:
    uint8_t output;
};

class RmwContext : public IAsyncContext {
 private:
  ShallowKey key_;
 public:
  typedef Key key_t;
  typedef Value value_t;

  explicit RmwContext(uint8_t* key, uint32_t key_length)
    : key_{ key, key_length } {
  }

  inline const ShallowKey& key() const {
    return key_;
  }

  inline static constexpr uint32_t value_size() {
    return Value::size();
  }

  inline static constexpr uint32_t value_size(const Value& old_value) {
    return Value::size();
  }

  inline void RmwInitial(Value& value) {
    value.value = 1;
  }

  inline void RmwCopy(const Value& old_value, Value& value) {
    value.value = old_value.value * 2 + 1;
  }

  inline bool RmwAtomic(Value& value) {
    // Not supported: so that operation would allocate a new entry for the update.
    return false;
  }
 protected:
  /// The explicit interface requires a DeepCopy_Internal() implementation.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }
};

}   // anonymous namespace

TEST(HotColdLog, InMemoryTest) {
  FasterKv<Key, Value, FASTER::device::NullDisk, true /*hotColdLog*/> store { 1048576, 1073741824, "" };

  store.StartSession();

  // Insert.
  for(uint32_t idx = 1; idx < 256; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
        // In-memory test.
        ASSERT_TRUE(false);
    };

    // Create the key as a variable length array
    uint8_t* key = (uint8_t*) malloc(idx);
    for (size_t j = 0; j < idx; ++j) {
      key[j] = 42;
    }

    UpsertContext context{ key, idx , 42 };
    Status result = store.Upsert(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
  }
  for(uint32_t idx = 256; idx < 512; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
        // In-memory test.
        ASSERT_TRUE(false);
    };

    // Create the key as a variable length array
    uint8_t* key = (uint8_t*) malloc(idx);
    for (size_t j = 0; j < idx; ++j) {
      key[j] = 42;
    }

    UpsertContext context{ key, idx , 42 };
    Status result = store.GetColdLog()->Upsert(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
  }
  // Read.
  for(uint32_t idx = 1; idx < 512; ++idx) {
    // Create the key as a variable length array
    uint8_t* key = (uint8_t*) malloc(idx);
    for (size_t j = 0; j < idx; ++j) {
      key[j] = 42;
    }

    ReadContext context{ key, idx };

    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_EQ(Status::Ok, result);
    };

    Status result = store.Read(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
    ASSERT_EQ(42, context.output);
  }
  for(uint32_t idx = 1; idx < 128; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
        // In-memory test.
        ASSERT_TRUE(false);
    };

    // Create the key as a variable length array
    uint8_t* key = (uint8_t*) malloc(idx);
    for (size_t j = 0; j < idx; ++j) {
      key[j] = 42;
    }

    DeleteContext context{ key, idx };
    Status result = store.Delete(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
  }
  for(uint32_t idx = 1; idx < 64; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
        // In-memory test.
        ASSERT_TRUE(false);
    };

    // Create the key as a variable length array
    uint8_t* key = (uint8_t*) malloc(idx);
    for (size_t j = 0; j < idx; ++j) {
      key[j] = 42;
    }

    UpsertContext context{ key, idx , 66 };
    Status result = store.GetColdLog()->Upsert(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
  }
  // Read again.
  for(uint32_t idx = 1; idx < 512; ++idx) {
    // Create the key as a variable length array
    uint8_t* key = (uint8_t*) malloc(idx);
    for (size_t j = 0; j < idx; ++j) {
      key[j] = 42;
    }

    ReadContext context{ key, idx };

    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_EQ(Status::Ok, result);
    };

    Status result = store.Read(context, callback, 1);
    if (idx < 128)
    {
      ASSERT_EQ(Status::NotFound, result);
    }
    else
    {
      ASSERT_EQ(Status::Ok, result);
      ASSERT_EQ(42, context.output);
    }
  }

  for(uint32_t idx = 1; idx < 512; ++idx) {
    // Create the key as a variable length array
    uint8_t* key = (uint8_t*) malloc(idx);
    for (size_t j = 0; j < idx; ++j) {
      key[j] = 42;
    }

    RmwContext context{ key, idx };

    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_EQ(Status::Ok, result);
    };

    Status result = store.Rmw(context, callback, 1);
  }

  for(uint32_t idx = 1; idx < 512; ++idx) {
    // Create the key as a variable length array
    uint8_t* key = (uint8_t*) malloc(idx);
    for (size_t j = 0; j < idx; ++j) {
      key[j] = 42;
    }

    ReadContext context{ key, idx };

    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_EQ(Status::Ok, result);
    };

    Status result = store.Read(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
    if (idx < 128)
    {
      ASSERT_EQ(1, context.output);
    }
    else
    {
      ASSERT_EQ(85, context.output);
    }
  }

  store.StopSession();
}

template<bool useTwoLevelHashTable>
void TestHotColdLogOnDisk()
{
  using handler_t = FASTER::environment::QueueIoHandler;
  using disk_t = FASTER::device::FileSystemDisk<handler_t, 1073741824ull>;
  using store_t = FASTER::core::FasterKv<Key, Value, disk_t, true, useTwoLevelHashTable>;

  store_t store { 16777216, 192 * 1048576ULL, "storage", 0.4 /*mutableFraction*/ };

  store.StartSession();

  const int x_keyLenU64 = 500;

  // Insert.
  for(uint32_t idx = 0; idx < 100000; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
        assert(result == Status::Ok);
    };

    uint64_t* key = new uint64_t[x_keyLenU64];
    for (size_t j = 0; j < x_keyLenU64; ++j) {
      key[j] = idx;
    }

    UpsertContext context{ reinterpret_cast<uint8_t*>(key), x_keyLenU64 * 8 , 42 };
    Status result = store.Upsert(context, callback, 1);
    assert(result == Status::Ok || result == Status::Pending);
    store.CompletePending(true);
    store.GetColdLog()->CompletePending(true);

    if (idx % 10000 == 0)
    {
      printf("inserted %d/%d\n", int(idx), 100000);
    }
  }
  for(uint32_t idx = 100000; idx < 200000; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
        assert(result == Status::Ok);
    };

    uint64_t* key = new uint64_t[x_keyLenU64];
    for (size_t j = 0; j < x_keyLenU64; ++j) {
      key[j] = idx;
    }

    UpsertContext context{ reinterpret_cast<uint8_t*>(key), x_keyLenU64 * 8 , 42 };
    Status result = store.GetColdLog()->Upsert(context, callback, 1);
    assert(result == Status::Ok || result == Status::Pending);
    store.CompletePending(true);
    store.GetColdLog()->CompletePending(true);

    if (idx % 10000 == 0)
    {
      printf("inserted %d/%d\n", int(idx - 100000), 100000);
    }
  }
  // Read.
  int pending_cnt = 0;
  static int cb_call_cnt;
  cb_call_cnt = 0;
  for(uint32_t i = 0; i < 10000; ++i) {
    uint32_t idx = rand() % 200000;
    uint64_t* key = new uint64_t[x_keyLenU64];
    for (size_t j = 0; j < x_keyLenU64; ++j) {
      key[j] = idx;
    }

    ReadContext context{ reinterpret_cast<uint8_t*>(key), x_keyLenU64 * 8 };
    context.output = 0;

    auto callback = [](IAsyncContext* ctxt, Status result) {
      assert(result == Status::Ok);
      ReadContext* c = static_cast<ReadContext*>(ctxt);
      assert(c->output == 42);
      cb_call_cnt++;
    };

    Status result = store.Read(context, callback, 1);
    if (result == Status::Ok)
    {
      assert(context.output == 42);
    }
    else
    {
      pending_cnt++;
      assert(result == Status::Pending);
      store.CompletePending(true);
      store.GetColdLog()->CompletePending(true);
    }

    if (i % 1000 == 0)
    {
      printf("read %d/%d\n", int(i), 10000);
    }
  }
  printf("read: total %d pending codepath\n", pending_cnt);
  assert(cb_call_cnt == pending_cnt);

  cb_call_cnt = 0;
  pending_cnt = 0;
  for(uint32_t idx = 0; idx < 50000; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
        assert(result == Status::Ok);
        cb_call_cnt++;
    };

    uint64_t* key = new uint64_t[x_keyLenU64];
    for (size_t j = 0; j < x_keyLenU64; ++j) {
      key[j] = idx;
    }

    DeleteContext context{ reinterpret_cast<uint8_t*>(key), x_keyLenU64 * 8 };
    Status result = store.Delete(context, callback, 1);
    assert(result == Status::Ok || result == Status::Pending);
    if (result == Status::Pending)
    {
      pending_cnt++;
    }
    store.CompletePending(true);
    store.GetColdLog()->CompletePending(true);

    if (idx % 10000 == 0)
    {
      printf("deleted %d/%d\n", int(idx), 50000);
    }
  }
  assert(cb_call_cnt == pending_cnt);

  cb_call_cnt = 0;
  pending_cnt = 0;
  for(uint32_t idx = 0; idx < 25000; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
        assert(result == Status::Ok);
        cb_call_cnt++;
    };

    uint64_t* key = new uint64_t[x_keyLenU64];
    for (size_t j = 0; j < x_keyLenU64; ++j) {
      key[j] = idx;
    }

    UpsertContext context{ reinterpret_cast<uint8_t*>(key), x_keyLenU64 * 8, 66 };
    Status result = store.GetColdLog()->Upsert(context, callback, 1);
    assert(result == Status::Ok || result == Status::Pending);
    if (result == Status::Pending)
    {
      pending_cnt++;
    }
    store.CompletePending(true);
    store.GetColdLog()->CompletePending(true);

    if (idx % 5000 == 0)
    {
      printf("fake compacted %d/%d\n", int(idx), 25000);
    }
  }
  assert(cb_call_cnt == pending_cnt);

  cb_call_cnt = 0;
  pending_cnt = 0;
  for(uint32_t idx = 100000; idx < 150000; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
        assert(result == Status::Ok);
        cb_call_cnt++;
    };

    uint64_t* key = new uint64_t[x_keyLenU64];
    for (size_t j = 0; j < x_keyLenU64; ++j) {
      key[j] = idx;
    }

    DeleteContext context{ reinterpret_cast<uint8_t*>(key), x_keyLenU64 * 8 };
    Status result = store.Delete(context, callback, 1);
    assert(result == Status::Ok || result == Status::Pending);
    if (result == Status::Pending)
    {
      pending_cnt++;
    }
    store.CompletePending(true);
    store.GetColdLog()->CompletePending(true);

    if (idx % 10000 == 0)
    {
      printf("deleted %d/%d\n", int(idx - 100000), 50000);
    }
  }
  assert(pending_cnt == cb_call_cnt);

  cb_call_cnt = 0;
  pending_cnt = 0;
  for(uint32_t idx = 200000; idx < 200100; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
        assert(result == Status::NotFound);
        cb_call_cnt++;
    };

    uint64_t* key = new uint64_t[x_keyLenU64];
    for (size_t j = 0; j < x_keyLenU64; ++j) {
      key[j] = idx;
    }

    DeleteContext context{ reinterpret_cast<uint8_t*>(key), x_keyLenU64 * 8 };
    Status result = store.Delete(context, callback, 1);
    assert(result == Status::NotFound || result == Status::Pending);
    if (result == Status::Pending)
    {
      pending_cnt++;
    }
    store.CompletePending(true);
    store.GetColdLog()->CompletePending(true);
  }
  assert(pending_cnt == cb_call_cnt);

  pending_cnt = 0;
  cb_call_cnt = 0;
  for(uint32_t i = 0; i < 10000; ++i) {
    uint32_t idx = rand() % 200000;
    uint64_t* key = new uint64_t[x_keyLenU64];
    for (size_t j = 0; j < x_keyLenU64; ++j) {
      key[j] = idx;
    }

    ReadContext context{ reinterpret_cast<uint8_t*>(key), x_keyLenU64 * 8 };
    context.output = 0;

    static uint32_t queriedIdx;
    queriedIdx = idx;
    auto callback = [](IAsyncContext* ctxt, Status result) {
      if (queriedIdx < 50000 || (100000 <= queriedIdx && queriedIdx < 150000))
      {
        assert(result == Status::NotFound);
      }
      else
      {
        assert(result == Status::Ok);
        ReadContext* c = static_cast<ReadContext*>(ctxt);
        assert(c->output == 42);
      }
      cb_call_cnt++;
    };

    Status result = store.Read(context, callback, 1);
    if (result == Status::Ok || result == Status::NotFound)
    {
      if (idx < 50000 || (100000 <= idx && idx < 150000))
      {
        assert(result == Status::NotFound);
      }
      else
      {
        assert(result == Status::Ok);
        assert(context.output == 42);
      }
    }
    else
    {
      pending_cnt++;
      assert(result == Status::Pending);
      store.CompletePending(true);
      store.GetColdLog()->CompletePending(true);
    }

    if (i % 1000 == 0)
    {
      printf("read %d/%d\n", int(i), 10000);
    }
  }
  printf("read: total %d pending codepath\n", pending_cnt);
  assert(cb_call_cnt == pending_cnt);

  pending_cnt = 0;
  cb_call_cnt = 0;
  for(uint32_t idx = 0; idx < 200000; ++idx) {
    uint64_t* key = new uint64_t[x_keyLenU64];
    for (size_t j = 0; j < x_keyLenU64; ++j) {
      key[j] = idx;
    }

    RmwContext context{ reinterpret_cast<uint8_t*>(key), x_keyLenU64 * 8 };

    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_EQ(Status::Ok, result);
      cb_call_cnt++;
    };

    Status result = store.Rmw(context, callback, 1);
    if (result == Status::Pending)
    {
      pending_cnt++;
    }
    store.CompletePending(true);
    store.GetColdLog()->CompletePending(true);

    if (idx % 10000 == 0)
    {
      printf("rmw %d/%d\n", int(idx), 200000);
    }
  }
  printf("rmw: total %d pending codepath\n", pending_cnt);
  assert(cb_call_cnt == pending_cnt);

  pending_cnt = 0;
  cb_call_cnt = 0;
  for(uint32_t i = 0; i < 10000; ++i) {
    uint32_t idx = rand() % 200000;
    uint64_t* key = new uint64_t[x_keyLenU64];
    for (size_t j = 0; j < x_keyLenU64; ++j) {
      key[j] = idx;
    }

    ReadContext context{ reinterpret_cast<uint8_t*>(key), x_keyLenU64 * 8 };
    context.output = 0;

    static int queryIdx;
    queryIdx = idx;
    auto callback = [](IAsyncContext* ctxt, Status result) {
      assert(result == Status::Ok);
      ReadContext* c = static_cast<ReadContext*>(ctxt);
      if (queryIdx < 50000 || (100000 <= queryIdx && queryIdx < 150000))
      {
        assert(c->output == 1);
      } else {
        assert(c->output == 85);
      }
      cb_call_cnt++;
    };

    Status result = store.Read(context, callback, 1);
    if (result == Status::Ok)
    {
      if (queryIdx < 50000 || (100000 <= queryIdx && queryIdx < 150000))
      {
        assert(context.output == 1);
      } else {
        assert(context.output == 85);
      }
    }
    else
    {
      pending_cnt++;
      assert(result == Status::Pending);
      store.CompletePending(true);
      store.GetColdLog()->CompletePending(true);
    }

    if (i % 1000 == 0)
    {
      printf("read %d/%d\n", int(i), 10000);
    }
  }
  printf("read: total %d pending codepath\n", pending_cnt);
  assert(cb_call_cnt == pending_cnt);

  store.StopSession();
}

TEST(HotColdLog, OnDiskTest)
{
    TestHotColdLogOnDisk<false /*useTwoLevelht*/>();
}

TEST(TwoLevelHashTable, OnDiskTest)
{
    TestHotColdLogOnDisk<true /*useTwoLevelht*/>();
}

template<bool useTwoLevelHashTable>
void TestHotColdLogCompaction()
{
  using handler_t = FASTER::environment::QueueIoHandler;
  using disk_t = FASTER::device::FileSystemDisk<handler_t, 1073741824ull>;
  using store_t = FASTER::core::FasterKv<Key, Value, disk_t, true, useTwoLevelHashTable>;

  store_t store { 1048576, 192 * 1048576ULL, "storage", 0.4 /*mutableFraction*/ };

  store.StartSession();

  const int x_keyLenU64 = 500;

  // Insert.
  for(uint32_t idx = 0; idx < 200000; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
        assert(result == Status::Ok);
    };

    uint64_t* key = new uint64_t[x_keyLenU64];
    for (size_t j = 0; j < x_keyLenU64; ++j) {
      key[j] = idx;
    }

    UpsertContext context{ reinterpret_cast<uint8_t*>(key), x_keyLenU64 * 8 , 42 };
    Status result = store.Upsert(context, callback, 1);
    assert(result == Status::Ok || result == Status::Pending);
    store.CompletePending(true);
    store.GetColdLog()->CompletePending(true);

    if (idx % 10000 == 0)
    {
      printf("inserted %d/%d\n", int(idx), 200000);
    }
  }
  static std::atomic<bool> compactionCompleted(false);

  int pending_cnt = 0;
  static int cb_call_cnt;
  cb_call_cnt = 0;
  for(uint32_t i = 0; i < 10000; ++i) {
    uint32_t idx = rand() % 200000;
    uint64_t* key = new uint64_t[x_keyLenU64];
    for (size_t j = 0; j < x_keyLenU64; ++j) {
      key[j] = idx;
    }

    ReadContext context{ reinterpret_cast<uint8_t*>(key), x_keyLenU64 * 8 };
    context.output = 0;

    auto callback = [](IAsyncContext* ctxt, Status result) {
      assert(result == Status::Ok);
      ReadContext* c = static_cast<ReadContext*>(ctxt);
      assert(c->output == 42);
      cb_call_cnt++;
    };

    Status result = store.Read(context, callback, 1);
    if (result == Status::Ok)
    {
      assert(context.output == 42);
    }
    else
    {
      pending_cnt++;
      if (result != Status::Pending) {
        store.Read(context, callback, 1);
      }
      assert(result == Status::Pending);
      store.CompletePending(true);
      store.GetColdLog()->CompletePending(true);
    }

    if (i % 1000 == 0)
    {
      printf("read %d/%d\n", int(i), 10000);
    }
  }
  printf("read: total %d pending codepath\n", pending_cnt);
  assert(cb_call_cnt == pending_cnt);

  auto completionCallback = []() {
    compactionCompleted.store(true);
  };

  compactionCompleted.store(false);
  store.CompactHotLog(completionCallback);

  // Read.
  pending_cnt = 0;
  cb_call_cnt = 0;
  for(uint32_t i = 0; i < 10000; ++i) {
    uint32_t idx = rand() % 200000;
    uint64_t* key = new uint64_t[x_keyLenU64];
    for (size_t j = 0; j < x_keyLenU64; ++j) {
      key[j] = idx;
    }

    ReadContext context{ reinterpret_cast<uint8_t*>(key), x_keyLenU64 * 8 };
    context.output = 0;

    auto callback = [](IAsyncContext* ctxt, Status result) {
      assert(result == Status::Ok);
      ReadContext* c = static_cast<ReadContext*>(ctxt);
      assert(c->output == 42);
      cb_call_cnt++;
    };

    Status result = store.Read(context, callback, 1);
    if (result == Status::Ok)
    {
      assert(context.output == 42);
    }
    else
    {
      pending_cnt++;
      if (result != Status::Pending) {
        store.Read(context, callback, 1);
      }
      assert(result == Status::Pending);
      store.CompletePending(true);
      store.GetColdLog()->CompletePending(true);
    }

    if (i % 1000 == 0)
    {
      printf("read %d/%d\n", int(i), 10000);
    }
  }
  printf("read: total %d pending codepath\n", pending_cnt);
  assert(cb_call_cnt == pending_cnt);

  cb_call_cnt = 0;
  pending_cnt = 0;
  for(uint32_t idx = 0; idx < 50000; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
        assert(result == Status::Ok);
        cb_call_cnt++;
    };

    uint64_t* key = new uint64_t[x_keyLenU64];
    for (size_t j = 0; j < x_keyLenU64; ++j) {
      key[j] = idx;
    }

    DeleteContext context{ reinterpret_cast<uint8_t*>(key), x_keyLenU64 * 8 };
    Status result = store.Delete(context, callback, 1);
    assert(result == Status::Ok || result == Status::Pending);
    if (result == Status::Pending)
    {
      pending_cnt++;
    }
    store.CompletePending(true);
    store.GetColdLog()->CompletePending(true);

    if (idx % 10000 == 0)
    {
      printf("deleted %d/%d\n", int(idx), 50000);
    }
  }
  assert(cb_call_cnt == pending_cnt);

  compactionCompleted.store(false);
  store.CompactHotLog(completionCallback);

  cb_call_cnt = 0;
  pending_cnt = 0;
  for(uint32_t idx = 100000; idx < 150000; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
        assert(result == Status::Ok);
        cb_call_cnt++;
    };

    uint64_t* key = new uint64_t[x_keyLenU64];
    for (size_t j = 0; j < x_keyLenU64; ++j) {
      key[j] = idx;
    }

    DeleteContext context{ reinterpret_cast<uint8_t*>(key), x_keyLenU64 * 8 };
    Status result = store.Delete(context, callback, 1);
    assert(result == Status::Ok || result == Status::Pending);
    if (result == Status::Pending)
    {
      pending_cnt++;
    }
    store.CompletePending(true);
    store.GetColdLog()->CompletePending(true);

    if (idx % 10000 == 0)
    {
      printf("deleted %d/%d\n", int(idx - 100000), 50000);
    }
  }
  assert(pending_cnt == cb_call_cnt);

  compactionCompleted.store(false);
  store.CompactHotLog(completionCallback);

  cb_call_cnt = 0;
  pending_cnt = 0;
  for(uint32_t idx = 200000; idx < 200100; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
        assert(result == Status::NotFound);
        cb_call_cnt++;
    };

    uint64_t* key = new uint64_t[x_keyLenU64];
    for (size_t j = 0; j < x_keyLenU64; ++j) {
      key[j] = idx;
    }

    DeleteContext context{ reinterpret_cast<uint8_t*>(key), x_keyLenU64 * 8 };
    Status result = store.Delete(context, callback, 1);
    assert(result == Status::NotFound || result == Status::Pending);
    if (result == Status::Pending)
    {
      pending_cnt++;
    }
    store.CompletePending(true);
    store.GetColdLog()->CompletePending(true);
  }
  assert(pending_cnt == cb_call_cnt);

  compactionCompleted.store(false);
  store.CompactHotLog(completionCallback);

  pending_cnt = 0;
  cb_call_cnt = 0;
  for(uint32_t i = 0; i < 10000; ++i) {
    uint32_t idx = rand() % 200000;
    uint64_t* key = new uint64_t[x_keyLenU64];
    for (size_t j = 0; j < x_keyLenU64; ++j) {
      key[j] = idx;
    }

    ReadContext context{ reinterpret_cast<uint8_t*>(key), x_keyLenU64 * 8 };
    context.output = 0;

    static uint32_t queriedIdx;
    queriedIdx = idx;
    auto callback = [](IAsyncContext* ctxt, Status result) {
      if (queriedIdx < 50000 || (100000 <= queriedIdx && queriedIdx < 150000))
      {
        assert(result == Status::NotFound);
      }
      else
      {
        assert(result == Status::Ok);
        ReadContext* c = static_cast<ReadContext*>(ctxt);
        assert(c->output == 42);
      }
      cb_call_cnt++;
    };

    Status result = store.Read(context, callback, 1);
    if (result == Status::Ok || result == Status::NotFound)
    {
      if (idx < 50000 || (100000 <= idx && idx < 150000))
      {
        assert(result == Status::NotFound);
      }
      else
      {
        assert(result == Status::Ok);
        assert(context.output == 42);
      }
    }
    else
    {
      pending_cnt++;
      assert(result == Status::Pending);
      store.CompletePending(true);
      store.GetColdLog()->CompletePending(true);
    }

    if (i % 1000 == 0)
    {
      printf("read %d/%d\n", int(i), 10000);
    }
  }
  printf("read: total %d pending codepath\n", pending_cnt);
  assert(cb_call_cnt == pending_cnt);

  pending_cnt = 0;
  cb_call_cnt = 0;
  for(uint32_t idx = 0; idx < 200000; ++idx) {
    uint64_t* key = new uint64_t[x_keyLenU64];
    for (size_t j = 0; j < x_keyLenU64; ++j) {
      key[j] = idx;
    }

    RmwContext context{ reinterpret_cast<uint8_t*>(key), x_keyLenU64 * 8 };

    auto callback = [](IAsyncContext* ctxt, Status result) {
      ASSERT_EQ(Status::Ok, result);
      cb_call_cnt++;
    };

    Status result = store.Rmw(context, callback, 1);
    if (result == Status::Pending)
    {
      pending_cnt++;
    }
    store.CompletePending(true);
    store.GetColdLog()->CompletePending(true);

    if (idx % 10000 == 0)
    {
      printf("rmw %d/%d\n", int(idx), 200000);
    }
  }
  printf("rmw: total %d pending codepath\n", pending_cnt);
  assert(cb_call_cnt == pending_cnt);

  compactionCompleted.store(false);
  store.CompactHotLog(completionCallback);

  pending_cnt = 0;
  cb_call_cnt = 0;
  for(uint32_t i = 0; i < 10000; ++i) {
    uint32_t idx = rand() % 200000;
    uint64_t* key = new uint64_t[x_keyLenU64];
    for (size_t j = 0; j < x_keyLenU64; ++j) {
      key[j] = idx;
    }

    ReadContext context{ reinterpret_cast<uint8_t*>(key), x_keyLenU64 * 8 };
    context.output = 0;

    static int queryIdx;
    queryIdx = idx;
    auto callback = [](IAsyncContext* ctxt, Status result) {
      assert(result == Status::Ok);
      ReadContext* c = static_cast<ReadContext*>(ctxt);
      if (queryIdx < 50000 || (100000 <= queryIdx && queryIdx < 150000))
      {
        assert(c->output == 1);
      } else {
        assert(c->output == 85);
      }
      cb_call_cnt++;
    };

    Status result = store.Read(context, callback, 1);
    if (result == Status::Ok)
    {
      if (queryIdx < 50000 || (100000 <= queryIdx && queryIdx < 150000))
      {
        assert(context.output == 1);
      } else {
        assert(context.output == 85);
      }
    }
    else
    {
      pending_cnt++;
      assert(result == Status::Pending);
      store.CompletePending(true);
      store.GetColdLog()->CompletePending(true);
    }

    if (i % 1000 == 0)
    {
      printf("read %d/%d\n", int(i), 10000);
    }
  }
  printf("read: total %d pending codepath\n", pending_cnt);
  assert(cb_call_cnt == pending_cnt);

  store.StopSession();
}

TEST(HotColdLog, CompactionTest)
{
    TestHotColdLogCompaction<false /*onDiskHashTable*/>();
}

TEST(TwoLevelHashTable, CompactionTest)
{
    TestHotColdLogCompaction<true /*onDiskHashTable*/>();
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
