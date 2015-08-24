// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/cache.h"

#include <vector>
#include "util/coding.h"
#include "util/ssd_cache.h"
#include "util/random.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace leveldb {

// Conversions between numeric keys/values and the types expected by Cache.
static std::string EncodeKey(int k) {
  std::string result;
  PutFixed32(&result, k);
  return result;
}
static int DecodeKey(const Slice& k) {
  assert(k.size() == 4);
  return DecodeFixed32(k.data());
}
static void* EncodeValue(uintptr_t v) { return reinterpret_cast<void*>(v); }
static int DecodeValue(void* v) { 
  std::string va {(char*)v, 4};
  return atoi(va.c_str());
}

class CacheTest {
 public:
  static CacheTest* current_;

  static void Deleter(const Slice& key, void* v) {
    current_->deleted_keys_.push_back(DecodeKey(key));
    current_->deleted_values_.push_back(DecodeValue(v));
  }

  static const size_t kCacheSize = (1 << 18) * 10000000;
  std::vector<int> deleted_keys_;
  std::vector<int> deleted_values_;
  Random rnd;
  SsdCache* cache_;

  CacheTest() : cache_(NewSSDCache(test::TmpDir(), kCacheSize)), rnd(test::RandomSeed()) {
    ASSERT_OK(cache_->Open());
    current_ = this;
  }

  ~CacheTest() {
    delete cache_;
  }

  int Lookup(int key) {
    int r = -1;
    cache_->Lookup(EncodeKey(key), &r, [](void* arg, const Slice& k, const Slice& val) {
      int* ret = (int*) arg;
      *ret = DecodeValue((void*)val.data());
    });
    return r;
  }

  bool LookupString(int key, std::string* s) {
    struct Saver {
      std::string* s;
      bool found;
    };
    Saver saver { s, false };
    cache_->Lookup(EncodeKey(key), &saver, [](void* arg, const Slice& k, const Slice& val) {
      Saver* sv = (Saver*) arg;
      sv->s->assign(val.data(), val.size());
      sv->found = true;
    });
    return saver.found;
  }

  void Insert(int key, int value, int size = 4) {
    std::string v = std::to_string(value);
    cache_->Insert(EncodeKey(key), (void*)v.c_str(), size);
  }

  void InsertString(int key, int size, std::string* val) {
    Slice v = test::RandomString(&rnd, size, val);
    cache_->Insert(EncodeKey(key), (void*)v.data(), v.size());
  }

  void Erase(int key) {
    cache_->Erase(EncodeKey(key));
  }

  bool ContainsKey(int key) {
    return cache_->ContainsKey(EncodeKey(key));
  }

  uint64_t Size() { return cache_->NumKeys(); }
};
CacheTest* CacheTest::current_;

TEST(CacheTest, HitAndMiss) {
  ASSERT_EQ(-1, Lookup(100));

  Insert(100, 101);
  ASSERT_EQ(101, Lookup(100));
  ASSERT_EQ(-1,  Lookup(200));
  ASSERT_EQ(-1,  Lookup(300));

  Insert(200, 201);
  ASSERT_EQ(101, Lookup(100));
  ASSERT_EQ(201, Lookup(200));
  ASSERT_EQ(-1,  Lookup(300));

  Insert(100, 102);
  ASSERT_EQ(102, Lookup(100));
  ASSERT_EQ(201, Lookup(200));
  ASSERT_EQ(-1,  Lookup(300));
}

TEST(CacheTest, Erase) {
  Erase(200);

  Insert(100, 101);
  Insert(200, 201);
  ASSERT_EQ(2, Size());

  Erase(100);
  ASSERT_EQ(1, Size());
  ASSERT_EQ(-1,  Lookup(100));
  ASSERT_EQ(201, Lookup(200));

  Erase(100);
  ASSERT_EQ(1, Size());
  ASSERT_EQ(-1,  Lookup(100));
  ASSERT_EQ(201, Lookup(200));
}

TEST(CacheTest, RollSegments) {
  std::string one, two, three, four, five, six, seven;
  InsertString(100, 1 << 14, &one);
  ASSERT_TRUE(ContainsKey(100));
  ASSERT_EQ(1, Size());

  InsertString(200, 1 << 10, &two);
  ASSERT_TRUE(ContainsKey(200));
  ASSERT_EQ(2, Size());
  
  InsertString(300, 1 << 8, &three);
  ASSERT_TRUE(ContainsKey(300));
  ASSERT_EQ(3, Size());

  InsertString(400, 1 << 9, &four);
  ASSERT_EQ(4, Size());

  InsertString(500, 1 << 16, &five);
  ASSERT_EQ(5, Size());

  InsertString(600, 1 << 17, &six);
  ASSERT_EQ(6, Size());

  InsertString(700, 1 << 17, &seven);
  ASSERT_EQ(7, Size());

  std::string one_l, two_l, three_l, four_l, five_l, six_l;
  LookupString(100, &one_l);
  ASSERT_EQ(one.compare(one_l), 0);

  LookupString(200, &two_l);
  ASSERT_EQ(two.compare(two_l), 0);

  LookupString(300, &three_l);
  ASSERT_EQ(three.compare(three_l), 0);

  LookupString(400, &four_l);
  ASSERT_EQ(four.compare(four_l), 0);

  LookupString(500, &five_l);
  ASSERT_EQ(five.compare(five_l), 0);

  LookupString(600, &six_l);
  ASSERT_EQ(six.compare(six_l), 0);
}

TEST(CacheTest, LRU) {
  std::unordered_map<int, std::string*> entries;
  for (unsigned i = 1; i < 10; i++) {
    std::string* s = new std::string();
    entries[i*100] = s;
    InsertString(i*100, 1 << 17, s);
  }

  std::string one_l, two_l, three_l, four_l, five_l, six_l;
  ASSERT_TRUE(LookupString(100, &one_l));
  ASSERT_TRUE(LookupString(200, &two_l));
  ASSERT_TRUE(LookupString(300, &three_l));
  ASSERT_TRUE(LookupString(400, &four_l));
  ASSERT_TRUE(LookupString(500, &five_l));
  ASSERT_TRUE(LookupString(600, &six_l));
  ASSERT_TRUE(LookupString(700, &six_l));
  ASSERT_TRUE(LookupString(800, &six_l));
  ASSERT_TRUE(LookupString(900, &six_l));

  for (auto& e : entries) {
    delete e.second;
  }
}

/*
TEST(CacheTest, Leak) {
  std::string s;
  for (unsigned i = 1; i < 20000000; i++) {
    s.clear();
    InsertString(i*100, 4, &s);
  }

  for (int i = 0; i < 10; i++) {
    std::cout << i;
  }
}
*/

}  // namespace leveldb

int main(int argc, char** argv) {
  return leveldb::test::RunAllTests();
}
