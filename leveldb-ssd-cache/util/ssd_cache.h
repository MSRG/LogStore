#ifndef SSD_CACHE_H_
#define SSD_CACHE_H_

#include <atomic>
#include <fcntl.h>
#include <functional>
#include <iostream>
#include <unistd.h>
#include <unordered_map>
#include <vector>

#include "leveldb/status.h"
#include "port/port.h"
#include "util/hash.h"
#include "util/mutexlock.h"

namespace leveldb {

static const int kSegmentOffsetBits = 18; // For 256K segments
static const int kSegmentNumBits = 64 - kSegmentOffsetBits;
static const int kSegmentSize = 1 << kSegmentOffsetBits;
static const int kSegmentOffsetMask = kSegmentSize-1;

#define SEGMENT_NUM(x) (x >> kSegmentOffsetBits)
#define SEGMENT_OFF(x) (x & kSegmentOffsetMask)
#define ADDRESS(segment, off) ((segment << kSegmentOffsetBits) | off)

struct Segment {
  uint32_t num;
  uint32_t size;
  uint32_t curr_offset;
  uint32_t garbage_bytes;

  Segment(uint32_t n, uint32_t sz): num(n), size(sz) { curr_offset = 0; garbage_bytes = 0; }
  
  double Unused() const { 
    double total_unused = garbage_bytes + (size - curr_offset);
    return static_cast<double>(total_unused) / static_cast<double>(size); 
  }

  uint32_t BytesAvailable() const { return size - curr_offset; }

  void MarkGarbage(uint64_t nbytes) { garbage_bytes += nbytes; }

  uint64_t StartOffset() const { return num * size; }
};

class SegmentManager {
 public:

 private:
  uint32_t segment_size;
  std::vector<Segment> segments_;
  int i;

 public:
  SegmentManager(size_t sz);

  inline Segment* NextSegment() { return &segments_[i++]; }

  inline Segment* GetSegment(int seg_num) { return &segments_[seg_num]; }
};

struct IndexEntry {
  uint64_t off;
  uint32_t len; // total length of entry on-disk (include key size)
  uint32_t key_len;
  //char* key_; // pointer to heap-allocated key
  uint32_t hash;

  IndexEntry* prev;
  IndexEntry* next;
  IndexEntry* next_hash;

  char key_[1];   // Beginning of key

  Slice key() const { return Slice{key_, key_len}; }
};

class HandleTable {
 public:
  HandleTable() : length_(0), elems_(0), list_(NULL) { Resize(); }
  ~HandleTable() {
      for (uint32_t i = 0; i < length_; i++) {
          IndexEntry* h = list_[i];
          while (h != NULL) {
              IndexEntry* next = h->next_hash;
              free(h);
              h = next;
          }
      }
      delete[] list_;
  }

  IndexEntry* Lookup(const Slice& key, uint32_t hash) {
    return *FindPointer(key, hash);
  }

  IndexEntry* Insert(IndexEntry* h) {
    IndexEntry** ptr = FindPointer(h->key(), h->hash);
    IndexEntry* old = *ptr;
    h->next_hash = (old == NULL ? NULL : old->next_hash);
    *ptr = h;
    if (old == NULL) {
      ++elems_;
      if (elems_ > length_) {
        // Since each cache entry is fairly large, we aim for a small
        // average linked list length (<= 1).
        Resize();
      }
    }
    return old;
  }

  IndexEntry* Remove(const Slice& key, uint32_t hash) {
    IndexEntry** ptr = FindPointer(key, hash);
    IndexEntry* result = *ptr;
    if (result != NULL) {
      *ptr = result->next_hash;
      --elems_;
    }
    return result;
  }

  uint32_t Size() const { return elems_; }

 private:
  // The table consists of an array of buckets where each bucket is
  // a linked list of cache entries that hash into the bucket.
  uint32_t length_;
  uint32_t elems_;
  IndexEntry** list_;

  // Return a pointer to slot that points to a cache entry that
  // matches key/hash.  If there is no such cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  IndexEntry** FindPointer(const Slice& key, uint32_t hash) {
    IndexEntry** ptr = &list_[hash & (length_ - 1)];
    while (*ptr != NULL &&
           ((*ptr)->hash != hash || key != (*ptr)->key())) {
      ptr = &(*ptr)->next_hash;
    }
    return ptr;
  }

  void Resize() {
    uint32_t new_length = 4;
    while (new_length < elems_) {
      new_length *= 2;
    }
    IndexEntry** new_list = new IndexEntry*[new_length];
    memset(new_list, 0, sizeof(new_list[0]) * new_length);
    uint32_t count = 0;
    for (uint32_t i = 0; i < length_; i++) {
      IndexEntry* h = list_[i];
      while (h != NULL) {
        IndexEntry* next = h->next_hash;
        uint32_t hash = h->hash;
        IndexEntry** ptr = &new_list[hash & (new_length - 1)];
        h->next_hash = *ptr;
        *ptr = h;
        h = next;
        count++;
      }
    }
    assert(elems_ == count);
    delete[] list_;
    list_ = new_list;
    length_ = new_length;
  }
};

class SsdCache {

 private:
  static inline uint32_t HashSlice(const Slice& key) {
    return Hash(key.data(), key.size(), 0);
  }

  static inline bool KeyEquals(const Slice& k1, const Slice& k2) {
    return k1 == k2;
  }

  // The name of the file used by this cache
  std::string cache_fname_;

  // The file descriptor of the cache file if the cache has been opened
  int fd_;
  port::Mutex mutex_;

  // The class that manages the segments in the file
  SegmentManager segment_manager_;
  Segment* active_segment_;

  // Write Buffer
  char buf_[kSegmentSize]; 

  // Read buffer
  char rbuf_[kSegmentSize];

  // The maximum size of the cache (in bytes)
  uint64_t capacity_;
  uint64_t curr_size_;

  // The in-memory index that maps keys to offset into the file
  /*
  std::unordered_map<
    Slice, 
    IndexEntry*, 
    std::function<decltype(HashSlice)>,
    std::function<decltype(KeyEquals)>> index_;
  */

  HandleTable table_;

  // LRU
  IndexEntry dummy_;

 public:
  explicit SsdCache(std::string cache_dir, size_t capacity);

  ~SsdCache();

  bool Insert(const Slice& key, void* value, size_t size);

  void Lookup(const Slice& key, void* arg, void (*saver)(void*, const Slice& key, const Slice& val));

  void Erase(const Slice& key);

  //inline bool ContainsKey(const Slice& key) const { return index_.find(key) != index_.end(); }
  inline bool ContainsKey(const Slice& key) { return table_.Lookup(key, HashSlice(key)) != nullptr; }

  //inline uint64_t NumKeys() const { return index_.size(); }
  inline uint64_t NumKeys() const { return table_.Size(); }

  Status Open() {
    fd_ = open(cache_fname_.c_str(), O_CREAT | O_RDWR, S_IRUSR | S_IWUSR );
    if (fd_ < 0) {
      // error
      std::cout << "Error opening file: " << strerror(errno) << std::endl;
      return Status::IOError(cache_fname_, strerror(errno));
    }
    return Status::OK();
  }

 private:
  // Persist current active segment and start using a new one
  void RollSegment();

  // Append the parameters to the current active segment
  uint64_t AppendToSegment(const Slice& key, void* value, size_t sz);

  // Ensure there is sufficient room in the index for the given kv pair
  void MakeRoomForInsert(const Slice& key, size_t sz);

  // Remove the key (and it's value) from the index and cleanup resources
  void Remove(IndexEntry* e);

  void LRU_Append(IndexEntry* e);

  void LRU_Remove(IndexEntry* e);
};

} // namespace

#endif
