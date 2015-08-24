#include "util/ssd_cache.h"

namespace leveldb {

SegmentManager::SegmentManager(size_t sz)
  : i(0) {
  uint64_t num_segments = sz / kSegmentSize;
  segments_.reserve(num_segments);
  for (unsigned i = 0; i < num_segments; i++) {
    segments_.emplace_back(i, kSegmentSize);
  }
}

// TODO: Implement recovery
SsdCache::SsdCache(std::string cache_dir, size_t capacity)
  : cache_fname_(cache_dir.append("/ssd_cache.db")),
    capacity_(capacity),
    curr_size_(0),
    //index_(16384, HashSlice, KeyEquals),
    segment_manager_(capacity) {
    //buf_(new char[kSegmentSize]) {
  active_segment_ = segment_manager_.NextSegment();
  memset(buf_, 0, kSegmentSize);
  dummy_.next = dummy_.prev = &dummy_;
}

SsdCache::~SsdCache() { 
  // free any entries we've brought into cache
  /*
  for (auto& e : index_) {
    auto entry = e.second;
    delete[] entry->key_;
    delete entry;
  }
  */
  //delete[] buf_;
}

bool SsdCache::Insert(const Slice& key, void* value, size_t size) {
  MutexLock l(&mutex_);

  // Make sure we have enough room in cache
  MakeRoomForInsert(key, size);

  IndexEntry* entry = table_.Lookup(key, HashSlice(key));
  if (entry != nullptr) {
    // This key exists in cache, garbage previous version, remove from LRU
    auto old_segment = segment_manager_.GetSegment(SEGMENT_NUM(entry->off));
    old_segment->MarkGarbage(entry->len);
    LRU_Remove(entry);
  } else {
    // This key/value is new to the cache
    entry = reinterpret_cast<IndexEntry*>(malloc(sizeof(IndexEntry)-1 + key.size()));

    // Copy the key to the index entry space
    memcpy(entry->key_, key.data(), key.size());
    entry->len = key.size() + size;
    entry->key_len = key.size();
    entry->hash = HashSlice(key);

    // Insert into hash table
    table_.Insert(entry);
  }

  if (active_segment_->BytesAvailable() < key.size() + size) {
    RollSegment();
  }

  // update the offset of the entry in the file
  entry->off = AppendToSegment(key, value, size);

  // update LRU
  LRU_Append(entry);

  // increment total size
  curr_size_ += entry->len;

  return true;
}

void SsdCache::Lookup(const Slice& key, 
                      void* arg,
                      void (*saver)(void* arg, const Slice& key, const Slice& value)) {
  MutexLock l(&mutex_);
  auto entry = table_.Lookup(key, HashSlice(key));
  if (entry == nullptr) {
    // Key not in cache
    return;
  }
  uint64_t val_size = entry->len - key.size();
  if (active_segment_ == segment_manager_.GetSegment(SEGMENT_NUM(entry->off))) {
    // if the lookup is stored in the active segment, serve from memory
    uint32_t off = SEGMENT_OFF(entry->off) + key.size();
    saver(arg, key, Slice{buf_+off, val_size});
  } else {
    // Do a regular read from the cache file, saving into buf
    //char* buf = new char[val_size];
    uint32_t segment_num = SEGMENT_NUM(entry->off);
    uint32_t segment_off = SEGMENT_OFF(entry->off);
    //size_t r = pread(fd_, buf, val_size, entry->off+key.size());
    size_t r = pread(fd_, rbuf_, kSegmentSize, segment_num*kSegmentSize);
    if (!r) {
    //if (r != val_size) {
      std::cout << strerror(errno) << std::endl;
      exit(1);
    }
    saver(arg, key, Slice{rbuf_+segment_off, val_size});
    //delete[] buf;
  }

  // update lru
  LRU_Remove(entry);
  LRU_Append(entry);
}


void SsdCache::Erase(const Slice& key) {
  MutexLock l(&mutex_);
  auto entry = table_.Lookup(key, HashSlice(key));
  if (entry != nullptr) {
    Remove(entry);
  }
}

void SsdCache::RollSegment() {
  // The segment is full, we need to:
  // (1) Flush the current active segment to its location in the file
  //     using the contents of the currenlty stored buffer
  // (2) Get a new segment from the segment manager
  size_t w = pwrite(fd_, buf_, active_segment_->size, active_segment_->StartOffset());
  if (w != active_segment_->size) {
    std::cout << strerror(errno) << std::endl;
    exit(1);
  }

  active_segment_ = segment_manager_.NextSegment();
}

uint64_t SsdCache::AppendToSegment(const Slice& key, void* value, size_t sz) {
  uint32_t start = active_segment_->curr_offset;
  memcpy(buf_+start, key.data(), key.size());
  memcpy(buf_+start+key.size(), value, sz);
  active_segment_->curr_offset += key.size() + sz;
  return ADDRESS(active_segment_->num, start);
}

void SsdCache::MakeRoomForInsert(const Slice& key, size_t sz) {
  uint64_t entry_size = key.size() + sz;
  //while ((curr_size_ + entry_size > capacity_ * 0.85) && dummy_.next != &dummy_) {
  if (table_.Size() < 4000000) {
    return;
  }
  while (table_.Size() > 3500000 && dummy_.next != &dummy_) {
    IndexEntry* old = dummy_.next;
    Remove(old);
  }
}

void SsdCache::LRU_Append(IndexEntry* e) {
  // Make e the newest entry
  e->next = &dummy_;
  e->prev = dummy_.prev;
  e->prev->next = e;
  e->next->prev = e;
}

void SsdCache::LRU_Remove(IndexEntry* e) {
  e->next->prev = e->prev;
  e->prev->next = e->next;
}

void SsdCache::Remove(IndexEntry* entry) {
  // Let the segment know that it's storing garbage
  auto segment = segment_manager_.GetSegment(SEGMENT_NUM(entry->off));
  segment->MarkGarbage(entry->len);

  // Remove the key from the index
  //index_.erase(key);
  table_.Remove(entry->key(), entry->hash);
  
  // Remove entry from LRU
  LRU_Remove(entry);

  // free the memory we allocated for the key and the index entry
  //delete[] entry->key_;
  //delete entry;
  free(entry);
}


} // namespace leveldb
