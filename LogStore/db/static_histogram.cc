#include "static_histogram.h"

#include <algorithm>
#include <cmath>
#ifndef NDEBUG
#include <iostream>
#endif

namespace leveldb {

StaticHistogram::StaticHistogram(std::vector<std::pair<Slice,Slice>>* bs,
                                 const Comparator* key_cmp)
  : key_cmp_(key_cmp),
    max_bucket_(nullptr),
    total_(0),
    dirty_heap_(false) {
  buckets_.reserve(bs->size());
  for (uint32_t i = 0; i < bs->size(); i++) {
    buckets_.emplace_back((*bs)[i].first, (*bs)[i].second);
    min_bucket_heap_.push_back(&buckets_.back());
  }
  // make heap for min buckets
  //std::make_heap(min_bucket_heap_.begin(), min_bucket_heap_.end(), BySmallestBucketCount(key_cmp_));
  // max bucket is (arbitrarily) the last element since they all have a zero count
  max_bucket_.store(&buckets_.back());
}

StaticHistogram::StaticHistogram(std::vector<FileMetaData*>* files,
                                 Histogram* base,
                                 const Comparator* key_cmp)
  : key_cmp_(key_cmp),
    max_bucket_(nullptr),
    total_(0),
    dirty_heap_(true) {
  buckets_.reserve(files->size());
  Bucket* m = nullptr;
  for (uint32_t i = 0; i < files->size(); i++) {
    Slice smallest = (*files)[i]->smallest.user_key();
    Slice largest = (*files)[i]->largest.user_key();
    uint64_t count = base ? base->EstimateCountForRange(smallest, largest) : 0;
    buckets_.emplace_back(smallest, largest, count);
    min_bucket_heap_.push_back(&buckets_.back());
    total_ += count;
    if (m == nullptr || count > m->count) {
      m = &buckets_.back();
    }
  }
  // create heap for min buckets
  std::make_heap(min_bucket_heap_.begin(), min_bucket_heap_.end(), BySmallestBucketCount(key_cmp_));
  dirty_heap_ = false;

  // set max bucket
  max_bucket_.store(m);
}

StaticHistogram::StaticHistogram(std::vector<FileMetaData*>* files,
                                 const Comparator* key_cmp)
  : StaticHistogram(files, nullptr, key_cmp)
{}

uint32_t StaticHistogram::IndexOf(const Slice& k) {
  Bucket b(k, k);
  auto fn = [&](const Bucket& a, const Bucket& b) {
    return key_cmp_->Compare(a.largest, b.largest) <= 0;
  };
  auto iter = std::upper_bound(buckets_.begin(), buckets_.end(), b, fn);
  return iter - buckets_.begin();
}

StaticHistogram::Bucket& StaticHistogram::BucketFor(const Slice& k) {
  auto idx = IndexOf(k);
#ifndef NDEBUG
  /*
  assert(key_cmp_->Compare(buckets_[idx].smallest, k) <= 0 
         && key_cmp_->Compare(k, buckets_[idx].largest) <= 0);
  */
#endif
  return buckets_[idx];
}

void StaticHistogram::MaybeUpdateMin(Bucket* updated) {
  MutexLock l(&min_bucket_mutex_);
  if (!dirty_heap_ && min_bucket_heap_.front() == updated) {
    dirty_heap_ = true;
  }
}

void StaticHistogram::MaybeUpdateMax(Bucket* updated) {
  for (;;) {
    Bucket* curr_max = max_bucket_.load();
    if (curr_max->count > updated->count ||
        max_bucket_.compare_exchange_strong(curr_max, updated)) {
      return;
    }
  }
}

uint64_t StaticHistogram::AddCount(const Slice& key, uint64_t c) {
  Bucket& bucket = BucketFor(key);
  bucket.count += c;
  MaybeUpdateMin(&bucket);
  MaybeUpdateMax(&bucket);
  total_ += c;
  return bucket.count;
}

uint64_t StaticHistogram::AddCountForRange(const Slice& first, const Slice& last, uint64_t c) {
  uint32_t start = IndexOf(first);
  uint32_t end = IndexOf(last);

  uint64_t count_for_range = 0;
  uint32_t num_buckets = end - start + 1;
  uint64_t add_per_bucket = c / num_buckets;
  for (uint32_t i = start; i <= end; i++) {
    buckets_[i].count += add_per_bucket;
    count_for_range += buckets_[i].count;
    MaybeUpdateMin(&buckets_[i]);
    MaybeUpdateMax(&buckets_[i]);
  }
  total_ += c;
  return count_for_range;
}

uint64_t StaticHistogram::EstimateCount(const Slice& key) {
  if (buckets_.empty()) {
    return 0;
  }
  Bucket& bucket = BucketFor(key);
  return bucket.count;
}

uint64_t StaticHistogram::EstimateCountForRange(const Slice& first, const Slice& last) {
#ifndef NDEBUG
  assert(key_cmp_->Compare(first, last) <= 1);
#endif
  if (buckets_.empty()) {
    // no buckets means no counts
    return 0;
  } else if (key_cmp_->Compare(last, buckets_.front().smallest) < 0) {
    // requested key range is entirely before what this histogram tracks
    return 0;
  } else if (key_cmp_->Compare(buckets_.back().largest, first) < 0) {
    // requested key range is entirely after what this histogram tracks
    return 0;
  }
  uint32_t start = IndexOf(first);
  uint32_t end = IndexOf(last);
  if (end >= buckets_.size()) {
    end = buckets_.size() - 1;
  }
#ifndef NDEBUG
  assert(start <= end);
#endif
  if (start == end) {
    if (key_cmp_->Compare(last, buckets_[start].smallest) < 0 ||
        key_cmp_->Compare(buckets_[end].largest, first) < 0) {
      // first/last falls into a gap between buckets start and start-1
      return 0;
    } else {    
      // buckets[start].smallest < first < last < buckets[start].largest
      return buckets_[start].count;
    }
  }
  uint64_t count = buckets_[start].count / 2 + buckets_[end].count / 2;
  for (unsigned i = start + 1; i <= end - 1; i++) {
    count += buckets_[i].count;
  }
  return count;
}

uint64_t StaticHistogram::GetBucketCount(const Slice& key) {
  if (buckets_.empty()) {
    return 0;
  }
  Bucket& bucket = BucketFor(key);
  return bucket.count;
}

void StaticHistogram::Merge(Histogram* other) {
  std::vector<Bucket> new_buckets;
  new_buckets.reserve(buckets_.size());
  uint64_t new_total = 0;
  for (auto& b : buckets_) {
    uint64_t new_count = other->EstimateCountForRange(b.smallest, b.largest);
    new_buckets.emplace_back(b.smallest, b.largest, new_count);
    new_total += new_count;
  }
  buckets_ = std::move(new_buckets);
  total_.store(new_total);
}

uint64_t StaticHistogram::GetAverageBucketCount() {
    if (buckets_.size() > 0){
        return total_.load() / buckets_.size();
    }
    else{
        return 0;
    }
}

uint64_t StaticHistogram::GetMedianBucketCount() {
  if (buckets_.empty()) {
    return 0;
  }
  return buckets_[buckets_.size() / 2].count;
}

uint64_t StaticHistogram::GetMaxBucketCount(std::pair<Slice,Slice>* p) {
  if (buckets_.empty()) {
    return 0;
  }
  Bucket* max = max_bucket_.load();
  if (p != nullptr) {
    p->first = max->smallest;
    p->second = max->largest;
  }
  return max->count;
}

uint64_t StaticHistogram::GetMinBucketCount(std::pair<Slice,Slice>* p) {
  if (buckets_.empty()) {
    return 0;
  }
  Bucket* min = nullptr;
  {
    MutexLock l(&min_bucket_mutex_);
    if (dirty_heap_) {
      BySmallestBucketCount cmp(key_cmp_);
      std::make_heap(min_bucket_heap_.begin(), min_bucket_heap_.end(), cmp);
      dirty_heap_ = false;
    }
    min = min_bucket_heap_.front();
  }
  if (p != nullptr) {
    p->first = min->smallest;
    p->second = min->largest;
  }
  return min->count;
}

uint64_t StaticHistogram::LowestNAverageCount(double pct) {
  if (buckets_.empty()) {
    return 0;
  } else if (pct < 0.0 || pct > 1.0) {
    return 0;
  }
  
  MutexLock l(&min_bucket_mutex_);
  BySmallestBucketCount cmp(key_cmp_);
  if (dirty_heap_) {
    std::make_heap(min_bucket_heap_.begin(), min_bucket_heap_.end(), cmp);
  }
  uint32_t n = static_cast<uint32_t>(pct * buckets_.size());
  uint64_t total = 0;
  for (uint32_t i = 0; i < n; i++) {
    total += min_bucket_heap_.front()->count;
    std::pop_heap(min_bucket_heap_.begin(), min_bucket_heap_.end()-i, cmp);
  }
  dirty_heap_ = true;
  
  return total / n;
}

// TEST ONLY
void StaticHistogram::Display()  {
#ifndef NDEBUG
  uint64_t num_buckets = 0;
  uint64_t curr_total = 0;
  for (auto& b : buckets_) {
    curr_total += b.count.load();
    double pct = static_cast<double>(curr_total) / static_cast<double>(GetTotal());
    double key_pct = static_cast<double>(++num_buckets) / static_cast<double>(buckets_.size());
    std::cout << "[" << b.smallest.ToString() << "-" << b.largest.ToString();
    std::cout << ", " << (key_pct * 100) << " ==> " << b.count.load();
    std::cout << " (" << curr_total << ", " << (pct * 100) << "%)" << std::endl;
  }
#endif
}

} // namespace leveldb
