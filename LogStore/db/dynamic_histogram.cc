#include "dynamic_histogram.h"

#include <algorithm>
#include <cmath>

namespace leveldb {

template <class T>
static uint32_t IndexOf(std::vector<T>* v, const Slice& k, const Comparator* cmp) {
  T val { k, k };
  auto fn = [&](const T& a, const T& b) {
    return cmp->Compare(a.Largest(), b.Largest()) <= 0;
  };
  auto iter = std::upper_bound(v->begin(), v->end(), val, fn);
  return iter - v->begin();
}

template <class T>
static T& BucketFor(std::vector<T>* v, const Slice& k, const Comparator* cmp) {
  auto idx = IndexOf(v, k, cmp);
#ifndef NDEBUG
  T& item = (*v)[idx];
  bool good = cmp->Compare((*v)[idx].Smallest(), k) <= 0 && cmp->Compare(k, (*v)[idx].Largest()) <= 0;
  if (!good) {
    item.DEBUG();
    std::cout << "Key " << k.ToString() << " doesn't belong in bucket " << item.Smallest().ToString() << "-" << item.Largest().ToString() << std::endl;
  }
  assert(cmp->Compare((*v)[idx].Smallest(), k) <= 0 && cmp->Compare(k, (*v)[idx].Largest()) <= 0);
#endif
  return (*v)[idx];
}

uint64_t DynamicHistogram::Bucket::EstimateCount(const Slice& k) { 
  SubBucket& sb = BucketFor(&sub_buckets_, k, key_cmp_);
  return sb.GetCount();
}

uint64_t DynamicHistogram::Bucket::EstimateCountForRange(const Slice& s, const Slice& e) {
  if (sub_buckets_.size() <= 1) {
    return count_.load();
  }
  uint32_t start = IndexOf(&sub_buckets_, s, key_cmp_);
  uint32_t end = IndexOf(&sub_buckets_, e, key_cmp_);
  uint64_t count = 0;
  for (unsigned i = start; i < end; i++) {
    count += sub_buckets_[i].GetCount();
  }
  return count;
}

void DynamicHistogram::Bucket::DEBUG() const {
#ifndef NDEBUG
  std::cout << "Bucket[" << smallest_.ToString() << "-" << largest_.ToString();
  std::cout << ",granularity=" << granularity_ << ",count=" << count_.load();
  std::cout << "] ==>" << std::endl;
  int width = 4;
  int curr_width = 0;
  for (auto& sb : sub_buckets_) {
    if (curr_width == 0) {
      std::cout << "    ";
    }
    std::cout << "SubBucket[" << sb.Smallest().ToString() << "-" << sb.Largest().ToString() << "=>" << sb.GetCount() << "],";
    if (curr_width++ > width) {
      std::cout << std::endl;
      curr_width = 0;
    }
  }
  std::cout << std::endl;
#endif
}

uint64_t DynamicHistogram::Bucket::AddCount(const Slice& k) { 
  // get the bucket and add a count to it
  uint32_t idx = IndexOf(&sub_buckets_, k, key_cmp_);
  SubBucket& sb = sub_buckets_[idx];
  uint64_t count = sb.AddCount();

  // if the granularity is fine and we haven't filled up our
  // sub-buckets, split this sub bucket
  if (granularity_ == Granularity::FINE && sub_buckets_.size() < 500 &&
      (key_cmp_->Compare(k, sb.Smallest()) != 0 && key_cmp_->Compare(k, sb.Largest()) != 0)) {
    sub_buckets_.insert(sub_buckets_.begin() + idx, SubBucket(sb.Smallest(), k, count / 2));
    sub_buckets_[idx+1].Smallest(k);
    sub_buckets_[idx+1].SetCount(count/2);
  }
  return ++count_;
}

void DynamicHistogram::Bucket::Promote() { 
  // set granularity
  if (granularity_ == Granularity::MIXED) {
    granularity_ = Granularity::FINE;
  } else if (granularity_ == Granularity::COARSE) {
    granularity_ = Granularity::MIXED;
  }
}

void DynamicHistogram::Bucket::Demote() { 
  // set update granularity
  if (granularity_ == Granularity::FINE) {
    granularity_ = Granularity::MIXED;
  } else if (granularity_ == Granularity::MIXED) {
    granularity_ = Granularity::COARSE;
  }

  // collapse some sub buckets
  sub_buckets_.clear();
  sub_buckets_.emplace_back(smallest_, largest_, count_.load());
}

/* ---------------------------- Dynamic Histogram ---------------------------- */

DynamicHistogram::DynamicHistogram(const Options* options,
                                   std::vector<std::pair<Slice,Slice>>* bs,
                                   const Comparator* key_cmp)
  : options_(options), 
    mixed_threshold_count_(2000),
    mixed_threshold_fraction_(0.5),
    fine_threshold_count_(4000),
    fine_threshold_fraction_(0.25),
    max_sub_buckets_count_(500),
    key_cmp_(key_cmp) {
  buckets_.reserve(bs->size());
  for (uint32_t i = 0; i < bs->size(); i++) {
    buckets_.emplace_back(key_cmp, (*bs)[i].first, (*bs)[i].second);
  }
}

DynamicHistogram::DynamicHistogram(const Options* options,
                                   std::vector<FileMetaData*>* files,
                                   Histogram* base,
                                   const Comparator* key_cmp)
  : options_(options), 
    mixed_threshold_count_(2000),
    mixed_threshold_fraction_(0.5),
    fine_threshold_count_(4000),
    fine_threshold_fraction_(0.25),
    max_sub_buckets_count_(500),
    key_cmp_(key_cmp) {
  buckets_.reserve(files->size());
  for (uint32_t i = 0; i < files->size(); i++) {
    Slice smallest = (*files)[i]->smallest.user_key();
    Slice largest = (*files)[i]->largest.user_key();
    uint64_t count = base->EstimateCountForRange(smallest, largest);
    buckets_.emplace_back(key_cmp, smallest, largest, count);
  }
}

uint64_t DynamicHistogram::AddCount(const Slice& key) {
  Bucket& bucket = BucketFor(&buckets_, key, key_cmp_);
  uint64_t count = bucket.AddCount(key);
  if (CanPromote(&bucket)) {
    PromoteBucket(&bucket);
  } 
  return count;
}

uint64_t DynamicHistogram::EstimateCount(const Slice& key) {
  Bucket& bucket = BucketFor(&buckets_, key, key_cmp_);
  return bucket.EstimateCount(key);
}

uint64_t DynamicHistogram::EstimateCountForRange(const Slice& first, const Slice& last) {
  uint32_t start = IndexOf(&buckets_, first, key_cmp_);
  uint32_t end = IndexOf(&buckets_, last, key_cmp_);
  if (start == end) {
    return buckets_[start].GetBucketCount();
  }
  uint64_t count = buckets_[start].EstimateCountGreaterThan(first) 
    + buckets_[end].EstimateCountLessThan(last);
  for (unsigned i = start + 1; i <= end - 1; i++) {
    count += buckets_[i].GetBucketCount();
  }
  return count;
}

uint64_t DynamicHistogram::GetBucketCount(const Slice& key) {
  Bucket& bucket = BucketFor(&buckets_, key, key_cmp_);
  return bucket.GetBucketCount();
}

void DynamicHistogram::Merge(Histogram* other) {
  std::vector<Bucket> new_buckets;
  new_buckets.reserve(buckets_.size());
  for (auto& b : buckets_) {
    uint64_t new_count = other->EstimateCountForRange(b.Smallest(), b.Largest());
    new_buckets.emplace_back(key_cmp_, b.Smallest(), b.Largest(), new_count);
  }
  buckets_ = std::move(new_buckets);
}

bool DynamicHistogram::CanPromote(Bucket* bucket) {
  if (bucket->GetBucketCount() > fine_threshold_count_ && 
      bucket->GetGranularity() != Granularity::FINE) {
    return true;
  } else if (bucket->GetBucketCount() > mixed_threshold_count_ && 
      bucket->GetGranularity() != Granularity::MIXED) {
    return true;
  } else {
    return false;
  }
}

void DynamicHistogram::PromoteBucket(Bucket* bucket) {
  //Log(options_->info_log, "MANIFEST write: %s\n", s.ToString().c_str());
  if (bucket->GetGranularity() == Granularity::MIXED) {
    if (fine_buckets_.size() > buckets_.size() * fine_threshold_fraction_) {
      // the heap property of the vector may have been invalidated since the 
      // last time a bucket was moved intot he fine_buckets_ list.
      std::make_heap(fine_buckets_.begin(), fine_buckets_.end(), bucket_cmp_);

      // pop the bucket off the vector witht he lowest count
      std::pop_heap(fine_buckets_.begin(), fine_buckets_.end(), bucket_cmp_);
      Bucket* kick = fine_buckets_.back();
      fine_buckets_.pop_back();

      // demote the bucket
      DemoteBucket(kick);
    }
    // add our new bucket to the list
    fine_buckets_.push_back(bucket);
    std::push_heap(fine_buckets_.begin(), fine_buckets_.end(), bucket_cmp_);
  } else if (bucket->GetGranularity() == Granularity::COARSE) {
    if (mixed_buckets_.size() > buckets_.size() * mixed_threshold_fraction_) {
      // the heap property of the vector may have been invalidated since the 
      // last time a bucket was moved intot he fine_buckets_ list.
      std::make_heap(mixed_buckets_.begin(), mixed_buckets_.end(), bucket_cmp_);

      // pop the bucket off the vector with the lowest count
      std::pop_heap(mixed_buckets_.begin(), mixed_buckets_.end(), bucket_cmp_);
      Bucket* kick = mixed_buckets_.back();
      mixed_buckets_.pop_back();

      // demote the bucket
      DemoteBucket(kick);
    }
    // add our new bucket to the list
    mixed_buckets_.push_back(bucket);
    std::push_heap(mixed_buckets_.begin(), mixed_buckets_.end(), bucket_cmp_);
  }
  
  // promote the bucket we want to
  bucket->Promote();
}

void DynamicHistogram::DemoteBucket(Bucket* bucket) {
  bucket->Demote();
}

void DynamicHistogram::DEBUG() const {
  for (auto& b : buckets_) {
    b.DEBUG();
  }
}

} // namespace leveldb
