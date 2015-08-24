#ifndef STATIC_HISTOGRAM_H_
#define STATIC_HISTOGRAM_H_

#include <atomic>
#include <queue>
#include <vector>

#ifndef NDEBUG
#include <iostream>
#endif

#include "db/histogram.h"
#include "db/version_edit.h"
#include "leveldb/comparator.h"
#include "leveldb/options.h"
#include "leveldb/slice.h"
#include "util/mutexlock.h"

namespace leveldb {

class StaticHistogram : public Histogram {
 private:

  /* ---------------------------- BUCKET ---------------------------- */
  struct Bucket {
    Slice smallest;
    Slice largest;
    uint64_t last_access_time;
    std::atomic_uint_fast64_t count;
    //std::atomic<uint64_t> count;

    Bucket(Slice s, Slice l, uint64_t c)
      : smallest(s), largest(l), count(c)
    {}
    Bucket(Slice s, Slice l): Bucket(s, l, 0) {}
    Bucket(Bucket&& other) noexcept 
      : smallest(other.smallest), largest(other.largest),
        count(other.count.load())
    {}

    Bucket& operator=(Bucket&& other) noexcept {
      smallest = other.smallest;
      largest = other.largest;
      count.store(other.count.load());
      return *this;
    }

#ifndef NDEBUG
    void Display() const {
      std::cout << "Bucket [" << smallest.ToString() << "-" << largest.ToString();
      std::cout << ", " << count << "]" << std::endl;
    }
#endif

    // no copying
    Bucket(const Bucket& other) = delete;
    Bucket& operator=(const Bucket& other) = delete;
  };

  /* 
   * The buckets that make up this histogram.  The buckets are
   * sorted by the user keys in ascending order, but their
   * widths may not be equal.
   */
  std::vector<Bucket> buckets_;

  struct BySmallestBucketCount {
    const Comparator* cmp_;
    BySmallestBucketCount(const Comparator* cmp): cmp_(cmp) {}
    bool operator()(const Bucket* a, const Bucket* b) {
      if (a->count == b->count) {
        //(pmenon)return cmp_->Compare(a->largest, b->largest) > 0;
        return cmp_->Compare(a->largest, b->largest) < 0;
      } else {
        return a->count > b->count;
      }
    }
  };
  port::Mutex min_bucket_mutex_;
  bool dirty_heap_;
  std::vector<Bucket*> min_bucket_heap_;

  /* The maximum bucket in this histogram.  Managed using CAS */
  std::atomic<Bucket*> max_bucket_;

  /* The total number of hits this histogram has seen.  Used for 
   * calculating averages
   */
  std::atomic<uint64_t> total_;

  /* The comparator we use to determine the order of user keys */
  const Comparator* key_cmp_;

  /* Return the index of the bucket the provided key falls into */
  uint32_t IndexOf(const Slice& k);

  /* Get the bucket the provided key falls into */
  Bucket& BucketFor(const Slice& k);

  /* Potentially update the minimum or maximum bucket given that 
   * the caller has updated the provided bucket
   */
  void MaybeUpdateMin(Bucket* updated);
  void MaybeUpdateMax(Bucket* updated);

 public:
  /* The constructor */
  // FOR TESTING ONLY
  StaticHistogram(std::vector<std::pair<Slice, Slice>>* buckets, 
                  const Comparator* cmp);
  StaticHistogram(std::vector<FileMetaData*>* files, Histogram* base,
                  const Comparator* cmp);
  StaticHistogram(std::vector<FileMetaData*>* files, const Comparator* cmp);
  virtual ~StaticHistogram() override {}

  /* Add an access count to the bucket for the provided key */
  virtual uint64_t AddCount(const Slice& key, uint64_t c) override;
  virtual uint64_t AddCountForRange(const Slice& first, const Slice& last, uint64_t c);

  /* Estimate (as precisely as possible) the number of occurences
   * of the provided key this histogram has seen 
   */
  virtual uint64_t EstimateCount(const Slice& key) override;

  /* Estimates (as precisely as possible) the number of occurences
   * of hits this histogram has seen for _all_ keys between first
   * and last inclusive
   */
  virtual uint64_t EstimateCountForRange(const Slice& first, const Slice& last);

  /* Get the total count for whichever bucket this key belongs to */
  virtual uint64_t GetBucketCount(const Slice& k) override;

  /* Merge the data from another histogram into this one */
  virtual void Merge(Histogram* other) override;

  // Mean
  virtual uint64_t LowestNAverageCount(double pct) override;
  virtual uint64_t GetAverageBucketCount() override;
  // Median
  virtual uint64_t GetMedianBucketCount() override;
  // Mode
  virtual uint64_t GetMaxBucketCount(std::pair<Slice,Slice>* p) override;
  virtual uint64_t GetMinBucketCount(std::pair<Slice,Slice>* p) override;

  inline virtual uint64_t GetTotal() override { return total_.load(); }

  void Display();

 private:
  // No copying
  StaticHistogram(const StaticHistogram& other) = delete;
  StaticHistogram& operator=(const StaticHistogram& other) = delete;

};
} // namespace leveldb

#endif // STATIC_HISTOGRAM_H_
