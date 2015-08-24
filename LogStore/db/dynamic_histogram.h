#ifndef DYNAMIC_HISTOGRAM_H_
#define DYNAMIC_HISTOGRAM_H_

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

namespace leveldb {

class SubBucket;
class Bucket;

class DynamicHistogram /*: public Histogram*/ {
 private:
  enum Granularity {
    COARSE,
    MIXED,
    FINE
  };

  /* ---------------------------- SUB-BUCKET ---------------------------- */
  struct SubBucket {
   private:
    std::string ss_;
    std::string sl_;
    Slice smallest_;
    Slice largest_;
    std::atomic<uint64_t> count_;

   public:
    SubBucket(Slice s, Slice l, uint64_t count)
      : ss_(s.data(), s.size()), sl_(l.data(), l.size()), count_(count) 
    {
      smallest_ = Slice(ss_);
      largest_ = Slice(sl_);
    }
    SubBucket(Slice s, Slice l) : SubBucket(s, l, 0) {}
    SubBucket(SubBucket&& other) noexcept 
      : ss_(std::move(other.ss_)), sl_(std::move(other.sl_)), count_(other.count_.load())
    {
      smallest_ = Slice(ss_);
      largest_ = Slice(sl_);
    }

    SubBucket& operator=(SubBucket&& other) noexcept {
      ss_ = std::move(other.ss_);
      sl_ = std::move(other.sl_);
      smallest_ = Slice(ss_);
      largest_ = Slice(sl_);
      count_.store(other.count_.load());
      return *this;
    }

    const Slice& Smallest() const { return smallest_; }
    Slice& Smallest(const Slice& s) { 
      ss_ = std::string(s.data(), s.size());
      smallest_ = Slice(ss_);
      return smallest_;
    }
    const Slice& Largest() const { return largest_; }
    uint64_t GetCount() const { return count_.load(); }
    uint64_t AddCount() { return ++count_; }
    void SetCount(uint64_t c) { count_.store(c); }
    void DEBUG() const { 
#ifndef NDEBUG
      std::cout << "SB[" << smallest_.ToString() << "-" << largest_.ToString() << "," << count_.load() << std::endl;
#endif
    }

   private:
    // no copying
    SubBucket(const SubBucket& other) = delete;
    SubBucket& operator=(const SubBucket& other) = delete;
  };

  /* ---------------------------- BUCKET ---------------------------- */
  class Bucket {
   private:
    const Comparator* key_cmp_;
    Granularity granularity_;
    Slice smallest_;
    Slice largest_;
    uint64_t last_access_time_;
    std::atomic<uint64_t> count_;
    std::vector<SubBucket> sub_buckets_;

   public:
    Bucket(const Comparator* key_cmp, Slice s, Slice l, uint64_t count)
      : key_cmp_(key_cmp), granularity_(COARSE),
        smallest_(s), largest_(l), count_(count)
    {
      sub_buckets_.emplace_back(s, l, count_);
    }
    Bucket(const Comparator* key_cmp, Slice s, Slice l): Bucket(key_cmp, s, l, 0) {}
    Bucket(Slice s, Slice l): Bucket(nullptr, s, l) {} // for searching only!
    Bucket(Bucket&& other) noexcept 
      : key_cmp_(other.key_cmp_), smallest_(other.smallest_), largest_(other.largest_),
        count_(other.count_.load()), sub_buckets_(std::move(other.sub_buckets_))
    {}

    Bucket& operator=(Bucket&& other) noexcept {
      key_cmp_ = other.key_cmp_;
      smallest_ = other.smallest_;
      largest_ = other.largest_;
      granularity_ = other.granularity_;
      count_.store(other.count_.load());
      sub_buckets_ = std::move(other.sub_buckets_);
      return *this;
    }

    const Slice& Smallest() const { return smallest_; }
    const Slice& Largest() const { return largest_; }
    Granularity GetGranularity() const { return granularity_; }
    uint64_t EstimateCount(const Slice& k);
    uint64_t EstimateCountForRange(const Slice& s, const Slice& e);
    uint64_t EstimateCountGreaterThan(const Slice& k) { 
      return EstimateCountForRange(k, largest_); 
    }
    uint64_t EstimateCountLessThan(const Slice& k) { 
      return EstimateCountForRange(smallest_, k); 
    }
    uint64_t GetBucketCount() const { return count_.load(); }
    uint64_t AddCount(const Slice& k);
    size_t NumSubBuckets() const { return sub_buckets_.size(); }
    void Promote();
    void Demote();
    void DEBUG() const;
   private:
    // no copying
    Bucket(const Bucket& other) = delete;
    Bucket& operator=(const Bucket& other) = delete;
  };

  /* ---------------------------- BUCKET ---------------------------- */
  /* Bucket Comparator enables sorting buckets by their total counts  */
  struct BucketComparator {
    bool operator()(const Bucket* b1, const Bucket* b2) {
      return b1->GetBucketCount() < b2->GetBucketCount();
    }
  };

  //
  const Options* const options_;

  uint32_t mixed_threshold_count_;
  double mixed_threshold_fraction_;

  uint32_t fine_threshold_count_;
  double fine_threshold_fraction_;

  uint32_t max_sub_buckets_count_;

  /* 
   * The buckets that make up this histogram.  The buckets are
   * sorted by the user keys in ascending order, but their
   * widths may not be equal.
   */
  std::vector<Bucket> buckets_;

  // we keep buckets that use fine and mixed granularities in separate heaps
  // each heap points to buckets stored in the buckets_ member variabe
  std::vector<Bucket*> fine_buckets_;
  std::vector<Bucket*> mixed_buckets_;
  //std::priority_queue<Bucket*,std::vector<Bucket*>,BucketComparator> fine_buckets_;
  //std::priority_queue<Bucket*,std::vector<Bucket*>,BucketComparator> mixed_buckets_;

  /* The comparator we use to determine the order of user keys */
  const Comparator* key_cmp_;

  /* The comparator to sort buckets by their access counts */
  BucketComparator bucket_cmp_;

  bool CanPromote(Bucket* bucket);

  void PromoteBucket(Bucket* bucket);

  void DemoteBucket(Bucket* bucket);

 public:
  /* The constructor */
  DynamicHistogram(const Options* options,
                   std::vector<std::pair<Slice, Slice>>* buckets, 
                   const Comparator* cmp);
  DynamicHistogram(const Options* options,
                   std::vector<FileMetaData*>* files,
                   Histogram* base,
                   const Comparator* cmp);
  /*virtual*/ ~DynamicHistogram() /*override*/ {}

  /* Add an access count to the bucket for the provided key */
  /*virtual*/ uint64_t AddCount(const Slice& key);// override;

  /* Estimate (as precisely as possible) the number of occurences
   * of the provided key this histogram has seen 
   */
  /*virtual*/ uint64_t EstimateCount(const Slice& key);// override;

  /* Estimates (as precisely as possible) the number of occurences
   * of hits this histogram has seen for _all_ keys between first
   * and last inclusive
   */
  /*virtual*/ uint64_t EstimateCountForRange(const Slice& first, const Slice& last);

  /* Get the total count for whichever bucket this key belongs to */
  /*virtual*/ uint64_t GetBucketCount(const Slice& k);// override;

  /* Merge the data from another histogram into this one */
  /*virtual*/ void Merge(Histogram* other);// override;

  void DEBUG() const;

};
} // namespace leveldb

#endif // DYNAMIC_HISTOGRAM_H_
