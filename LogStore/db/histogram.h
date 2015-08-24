#ifndef HISTOGRAM_H_
#define HISTOGRAM_H_

#include "leveldb/slice.h"

namespace leveldb {

class Histogram {
 public:
  Histogram() {}
  virtual ~Histogram() {}
  virtual uint64_t EstimateCount(const Slice& key) = 0;
  virtual uint64_t EstimateCountForRange(const Slice& first, const Slice& last) = 0;
  virtual uint64_t GetBucketCount(const Slice& key) = 0;
  virtual uint64_t AddCount(const Slice& key, uint64_t c) = 0;
  virtual uint64_t AddCountForRange(const Slice& first, const Slice& last, uint64_t c) = 0;
  virtual void Merge(Histogram* other) = 0;
  virtual uint64_t GetTotal() = 0;
  // Mean
  virtual uint64_t LowestNAverageCount(double pct) = 0;
  virtual uint64_t GetAverageBucketCount() = 0;
  // Median
  virtual uint64_t GetMedianBucketCount() = 0;
  // Mode
  virtual uint64_t GetMaxBucketCount(std::pair<Slice,Slice>* p) = 0;
  virtual uint64_t GetMinBucketCount(std::pair<Slice,Slice>* p) = 0;

 private:
  Histogram(const Histogram& o) = delete;
  Histogram& operator=(const Histogram& o) = delete;
};

} // namespace

#endif // HISTOGRAM_H_
