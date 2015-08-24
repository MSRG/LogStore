#include "db/dynamic_histogram.h"

#include <vector>
#include "util/testharness.h"
#include "util/zipfian.h"

namespace leveldb {

#define MIN_KEY 100000
#define MAX_KEY 980000
#define NUM_KEYS (MAX_KEY - MIN_KEY)

class DynamicHistogramTest {
 public:
  uint32_t num_buckets_;
  Random rand_;
  Options options_;
  DynamicHistogram *hist_;
  std::vector<std::pair<std::string, std::string>> boundaries;

  DynamicHistogramTest(): num_buckets_(1000), rand_(test::RandomSeed()) {
    uint32_t keys_per_bucket = NUM_KEYS / num_buckets_;
    for (int i = 0; i < num_buckets_; i++) {
      uint32_t start = i * keys_per_bucket + MIN_KEY;
      uint32_t end = start + keys_per_bucket - 1;
      //boundaries.emplace_back(std::to_string(start), std::to_string(end));
      boundaries.emplace_back(Key(start), Key(end));
    }
    hist_ = NewHistogram(&boundaries);
  }

  ~DynamicHistogramTest() {
    if (hist_) 
      delete hist_;
  }

  inline uint32_t num_buckets() const { return num_buckets_; }
  inline Random * random() { return &rand_; }
  inline DynamicHistogram * histogram() { return hist_; }

  std::string Key(int i) {
    char buf[100];
    snprintf(buf, sizeof(buf), "key%06d", i);
    return std::string(buf);
  }

  void AddCount(const char* key) {
    histogram()->AddCount(key);
  }

  DynamicHistogram* NewHistogram(std::vector<std::pair<std::string,std::string>>* bs) const {
    std::vector<std::pair<Slice, Slice>> buckets;
    buckets.reserve(bs->size());
    for (auto& p : (*bs)) {
      buckets.emplace_back(Slice(p.first), Slice(p.second));
    }
    return new DynamicHistogram(&options_, &buckets, BytewiseComparator());
  }
};

TEST(DynamicHistogramTest, UniformAccessPattern) {
  uint32_t num_hits = 2000000;
  for (int i = 0; i < num_hits; i++) {
    int key = random()->Uniform(NUM_KEYS) + MIN_KEY;
    histogram()->AddCount(Key(key));
  }

  uint32_t avg_bucket_count = num_hits / num_buckets();
  uint32_t keys_per_bucket = (MAX_KEY - MIN_KEY) / num_buckets();
  uint32_t total_count = 0;
  for (int i = 0; i < num_buckets(); i++) {
    int key = (i * keys_per_bucket + (keys_per_bucket / 2)) + MIN_KEY;
    uint64_t bucket_count = histogram()->GetBucketCount(Key(key));
    total_count += bucket_count;
    ASSERT_TRUE(abs(bucket_count - avg_bucket_count) < 200);
  }
  fprintf(stderr, "Total Count = %d\n", total_count);
  ASSERT_EQ(total_count, num_hits);
}

TEST(DynamicHistogramTest, ZipfianAccessPattern) {
  ZipfianGenerator zip(NUM_KEYS);
  uint32_t num_hits = 2000000;
  for (unsigned i = 0; i < num_hits; i++) {
    int key = zip.nextInt() + MIN_KEY;
    histogram()->AddCount(Key(key));
    //fprintf(stderr, "insertion #%d with key %d\n", i, key);
  }

  histogram()->DEBUG();
  fprintf(stderr, "\n\n\n");

  // make sure the bucket_counts generall decrease as we move from
  // towards the tail end of the Zipf distribution.  While the counts
  // should not be strictly decreasing, we just ensure that they don't
  // increase between 5 successive buckets
  uint32_t keys_per_bucket = NUM_KEYS / num_buckets();
  uint32_t total_count = 0;
  uint64_t last_count = 0;
  uint32_t rise_length = 0;
  for (int i = 0; i < num_buckets(); i++) {
    int key = (i * keys_per_bucket + (keys_per_bucket / 2)) + MIN_KEY;
    uint64_t bucket_count = histogram()->GetBucketCount(Key(key));
    if (i > 0 && bucket_count > last_count) {
      assert(rise_length < 5);
      rise_length++;
    } else if (bucket_count < last_count) {
      rise_length = 0;
    }
    last_count = bucket_count;
    total_count += bucket_count;
    //fprintf(stderr, "Bucket %d has count %llu\n", i, bucket_count);
  }
  fprintf(stderr, "Total Count = %d\n", total_count);
  //histogram()->DEBUG();
  ASSERT_EQ(total_count, num_hits);
}

TEST(DynamicHistogramTest, SimpleZipfianAndLatest) {
  ZipfianGenerator zip(NUM_KEYS);

  fprintf(stderr, "Inserting zipfian data into histogram ...\n");
  uint32_t num_hits = 2000000;
  for (unsigned i = 0; i < num_hits; i++) {
    int key = zip.nextInt() + MIN_KEY;
    histogram()->AddCount(Key(key));
  }

  fprintf(stderr, "Starting reverse access ...\n");

  // insert in latest-key-first order
  for (unsigned i = 0; i < num_hits; i++) {
    int key = MAX_KEY - zip.nextInt() - 1;
    histogram()->AddCount(Key(key));
  }

  fprintf(stderr, "Starting histogram check ...\n");
  
  uint32_t keys_per_bucket = NUM_KEYS / num_buckets();
  uint32_t total_count = 0;
  for (int i = 0; i < num_buckets(); i++) {
    int key = (i * keys_per_bucket + (keys_per_bucket / 2)) + MIN_KEY;
    uint64_t bucket_count = histogram()->GetBucketCount(Key(key));
    total_count += bucket_count;
  }
  fprintf(stderr, "Total Count = %d\n", total_count);
  ASSERT_EQ(total_count, 2*num_hits);
}

TEST(DynamicHistogramTest, Merging) {
  ZipfianGenerator zip(NUM_KEYS);

  fprintf(stderr, "Inserting zipfian data into first histogram ...\n");
  uint32_t num_hits = 2000000;
  for (unsigned i = 0; i < num_hits; i++) {
    int key = zip.nextInt() + MIN_KEY;
    histogram()->AddCount(Key(key));
  }

  fprintf(stderr, "Creating new histogram and merging ...\n");

  // insert in latest-key-first order
  auto hist2 = NewHistogram(&boundaries);
  hist2->Merge(histogram());

  fprintf(stderr, "Checking merged histogram for correctness ...\n");

  // make sure the bucket_counts generall decrease as we move from
  // towards the tail end of the Zipf distribution.  While the counts
  // should not be strictly decreasing, we just ensure that they don't
  // increase between 5 successive buckets
  uint32_t keys_per_bucket = NUM_KEYS / num_buckets();
  uint32_t total_count = 0;
  uint64_t last_count = 0;
  uint32_t rise_length = 0;
  for (int i = 0; i < num_buckets(); i++) {
    int key = (i * keys_per_bucket + (keys_per_bucket / 2)) + MIN_KEY;
    uint64_t bucket_count = hist2->GetBucketCount(Key(key));
    if (i > 0 && bucket_count > last_count) {
      assert(rise_length < 5);
      rise_length++;
    } else if (bucket_count < last_count) {
      rise_length = 0;
    }
    last_count = bucket_count;
    total_count += bucket_count;
  }
  fprintf(stderr, "Total Count = %d\n", total_count);
  ASSERT_EQ(total_count, num_hits);
}

}  // namespace leveldb

int main(int argc, char** argv) {
  return leveldb::test::RunAllTests();
}
