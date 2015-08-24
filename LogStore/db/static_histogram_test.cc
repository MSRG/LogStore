#include "db/static_histogram.h"

#include <thread>
#include <unistd.h>
#include <vector>
#include "util/testharness.h"
#include "util/scrambled_zipfian.h"
#include "util/zipfian.h"

namespace leveldb {

#define MIN_KEY 1000000
#define MAX_KEY 9800000
#define NUM_KEYS (MAX_KEY - MIN_KEY)

class StaticHistogramTest {
 public:
  uint32_t num_buckets_;
  Random rand_;
  Options options_;
  StaticHistogram *hist_;
  std::vector<std::pair<std::string, std::string>> boundaries;

  StaticHistogramTest(): num_buckets_(1000), rand_(test::RandomSeed()) {
    uint32_t keys_per_bucket = NUM_KEYS / num_buckets_;
    for (int i = 0; i < num_buckets_; i++) {
      uint32_t start = i * keys_per_bucket + MIN_KEY;
      uint32_t end = start + keys_per_bucket - 1;
      //boundaries.emplace_back(std::to_string(start), std::to_string(end));
      boundaries.emplace_back(Key(start), Key(end));
    }
    hist_ = NewHistogram(&boundaries);
  }

  ~StaticHistogramTest() {
    if (hist_) 
      delete hist_;
  }

  inline uint32_t num_buckets() const { return num_buckets_; }
  inline Random& random() { return rand_; }
  inline Random new_random() { return Random(options_.env->NowMicros()); }
  inline StaticHistogram * histogram() { return hist_; }

  std::string Key(int i) {
    char buf[100];
    snprintf(buf, sizeof(buf), "%016d", i);
    return std::string(buf);
  }

  void AddCount(Slice key) {
    histogram()->AddCount(key, 1);
  }

  StaticHistogram* NewHistogram(std::vector<std::pair<std::string,std::string>>* bs) const {
    std::vector<std::pair<Slice, Slice>> buckets;
    buckets.reserve(bs->size());
    for (auto& p : (*bs)) {
      buckets.emplace_back(Slice(p.first), Slice(p.second));
    }
    return new StaticHistogram(&buckets, BytewiseComparator());
  }

  void PrintStats(StaticHistogram* hist) {
    std::pair<Slice,Slice> min,max;
    uint64_t min_count = hist->GetMinBucketCount(&min);
    uint64_t max_count = hist->GetMaxBucketCount(&max);
    uint64_t avg_count = hist->GetAverageBucketCount();
    std::cout << "Lowest 10% average: " << hist->LowestNAverageCount(0.1) << std::endl;
    std::cout << "Lowest 20% average: " << hist->LowestNAverageCount(0.2) << std::endl;
    std::cout << "Lowest 30% average: " << hist->LowestNAverageCount(0.3) << std::endl;
    std::cout << "Lowest 40% average: " << hist->LowestNAverageCount(0.4) << std::endl;
    std::cout << "Lowest 50% average: " << hist->LowestNAverageCount(0.5) << std::endl;
    std::cout << "Lowest 60% average: " << hist->LowestNAverageCount(0.6) << std::endl;
    std::cout << "Lowest 70% average: " << hist->LowestNAverageCount(0.7) << std::endl;
    std::cout << "Lowest 80% average: " << hist->LowestNAverageCount(0.8) << std::endl;
    std::cout << "Lowest 90% average: " << hist->LowestNAverageCount(0.9) << std::endl;
    std::cout << "Lowest 100% average: " << hist->LowestNAverageCount(1.0) << std::endl;
    std::cout << "Histogram\n\t";
    std::cout << "Min: " << min_count << " @ " << min.first.ToString() << "-" << min.second.ToString();
    std::cout << "\n\tMax: " << max_count << " @ " << max.first.ToString() << "-" << max.second.ToString();
    std::cout << "\n\tAverage: " << avg_count << std::endl;
  }

  void PrintStats() { PrintStats(histogram()); }
};

/*
TEST(StaticHistogramTest, UniformAccessPattern) {
  uint32_t num_hits = 2000000;
  for (int i = 0; i < num_hits; i++) {
    int key = random().Uniform(NUM_KEYS) + MIN_KEY;
    AddCount(Key(key));
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
  histogram()->Display();
  PrintStats();
  fprintf(stderr, "Total Count = %d\n", total_count);
  ASSERT_EQ(total_count, num_hits);
}

TEST(StaticHistogramTest, ZipfianAccessPattern) {
  ZipfianGenerator zip(random(), NUM_KEYS);
  uint32_t num_hits = 2000000;
  for (unsigned i = 0; i < num_hits; i++) {
    int key = zip.NextInt() + MIN_KEY;
    AddCount(Key(key));
  }

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
      assert(rise_length < 100);
      rise_length++;
    } else if (bucket_count < last_count) {
      rise_length = 0;
    }
    last_count = bucket_count;
    total_count += bucket_count;
  }
  histogram()->Display();
  PrintStats();
  fprintf(stderr, "Total Count = %d\n", total_count);
  ASSERT_EQ(total_count, num_hits);
}

TEST(StaticHistogramTest, Merging) {
  ZipfianGenerator zip(random(), NUM_KEYS);

  fprintf(stderr, "Inserting zipfian data into first histogram ...\n");
  uint32_t num_hits = 2000000;
  for (unsigned i = 0; i < num_hits; i++) {
    int key = zip.NextInt() + MIN_KEY;
    AddCount(Key(key));
  }

  fprintf(stderr, "Creating new histogram and merging ...\n");

  // insert in latest-key-first order
  auto hist2 = NewHistogram(&boundaries);
  hist2->Merge(histogram());

  fprintf(stderr, "Inserting in latest order into merged histogram ...\n");

  // insert in latest-key-first order
  for (unsigned i = 0; i < num_hits; i++) {
    int key = MAX_KEY - zip.NextInt() - 1;
    hist2->AddCount(Key(key), 1);
  }

  fprintf(stderr, "Checking merged histogram for correctness ...\n");

  uint32_t keys_per_bucket = NUM_KEYS / num_buckets();
  uint32_t total_count = 0;
  for (int i = 0; i < num_buckets(); i++) {
    int key = (i * keys_per_bucket + (keys_per_bucket / 2)) + MIN_KEY;
    uint64_t bucket_count = hist2->GetBucketCount(Key(key));
    total_count += bucket_count;
  }
  hist2->Display();
  PrintStats();
  fprintf(stderr, "Total Count = %d\n", total_count);
  //ASSERT_EQ(total_count, 2*num_hits);
}

TEST(StaticHistogramTest, MinMaxMean) {
  ZipfianGenerator zip(random(), NUM_KEYS);
  fprintf(stderr, "Inserting zipfian data into first histogram ...\n");
  uint32_t num_hits = 2000000;
  for (unsigned i = 0; i < num_hits; i++) {
    int key = zip.NextInt() + MIN_KEY;
    AddCount(Key(key));
  }
  PrintStats();
}

TEST(StaticHistogramTest, ScrambledZipfianAccessPattern) {
  ScrambledZipfianGenerator zip(random(), NUM_KEYS);
  uint32_t num_hits = 2000000;
  for (unsigned i = 0; i < num_hits; i++) {
    int key = zip.NextInt() + MIN_KEY;
    AddCount(Key(key));
  }

  histogram()->Display();
  PrintStats();
}

TEST(StaticHistogramTest, SmallestKeyWhenMinimumCountMatch) {
  uint32_t keys_per_bucket = (MAX_KEY - MIN_KEY) / num_buckets();
  uint32_t num_hits = 2000000;
  for (int i = 0; i < num_hits; i++) {
    int key = fmin(random().Uniform(NUM_KEYS) + MIN_KEY, MAX_KEY - (4*keys_per_bucket-1));
    AddCount(Key(key));
  }

  histogram()->Display();
  PrintStats();
}

TEST(StaticHistogramTest, LowestNAverageCountWithUniform) {
  uint32_t num_hits = 2000000;
  for (int i = 0; i < num_hits; i++) {
    int key = random().Uniform(NUM_KEYS) + MIN_KEY;
    AddCount(Key(key));
  }
  PrintStats();
  histogram()->Display();
}

TEST(StaticHistogramTest, LowestNAverageCountWithZipfian) {
  ZipfianGenerator zip(random(), NUM_KEYS);
  uint32_t num_hits = 2000000;
  for (unsigned i = 0; i < num_hits; i++) {
    int key = zip.NextInt() + MIN_KEY;
    AddCount(Key(key));
  }

  PrintStats();
  histogram()->Display();
}
*/

TEST(StaticHistogramTest, ZipfianDistribution) {
  std::vector<std::pair<std::string, std::string>> bs;
  int start = 0;
  int end = 100000000;
  int num_keys = end - start;
  int num_buckets = 12500;
  int keys_per_bucket = num_keys / num_buckets;
  for (unsigned i = 0; i < num_buckets; i++) {
    int low = i * keys_per_bucket;
    int high = low + keys_per_bucket - 1;
    bs.emplace_back(Key(low), Key(high));
  }

  auto hist = NewHistogram(&bs);
  int num_hits = 4000000;
  int num_runs = 2;

  for (unsigned run = 0; run < num_runs; run++) {
    Random r = new_random();
    ZipfianGenerator zipf(r, num_keys);
    for (unsigned i = 0; i < num_hits; i++) {
      int k = zipf.NextInt();
      hist->AddCount(Key(k), 1);
    }
  }
  hist->Display();
  PrintStats(hist);
}

/*
TEST(StaticHistogramTest, ConcurrentZipfianDistribution) {
  std::vector<std::pair<std::string, std::string>> bs;
  int start = 0;
  int end = 1000000;
  int num_keys = end - start;
  int num_buckets = 5000;
  int keys_per_bucket = num_keys / num_buckets;
  for (unsigned i = 0; i < num_buckets; i++) {
    int low = i * keys_per_bucket;
    int high = low + keys_per_bucket - 1;
    bs.emplace_back(Key(low), Key(high));
  }

  auto hist = NewHistogram(&bs);
  int nthreads = 4;
  int nruns = 2;
  int req_per_run = 1000000;

  std::function<void(void)> f = [&](void) {
    for (unsigned run = 0; run < nruns; run++) {
      Random r = new_random();
      ZipfianGenerator zipf(r, num_keys); 
      for (unsigned i = 0; i < req_per_run; i++) {
        int k = zipf.NextInt();
        hist->AddCount(Key(k), 1);
        if (i % 5000 == 0) {
          hist->GetMinBucketCount(nullptr);
        }
      }
    }
  };
  std::vector<std::thread> threads;
  for (unsigned i = 0; i < nthreads; i++) {
    threads.emplace_back(f); 
    usleep(random().Next() % 200);
  }
  for (unsigned i = 0; i < nthreads; i++) {
    threads[i].join();
  }

  hist->Display();
  PrintStats(hist);
}

TEST(StaticHistogramTest, AddToBucket) {
  std::string my_key = Key(MAX_KEY-1);
  for (unsigned i = 0; i < 50; i++) {
    histogram()->AddCount(my_key, 1);
  }
  ASSERT_EQ(50, histogram()->GetBucketCount(my_key));
  histogram()->Display();
}
*/

}  // namespace leveldb

int main(int argc, char** argv) {
  return leveldb::test::RunAllTests();
}
