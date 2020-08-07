// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

//#include <fstream>
#include <fcntl.h>
#include <memory>
#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <vector>
#include <time.h>
#include <dirent.h>
#include <algorithm>
#include "db/db_impl.h"
#include "db/version_set.h"
#include "leveldb/cache.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/write_batch.h"
#include "port/port.h"
#include "util/crc32c.h"
#include "util/histogram.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "util/scrambled_zipfian.h"
#include "util/testutil.h"
#include "util/zipfian.h"

// Comma-separated list of operations to run in the specified order
//   Actual benchmarks:
//      fillseq       -- write N values in sequential key order in async mode
//      fillrandom    -- write N values in random key order in async mode
//      overwrite     -- overwrite N values in random key order in async mode
//      fillsync      -- write N/100 values in random key order in sync mode
//      fill100K      -- write N/1000 100K values in random order in async mode
//      deleteseq     -- delete N keys in sequential order
//      deleterandom  -- delete N keys in random order
//      readseq       -- read N times sequentially
//      readreverse   -- read N times in reverse order
//      readrandom    -- read N times in random order
//      readmissing   -- read N missing keys in random order
//      readhot       -- read N times in random order from 1% section of DB
//      seekrandom    -- N random seeks
//      crc32c        -- repeated crc32c of 4K of data
//      acquireload   -- load N*1000 times
//   Meta operations:
//      compact     -- Compact the entire DB
//      stats       -- Print DB stats
//      sstables    -- Print sstable info
//      heapprofile -- Dump a heap profile (if supported by this port)
static const char* FLAGS_benchmarks =
    "fillseq,"
    "fillsync,"
    "fillrandom,"
    "overwrite,"
    "readrandom,"
    "readrandom,"  // Extra run to allow previous compactions to quiesce
    "readseq,"
    "readreverse,"
    "compact,"
    "readrandom,"
    "readseq,"
    "readreverse,"
    "fill100K,"
    "crc32c,"
    "snappycomp,"
    "snappyuncomp,"
    "acquireload,"
    ;

// Number of key/values to place in database
static int FLAGS_num = 1000000;

// The length of gaps between keys with using fillgap
static int FLAGS_gap_len = -1;

// Number of read operations to do.  If negative, do FLAGS_num reads.
static int FLAGS_reads = -1;

// Number of concurrent threads to run.
static int FLAGS_threads = 1;

// Size of each value
static int FLAGS_value_size = 100;

// Arrange to generate values that shrink to this fraction of
// their original size after compression
static double FLAGS_compression_ratio = 0.5;

// Print histogram of operation timings
static bool FLAGS_histogram = false;

// Number of bytes to buffer in memtable before compacting
// (initialized to default value by "main")
static int FLAGS_write_buffer_size = 0;

// Number of bytes to use as a cache of uncompressed data.
// Negative means use default settings.
static long FLAGS_cache_size = -1;

// Maximum number of files to keep open at the same time (use default if == 0)
static int FLAGS_open_files = 0;

// Bloom filter bits per key.
// Negative means use default settings.
static int FLAGS_bloom_bits = -1;

// If true, do not destroy the existing database.  If you set this
// flag and also specify a benchmark that wants a fresh database, that
// benchmark will fail.
static bool FLAGS_use_existing_db = false;

// Use the db with the following name.
static const char* FLAGS_db = NULL;

// Zipfian skew parameter (s)
static double FLAGS_zipf_skew = 0.9;

// After how many completed options should stats be printed
static int FLAGS_stats_interval = 0;

// Ratio of total data that should be warmed before workload
static double FLAGS_warm_ratio = 0.1;

// Should we collect performance statistics
static bool FLAGS_use_statistics = true;

// When Read/Write workload, use this to set read percentage. The 
// write percentage is what's left over
static double FLAGS_read_pct = 0.9;

static char* FLAGS_ssd_cache_dir = NULL;
static double FLAGS_ssd_cache_size = 5000 * 1048576.0; // 50GB cache

namespace leveldb {

namespace {

// Helper for quickly generating random data.
class RandomGenerator {
 private:
  std::string data_;
  int pos_;

 public:
  RandomGenerator() {
    // We use a limited amount of data over and over again and ensure
    // that it is larger than the compression window (32KB), and also
    // large enough to serve all typical value sizes we want to write.
    Random rnd(301);
    std::string piece;
    while (data_.size() < 1048576) {
      // Add a short fragment that is as compressible as specified
      // by FLAGS_compression_ratio.
      test::CompressibleString(&rnd, FLAGS_compression_ratio, 100, &piece);
      data_.append(piece);
    }
    pos_ = 0;
  }

  Slice Generate(size_t len) {
    if (pos_ + len > data_.size()) {
      pos_ = 0;
      assert(len < data_.size());
    }
    pos_ += len;
    return Slice(data_.data() + pos_ - len, len);
  }
};

static Slice TrimSpace(Slice s) {
  size_t start = 0;
  while (start < s.size() && isspace(s[start])) {
    start++;
  }
  size_t limit = s.size();
  while (limit > start && isspace(s[limit-1])) {
    limit--;
  }
  return Slice(s.data() + start, limit - start);
}

static void AppendWithSpace(std::string* str, Slice msg) {
  if (msg.empty()) return;
  if (!str->empty()) {
    str->push_back(' ');
  }
  str->append(msg.data(), msg.size());
}

class Stats {
 private:
  int id_;
  double start_;
  double finish_;
  double seconds_;
  int done_;
  int last_report_done_;
  int next_report_;
  int64_t bytes_;
  double last_op_finish_;
  double last_report_finish_;
  int last_div_;
  HistogramImpl hist_;
  std::string message_;

 public:
  Stats() { Start(); }

  void Start() {
    next_report_ = FLAGS_stats_interval ? FLAGS_stats_interval : 100;
    last_op_finish_ = start_;
    hist_.Clear();
    done_ = 0;
    last_report_done_ = 0;
    bytes_ = 0;
    seconds_ = 0;
    start_ = Env::Default()->NowMicros();
    finish_ = start_;
    last_report_finish_ = start_;
    last_div_ = 0;
    message_.clear();
  }

  void Merge(const Stats& other) {
    hist_.Merge(other.hist_);
    done_ += other.done_;
    bytes_ += other.bytes_;
    seconds_ += other.seconds_;
    if (other.start_ < start_) start_ = other.start_;
    if (other.finish_ > finish_) finish_ = other.finish_;

    // Just keep the messages from one thread
    if (message_.empty()) message_ = other.message_;
  }

  void Stop() {
    finish_ = Env::Default()->NowMicros();
    seconds_ = (finish_ - start_) * 1e-6;
  }

  void AddMessage(Slice msg) {
    AppendWithSpace(&message_, msg);
  }

  void SetId(int id) { id_ = id; }

  std::string TimeToString(uint64_t secondsSince1970) {
    const time_t seconds = (time_t)secondsSince1970;
    struct tm t;
    int maxsize = 64;
    std::string dummy;
    dummy.reserve(maxsize);
    dummy.resize(maxsize);
    char* p = &dummy[0];
    localtime_r(&seconds, &t);
    snprintf(p, maxsize,
           "%04d/%02d/%02d-%02d:%02d:%02d ",
           t.tm_year + 1900,
           t.tm_mon + 1,
           t.tm_mday,
           t.tm_hour,
           t.tm_min,
           t.tm_sec);
    return dummy;
  }

  void FinishedSingleOp(DB* db) {
    if (FLAGS_histogram) {
      double now = Env::Default()->NowMicros();
      double micros = now - last_op_finish_;
      hist_.Add(micros);
      if (micros > 20000) {
        fprintf(stderr, "long op: %.1f micros%30s\r", micros, "");
        fflush(stderr);
      }
      last_op_finish_ = now;
    }

    done_++;
    double now = Env::Default()->NowMicros();
    double time_since_finish = (now - last_report_finish_) / 1000000.0;
    // output report if we've performed enough opertions _or_ more than
    // five seconds have elapsed
    if (done_ >= next_report_ || time_since_finish >= 10) {
      if      (next_report_ < 1000)   next_report_ += 100;
      else if (next_report_ < 5000)   next_report_ += 500;
      else if (next_report_ < 10000)  next_report_ += 1000;
      else if (next_report_ < 50000)  next_report_ += 5000;
      else if (next_report_ < 100000) next_report_ += 10000;
      else if (next_report_ < 500000) next_report_ += 50000;
      else                            next_report_ += 100000;
      //fprintf(stderr, "... finished %d ops%30s\r", done_, "");
      fprintf(stderr,
        "%s ... thread %d: (%d,%d) ops and "
        "(%.1f,%.1f) ops/second in (%.6f,%.6f) seconds\n",
        TimeToString((uint64_t) now/1000000).c_str(),
        id_,
        done_ - last_report_done_, 
        done_,
        (done_ - last_report_done_) / ((now - last_report_finish_) / 1000000.0),
        done_ / ((now - start_) / 1000000.0),
        (now - last_report_finish_) / 1000000.0,
        (now - start_) / 1000000.0);

      int elapsed = static_cast<int>((now - start_) * 1e-6);
      if (elapsed / 500 > last_div_) {
        last_div_++;
        std::string stats;
        if (db && db->GetProperty("leveldb.stats", &stats))
          fprintf(stderr, "%s\n", stats.c_str());
      }

       fflush(stderr);
       next_report_ += FLAGS_stats_interval;
       last_report_finish_ = now;
       last_report_done_ = done_;
    }
  }

  void AddBytes(int64_t n) {
    bytes_ += n;
  }

  void Report(const Slice& name) {
    // Pretend at least one op was done in case we are running a benchmark
    // that does not call FinishedSingleOp().
    if (done_ < 1) done_ = 1;

    std::string extra;
    if (bytes_ > 0) {
      // Rate is computed on actual elapsed time, not the sum of per-thread
      // elapsed times.
      double elapsed = (finish_ - start_) * 1e-6;
      char rate[100];
      snprintf(rate, sizeof(rate), "%6.1f MB/s",
               (bytes_ / 1048576.0) / elapsed);
      extra = rate;
    }
    AppendWithSpace(&extra, message_);

    fprintf(stdout, "%-12s : %11.3f micros/op;%s%s\n",
            name.ToString().c_str(),
            seconds_ * 1e6 / done_,
            (extra.empty() ? "" : " "),
            extra.c_str());
    if (FLAGS_histogram) {
      fprintf(stdout, "Microseconds per op:\n%s\n", hist_.ToString().c_str());
    }
    fflush(stdout);
  }
};

// State shared by all concurrent executions of the same benchmark.
struct SharedState {
  port::Mutex mu;
  port::CondVar cv;
  int total;

  // Each thread goes through the following states:
  //    (1) initializing
  //    (2) waiting for others to be initialized
  //    (3) running
  //    (4) done

  int num_initialized;
  int num_done;
  bool start;

  SharedState() : cv(&mu) { }
};

// Per-thread state for concurrent executions of the same benchmark.
struct ThreadState {
  int tid;             // 0..n-1 when running in n threads
  Random rand;         // Has different seeds for different threads
  Stats stats;
  SharedState* shared;

  ThreadState(int seed, int index)
      : tid(index),
        rand(1000 + seed + index) {
    stats.SetId(tid);
  }
};

}  // namespace

class Benchmark {
 private:
  Cache* cache_;
  std::shared_ptr<Statistics> statistics_;
  const FilterPolicy* filter_policy_;
  DB* db_;
  int num_;
  int value_size_;
  int entries_per_batch_;
  WriteOptions write_options_;
  int reads_;
  int heap_counter_;
  time_t start_;
  std::vector<std::string> zipf_inputs;

#ifdef CFlagPregenZipfData
  int gen_zipf_counter;
#endif
#ifdef CFLagPregenZipfAvailableDataFiles
  std::vector<std::string> zipf_filelist;
  size_t filelist_ptr = 0;
#endif
  void PrintHeader() {
    const int kKeySize = 16;
    PrintEnvironment();
    fprintf(stdout, "DB Dir:     %s\n", FLAGS_db);
    fprintf(stdout, "SSD Cache Dir: %s\n", FLAGS_ssd_cache_dir);
    fprintf(stdout, "SSD Cache Size: %.1f\n", FLAGS_ssd_cache_size);
    fprintf(stdout, "Keys:       %d bytes each\n", kKeySize);
    fprintf(stdout, "Values:     %d bytes each (%d bytes after compression)\n",
            FLAGS_value_size,
            static_cast<int>(FLAGS_value_size * FLAGS_compression_ratio + 0.5));
    fprintf(stdout, "Entries:    %d\n", num_);
    fprintf(stdout, "RawSize:    %.1f MB (estimated)\n",
            ((static_cast<int64_t>(kKeySize + FLAGS_value_size) * num_)
             / 1048576.0));
    fprintf(stdout, "FileSize:   %.1f MB (estimated)\n",
            (((kKeySize + FLAGS_value_size * FLAGS_compression_ratio) * num_)
             / 1048576.0));
    fprintf(stdout, "Block Cache Size: %.1f MB (esimated)\n", FLAGS_cache_size < 0 ? 0 : 
            static_cast<double>(FLAGS_cache_size) / 1048576.0);
    fprintf(stdout, "Warm Ratio: %f\n", FLAGS_warm_ratio);
    fprintf(stdout, "Zipf Skew:  %.2f\n", FLAGS_zipf_skew);
    PrintWarnings();
    fprintf(stdout, "------------------------------------------------\n");
  }

  void PrintWarnings() {
#if defined(__GNUC__) && !defined(__OPTIMIZE__)
    fprintf(stdout,
            "WARNING: Optimization is disabled: benchmarks unnecessarily slow\n"
            );
#endif
#ifndef NDEBUG
    fprintf(stdout,
            "WARNING: Assertions are enabled; benchmarks unnecessarily slow\n");
#endif

    // See if snappy is working by attempting to compress a compressible string
    const char text[] = "yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy";
    std::string compressed;
    if (!port::Snappy_Compress(text, sizeof(text), &compressed)) {
      fprintf(stdout, "WARNING: Snappy compression is not enabled\n");
    } else if (compressed.size() >= sizeof(text)) {
      fprintf(stdout, "WARNING: Snappy compression is not effective\n");
    }
  }

  void PrintEnvironment() {
    fprintf(stderr, "LevelDB:    version %d.%d\n",
            kMajorVersion, kMinorVersion);

#if defined(__linux)
    time_t now = time(NULL);
    fprintf(stderr, "Date:       %s", ctime(&now));  // ctime() adds newline

    FILE* cpuinfo = fopen("/proc/cpuinfo", "r");
    if (cpuinfo != NULL) {
      char line[1000];
      int num_cpus = 0;
      std::string cpu_type;
      std::string cache_size;
      while (fgets(line, sizeof(line), cpuinfo) != NULL) {
        const char* sep = strchr(line, ':');
        if (sep == NULL) {
          continue;
        }
        Slice key = TrimSpace(Slice(line, sep - 1 - line));
        Slice val = TrimSpace(Slice(sep + 1));
        if (key == "model name") {
          ++num_cpus;
          cpu_type = val.ToString();
        } else if (key == "cache size") {
          cache_size = val.ToString();
        }
      }
      fclose(cpuinfo);
      fprintf(stderr, "CPU:        %d * %s\n", num_cpus, cpu_type.c_str());
      fprintf(stderr, "CPUCache:   %s\n", cache_size.c_str());
    }
#endif
  }

 public:
  Benchmark()
  : cache_(FLAGS_cache_size >= 0 ? NewLRUCache(FLAGS_cache_size) : NULL),
    statistics_(FLAGS_use_statistics ? CreateDBStatistics() : nullptr),
    filter_policy_(FLAGS_bloom_bits >= 0
                   ? NewBloomFilterPolicy(FLAGS_bloom_bits)
                   : NULL),
    db_(NULL),
    num_(FLAGS_num),
    value_size_(FLAGS_value_size),
    entries_per_batch_(1),
    reads_(FLAGS_reads < 0 ? FLAGS_num : FLAGS_reads),
    heap_counter_(0),
#ifdef CFlagPregenZipfData
    gen_zipf_counter(0),
#endif
    start_(time(nullptr)) {
    std::vector<std::string> files;
    Env::Default()->GetChildren(FLAGS_db, &files);
    for (size_t i = 0; i < files.size(); i++) {
      if (Slice(files[i]).starts_with("heap-")) {
        Env::Default()->DeleteFile(std::string(FLAGS_db) + "/" + files[i]);
      }
    }
    if (!FLAGS_use_existing_db) {
      DestroyDB(FLAGS_db, Options());
    }
  }

  ~Benchmark() {
    delete db_;
    delete cache_;
    delete filter_policy_;
  }

  void Run() {
    PrintHeader();
    Open();

    const char* benchmarks = FLAGS_benchmarks;
    while (benchmarks != NULL) {
      const char* sep = strchr(benchmarks, ',');
      Slice name;
      if (sep == NULL) {
        name = benchmarks;
        benchmarks = NULL;
      } else {
        name = Slice(benchmarks, sep - benchmarks);
        benchmarks = sep + 1;
      }

      // Reset parameters that may be overriddden bwlow
      num_ = FLAGS_num;
      reads_ = (FLAGS_reads < 0 ? FLAGS_num : FLAGS_reads);
      value_size_ = FLAGS_value_size;
      entries_per_batch_ = 1;
      write_options_ = WriteOptions();

      void (Benchmark::*method)(ThreadState*) = NULL;
      bool fresh_db = false;
      int num_threads = FLAGS_threads;

      if (name == Slice("fillseq")) {
        num_threads = 1;
        fresh_db = true;
        method = &Benchmark::WriteSeq;
      } else if (name == Slice("fillbatch")) {
        num_threads = 1;
        fresh_db = true;
        entries_per_batch_ = 1000;
        method = &Benchmark::WriteSeq;
      } else if (name == Slice("fillrandom")) {
        num_threads = 1;
        fresh_db = true;
        method = &Benchmark::WriteRandom;
      } else if (name == Slice("overwrite")) {
        fresh_db = false;
        method = &Benchmark::WriteRandom;
      } else if (name == Slice("fillsync")) {
        fresh_db = true;
        num_ /= 1000;
        write_options_.sync = true;
        method = &Benchmark::WriteRandom;
      } else if (name == Slice("fill100K")) {
        fresh_db = true;
        num_ /= 1000;
        value_size_ = 100 * 1000;
        method = &Benchmark::WriteRandom;
      } else if (name == Slice("fillgaps")) {
        num_threads = 1;
        if (FLAGS_gap_len < 0) {
          FLAGS_gap_len = FLAGS_num / 1000;
        }
        method = &Benchmark::WriteGaps;
      } else if (name == Slice("readseq")) {
        method = &Benchmark::ReadSequential;
      } else if (name == Slice("readreverse")) {
        method = &Benchmark::ReadReverse;
      } else if (name == Slice("readrandom")) {
        method = &Benchmark::ReadRandom;
      } else if (name == Slice("readmissing")) {
        method = &Benchmark::ReadMissing;
      } else if (name == Slice("seekrandom")) {
        method = &Benchmark::SeekRandom;
      } else if (name == Slice("readhot")) {
        method = &Benchmark::ReadHot;
      } else if (name == Slice("warmlogstore")) {
        num_threads = 1;
        method = &Benchmark::WarmLogStore;
      } else if (name == Slice("warmcache")) {
        num_threads = 1;
        method = &Benchmark::WarmCache;
      } else if (name == Slice("readzipf")) {
        method = &Benchmark::ReadZipfian;
      } else if (name == Slice("genzipfinput")) {
        GenerateZipfInput();
      } else if (name == Slice("readzipfinput")) {
        method = &Benchmark::ReadZipfInput;
      } else if (name == Slice("writezipf")) {
        method = &Benchmark::WriteZipf;
      } else if (name == Slice("readwritezipf")) {
        method = &Benchmark::ReadWriteZipf;
      } else if (name == Slice("ycsb")) {
          method = &Benchmark::YCSBWorkload;
      } else if (name == Slice("ycsbscan")) {
          method = &Benchmark::YCSBWorkloadScan;
      } else if (name == Slice("writerandom")) {
        num_ = FLAGS_reads;
        method = &Benchmark::WriteRandom;
      } else if (name == Slice("readlatest")) {
        method = &Benchmark::ReadLatest;
      } else if (name == Slice("readrandomsmall")) {
        reads_ /= 1000;
        method = &Benchmark::ReadRandom;
      } else if (name == Slice("deleteseq")) {
        method = &Benchmark::DeleteSeq;
      } else if (name == Slice("deleterandom")) {
        method = &Benchmark::DeleteRandom;
      } else if (name == Slice("readwhilewriting")) {
        num_threads++;  // Add extra thread for writing
        method = &Benchmark::ReadWhileWriting;
      } else if (name == Slice("compact")) {
        method = &Benchmark::Compact;
      } else if (name == Slice("crc32c")) {
        method = &Benchmark::Crc32c;
      } else if (name == Slice("acquireload")) {
        method = &Benchmark::AcquireLoad;
      } else if (name == Slice("snappycomp")) {
        method = &Benchmark::SnappyCompress;
      } else if (name == Slice("snappyuncomp")) {
        method = &Benchmark::SnappyUncompress;
      } else if (name == Slice("heapprofile")) {
        HeapProfile();
      } else if (name == Slice("stats")) {
        PrintStats("leveldb.stats");
        if (statistics_.get() != nullptr) {
          statistics_->clear();
        }
      } else if (name == Slice("sstables")) {
        PrintStats("leveldb.sstables");
      } else if (name == Slice("pause")) {
        int pause_time = 1 * 60 * 10; // in seconds
#ifdef PAUSE_PERIOD
        pause_time = PAUSE_PERIOD;
#endif
        printf("Pausing for %d seconds\n", pause_time);
        fflush(stdout);
        sleep(pause_time);
        fprintf(stderr, "Resume after pausing for %d seconds\n", pause_time);
      } else {
        if (name != Slice()) {  // No error message for empty name
          fprintf(stderr, "unknown benchmark '%s'\n", name.ToString().c_str());
        }
      }

      if (fresh_db) {
        if (FLAGS_use_existing_db) {
          fprintf(stdout, "%-12s : skipped (--use_existing_db is true)\n",
                  name.ToString().c_str());
          method = NULL;
        } else {
          delete db_;
          db_ = NULL;
          DestroyDB(FLAGS_db, Options());
          Open();
        }
      }

      if (method != NULL) {
        RunBenchmark(num_threads, name, method);
      }
    }
  }

 private:
  struct ThreadArg {
    Benchmark* bm;
    SharedState* shared;
    ThreadState* thread;
    void (Benchmark::*method)(ThreadState*);
  };

  static void ThreadBody(void* v) {
    ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
    SharedState* shared = arg->shared;
    ThreadState* thread = arg->thread;
    {
      MutexLock l(&shared->mu);
      shared->num_initialized++;
      if (shared->num_initialized >= shared->total) {
        shared->cv.SignalAll();
      }
      while (!shared->start) {
        shared->cv.Wait();
      }
    }

    thread->stats.Start();
    (arg->bm->*(arg->method))(thread);
    thread->stats.Stop();

    {
      MutexLock l(&shared->mu);
      shared->num_done++;
      if (shared->num_done >= shared->total) {
        shared->cv.SignalAll();
      }
    }
  }

  void RunBenchmark(int n, Slice name,
                    void (Benchmark::*method)(ThreadState*)) {
    SharedState shared;
    shared.total = n;
    shared.num_initialized = 0;
    shared.num_done = 0;
    shared.start = false;

    ThreadArg* arg = new ThreadArg[n];
    for (int i = 0; i < n; i++) {
      time_t now = time(0);
      arg[i].bm = this;
      arg[i].method = method;
      arg[i].shared = &shared;
      arg[i].thread = new ThreadState(difftime(now, start_), i);
      arg[i].thread->shared = &shared;
      Env::Default()->StartThread(ThreadBody, &arg[i]);
    }

    shared.mu.Lock();
    while (shared.num_initialized < n) {
      shared.cv.Wait();
    }

    shared.start = true;
    shared.cv.SignalAll();
    while (shared.num_done < n) {
      shared.cv.Wait();
    }
    shared.mu.Unlock();

    for (int i = 1; i < n; i++) {
      arg[0].thread->stats.Merge(arg[i].thread->stats);
    }
    arg[0].thread->stats.Report(name);

    for (int i = 0; i < n; i++) {
      delete arg[i].thread;
    }
    delete[] arg;
  }

  void Crc32c(ThreadState* thread) {
    // Checksum about 500MB of data total
    const int size = 4096;
    const char* label = "(4K per op)";
    std::string data(size, 'x');
    int64_t bytes = 0;
    uint32_t crc = 0;
    while (bytes < 500 * 1048576) {
      crc = crc32c::Value(data.data(), size);
      thread->stats.FinishedSingleOp(db_);
      bytes += size;
    }
    // Print so result is not dead
    fprintf(stderr, "... crc=0x%x\r", static_cast<unsigned int>(crc));

    thread->stats.AddBytes(bytes);
    thread->stats.AddMessage(label);
  }

  void AcquireLoad(ThreadState* thread) {
    int dummy;
    port::AtomicPointer ap(&dummy);
    int count = 0;
    void *ptr = NULL;
    thread->stats.AddMessage("(each op is 1000 loads)");
    while (count < 100000) {
      for (int i = 0; i < 1000; i++) {
        ptr = ap.Acquire_Load();
      }
      count++;
      thread->stats.FinishedSingleOp(db_);
    }
    if (ptr == NULL) exit(1); // Disable unused variable warning.
  }

  void SnappyCompress(ThreadState* thread) {
    RandomGenerator gen;
    Slice input = gen.Generate(Options().block_size);
    int64_t bytes = 0;
    int64_t produced = 0;
    bool ok = true;
    std::string compressed;
    while (ok && bytes < 1024 * 1048576) {  // Compress 1G
      ok = port::Snappy_Compress(input.data(), input.size(), &compressed);
      produced += compressed.size();
      bytes += input.size();
      thread->stats.FinishedSingleOp(db_);
    }

    if (!ok) {
      thread->stats.AddMessage("(snappy failure)");
    } else {
      char buf[100];
      snprintf(buf, sizeof(buf), "(output: %.1f%%)",
               (produced * 100.0) / bytes);
      thread->stats.AddMessage(buf);
      thread->stats.AddBytes(bytes);
    }
  }

  void SnappyUncompress(ThreadState* thread) {
    RandomGenerator gen;
    Slice input = gen.Generate(Options().block_size);
    std::string compressed;
    bool ok = port::Snappy_Compress(input.data(), input.size(), &compressed);
    int64_t bytes = 0;
    char* uncompressed = new char[input.size()];
    while (ok && bytes < 1024 * 1048576) {  // Compress 1G
      ok =  port::Snappy_Uncompress(compressed.data(), compressed.size(),
                                    uncompressed);
      bytes += input.size();
      thread->stats.FinishedSingleOp(db_);
    }
    delete[] uncompressed;

    if (!ok) {
      thread->stats.AddMessage("(snappy failure)");
    } else {
      thread->stats.AddBytes(bytes);
    }
  }

  void Open() {
    assert(db_ == NULL);
    Options options;
    options.create_if_missing = !FLAGS_use_existing_db;
    options.block_cache = cache_;
    options.statistics = statistics_.get();
    options.write_buffer_size = FLAGS_write_buffer_size;
    options.max_open_files = FLAGS_open_files;
    options.filter_policy = filter_policy_;
    if (FLAGS_ssd_cache_dir == NULL){
        options.ssd_cache_dir = std::string("");
    }
    else {
        options.ssd_cache_dir = std::string(FLAGS_ssd_cache_dir);
    }
    options.ssd_cache_size = FLAGS_ssd_cache_size;
    Status s = DB::Open(options, FLAGS_db, &db_);
    if (!s.ok()) {
      fprintf(stderr, "open error: %s\n", s.ToString().c_str());
      exit(1);
    }
  }

  void WriteSeq(ThreadState* thread) {
    DoWrite(thread, true);
  }

  void WriteRandom(ThreadState* thread) {
    DoWrite(thread, false);
  }

  void DoWrite(ThreadState* thread, bool seq) {
    if (num_ != FLAGS_num) {
      char msg[100];
      snprintf(msg, sizeof(msg), "(%d ops)", num_);
      thread->stats.AddMessage(msg);
    }
    
    fprintf(stderr, "Writing %d random keys in %s order ...\n", num_, seq ? "sequential" : "non-sequential");

    RandomGenerator gen;
    WriteBatch batch;
    Status s;
    int64_t bytes = 0;
    for (int i = 0; i < num_; i += entries_per_batch_) {
      batch.Clear();
      for (int j = 0; j < entries_per_batch_; j++) {
        const int k = seq ? i+j : (thread->rand.Next() % FLAGS_num);
        char key[100];
        snprintf(key, sizeof(key), "%016d", k);
        batch.Put(key, gen.Generate(value_size_));
        bytes += value_size_ + strlen(key);
        thread->stats.FinishedSingleOp(db_);
      }
      s = db_->Write(write_options_, &batch);
      if (!s.ok()) {
        fprintf(stderr, "put error: %s\n", s.ToString().c_str());
        exit(1);
      }
    }
    thread->stats.AddBytes(bytes);
  }

  void WriteGaps(ThreadState* thread) {
    if (num_ != FLAGS_num) {
      char msg[100];
      snprintf(msg, sizeof(msg), "(%d ops)", num_);
      thread->stats.AddMessage(msg);
    }

    RandomGenerator gen;
    WriteBatch batch;
    Status s;
    int64_t bytes = 0;
    for (int i = 0; i < num_; i += FLAGS_gap_len) {
      batch.Clear();
      char key[100];
      snprintf(key, sizeof(key), "%016d", i);
      batch.Put(key, gen.Generate(value_size_));
      bytes += value_size_ + strlen(key);
      thread->stats.FinishedSingleOp(db_);
      s = db_->Write(write_options_, &batch);
      if (!s.ok()) {
        fprintf(stderr, "put error: %s\n", s.ToString().c_str());
        exit(1);
      }
    }
    thread->stats.AddBytes(bytes);
  }

  void ReadSequential(ThreadState* thread) {
    Iterator* iter = db_->NewIterator(ReadOptions());
    int i = 0;
    int64_t bytes = 0;
    for (iter->SeekToFirst(); i < reads_ && iter->Valid(); iter->Next()) {
      bytes += iter->key().size() + iter->value().size();
      thread->stats.FinishedSingleOp(db_);
      ++i;
    }
    delete iter;
    thread->stats.AddBytes(bytes);
  }

  void WarmLogStore(ThreadState* thread) {
    ReadOptions options;
    std::string value;
    Status s;
    int found = 0;
    int num_warm = static_cast<int>(FLAGS_num * FLAGS_warm_ratio);
    //int skip = 1024*1024*2 / value_size_;
    for (int i = 0; i < num_warm; i += 2000) {
      char key[100];
      snprintf(key, sizeof(key), "%016d", i);
      for (int j = 0; j < 120; j++) {
        s = db_->Get(options, key, &value);
        if (s.ok()) {
          found++;
        } else {
          fprintf(stderr, "Get error: %s\n", s.ToString().c_str());
          exit(1);
        }
        thread->stats.FinishedSingleOp(db_);
      }
      usleep(10);
    }
  }

  void WarmCache(ThreadState* thread) {
    ReadOptions options;
    std::string value;
    Status s;
    int found = 0;
    int num_warm = static_cast<int>(FLAGS_num * FLAGS_warm_ratio);
//    int num_warm = 8000000; // This can result in Key not found
    for (int i = num_warm; i >= 0; i--) {
      char key[100];
      snprintf(key, sizeof(key), "%016d", i);
      s = db_->Get(options, key, &value);
      if (s.ok()) {
        found++;
      } else {
        fprintf(stderr, "Get error: %s\n", s.ToString().c_str());
        exit(1);
      }
      thread->stats.FinishedSingleOp(db_);
    }
  }

  void ReadReverse(ThreadState* thread) {
    Iterator* iter = db_->NewIterator(ReadOptions());
    int i = 0;
    int64_t bytes = 0;
    for (iter->SeekToLast(); i < reads_ && iter->Valid(); iter->Prev()) {
      bytes += iter->key().size() + iter->value().size();
      thread->stats.FinishedSingleOp(db_);
      ++i;
    }
    delete iter;
    thread->stats.AddBytes(bytes);
  }

  void ReadRandom(ThreadState* thread) {
    ReadOptions options;
    std::string value;
    int found = 0;
    for (int i = 0; i < reads_; i++) {
      char key[100];
      const int k = thread->rand.Next() % FLAGS_num;
      snprintf(key, sizeof(key), "%016d", k);
      if (db_->Get(options, key, &value).ok()) {
        found++;
      }
      thread->stats.FinishedSingleOp(db_);
    }
    char msg[100];
    snprintf(msg, sizeof(msg), "(%d of %d found)", found, num_);
    thread->stats.AddMessage(msg);
  }

  void ReadMissing(ThreadState* thread) {
    ReadOptions options;
    std::string value;
    for (int i = 0; i < reads_; i++) {
      char key[100];
      const int k = thread->rand.Next() % FLAGS_num;
      snprintf(key, sizeof(key), "%016d.", k);
      db_->Get(options, key, &value);
      thread->stats.FinishedSingleOp(db_);
    }
  }

  void ReadHot(ThreadState* thread) {
    ReadOptions options;
    std::string value;
    const int range = (FLAGS_num + 99) / 100;
    for (int i = 0; i < reads_; i++) {
      char key[100];
      const int k = thread->rand.Next() % range;
      snprintf(key, sizeof(key), "%016d", k);
      db_->Get(options, key, &value);
      thread->stats.FinishedSingleOp(db_);
    }
  }

  void ReadZipfian(ThreadState* thread) {
    ReadOptions options;
    std::string value;
    ZipfianGenerator zipf(thread->rand, FLAGS_num);
    int found = 0;
    for (int i = 0; i < reads_; i++) {
      char key[100];
      const int k = zipf.NextInt();
      snprintf(key, sizeof(key), "%016d", k);
      if (db_->Get(options, key, &value).ok()) {
        found++;
      }
      thread->stats.FinishedSingleOp(db_);
    }
    char msg[100];
    snprintf(msg, sizeof(msg), "(%d of %d found)", found, num_);
    thread->stats.AddMessage(msg);
  }

  void GenerateZipfInput() {
#ifdef CFlagPregenZipfData
      // Assume that the file already exists
      char file_name[100];
      gen_zipf_counter++;
      snprintf(file_name, sizeof(file_name), "zipf-input-%d-pt9.dat", gen_zipf_counter);
      std::string input(file_name);
      zipf_inputs.push_back(input);
      fprintf(stdout, "Assuming that Zipf Input %s exist\n", input.c_str());
#else

#ifdef CFLagPregenZipfAvailableDataFiles
      if (zipf_filelist.size() == 0){
          filelist_ptr = 0;
          DIR *dir;
          struct dirent *ent;
          if ((dir = opendir (".")) != NULL) {
              /* print all the files and directories within directory */
              while ((ent = readdir (dir)) != NULL) {
                  std::string fname(ent->d_name);
                  std::string ext(".dat");
                  std::size_t pos = fname.find(ext);
                  if (pos != std::string::npos){
//                      printf("File: %s\n", fname.c_str());
                      zipf_filelist.push_back(fname);
                  }
              }
              closedir (dir);
              std::sort(zipf_filelist.begin(), zipf_filelist.end());
//              for(auto it = zipf_filelist.begin(); it != zipf_filelist.end(); ++it){
//                 printf("%s\n", it->c_str());
//              }

          } else {
              /* could not open directory */
              fprintf(stdout, "Could not open directory!\n");
              fprintf(stderr, "Could not open directory!\n");
              exit(1);
          }
      }

      if (zipf_filelist.size() == 0){
          fprintf(stdout, "No data files available at current working directory!\n");
          fprintf(stderr, "No data files available at current working directory!\n");
          exit(1);
      }
      if (filelist_ptr + 1 > zipf_filelist.size()){
          filelist_ptr = 0;
      }
      zipf_inputs.push_back(zipf_filelist[filelist_ptr]);
      filelist_ptr++;
#else
    for (uint32_t i = 0; i < FLAGS_threads; i++) {
      std::string now_str = std::to_string(Env::Default()->NowMicros());
      std::string input = "zipf-input-" + now_str + ".dat";
      char cmd[100];
      snprintf(cmd, sizeof(cmd), "sh gen-zipf.sh %d %d %.2f %s", 
          FLAGS_num, reads_, FLAGS_zipf_skew, input.c_str());
      fprintf(stdout, "RUN: %s\n", cmd);
      system(cmd);
      zipf_inputs.push_back(input);
      fprintf(stdout, "Generated Zipf Input %s\n", input.c_str());
    }
#endif // #ifdef CFLagPregenZipfAvailableDataFiles
#endif //#ifdef CFlagPregenZipfData
  }

  void ReadZipfInput(ThreadState* thread) {
    ReadOptions options;
    std::string value;
    int found = 0;

    std::string fname("n/a");
    {
      MutexLock l(&thread->shared->mu);
      if (zipf_inputs.empty()) {
        fprintf(stderr, "No Zipf Input Available!\n");
        return;
      } 
      fname = zipf_inputs.back();
      zipf_inputs.pop_back();
    }
    fprintf(stdout, "%d: processing Zipf input %s\n", thread->tid, fname.c_str());

#if 0
    std::ifstream infile(fname.c_str());
    std::vector<std::string> nums;
    nums.reserve(25000);
    bool done = false;
    while (!done) {
      // buffer up numbers
      nums.clear();
      std::string line;
      while (std::getline(infile, line)) {
        nums.push_back(line);
        if (nums.size() >= 25000) {
          break;
        }
      }
      done = infile.eof();
      //char key[100];
      for (unsigned i = 0; i < nums.size(); i++) {
        //const int k = std::stoi(nums[i]);
        //snprintf(key, sizeof(key), "%016d", k);
        //assert(nums[i].size() == 16);
        //assert(strncmp(key, nums[i].c_str(), strlen(key)) == 0);
        if (db_->Get(options, nums[i], &value).ok()) {
          found++;
        }
        thread->stats.FinishedSingleOp(db_);
      }
    }
#endif
    int in_fd = open(fname.c_str(), O_RDONLY);
    if (in_fd < 0) {
      fprintf(stderr, "Error opening %s: %s\n", fname.c_str(), strerror(errno));
      close(in_fd);
      return;
    }
    int key_size = 16;
    int line_size = key_size + 1;
    int buf_keys = 100000; // 100K keys in buffer
    std::unique_ptr<char[]> buf(new char[key_size*line_size*buf_keys]);

    bool done = false;
    int j = 0;
    fprintf(stderr, "Starting ReadZipfInput workload ...\n");
    // Stopping either when we scan the whole input file or the number of configured reads is reached
    while (!done) {
      // buffer up numbers
      ssize_t r = read(in_fd, buf.get(), buf_keys*line_size);
      if (r < 0) {
        fprintf(stderr, "Error reading %s: %s\n", fname.c_str(), strerror(errno));
        close(in_fd);
        return;
      } else if (r == 0) {
        done = true;
      }
      int num_keys = r / line_size;
      for (unsigned i = 0; i < num_keys; i++) {
        char* key = buf.get() + (i * line_size);
        if (db_->Get(leveldb::ReadOptions(), Slice(key, key_size), &value).ok()) {
          found++;
        }
        thread->stats.FinishedSingleOp(db_);
        j++;
        if (j >= reads_){
            done = true;
            break;
        }
      }
    }
    close(in_fd);
    char msg[100];
    snprintf(msg, sizeof(msg), "(%d of %d found)", found, num_);
    thread->stats.AddMessage(msg);
      fprintf(stderr, "ReadZipfInput workload done!\n");
  }

  void WriteZipf(ThreadState* thread) {
    // 1. Pick up and input file containing keys
    std::string fname("n/a");
    {
      MutexLock l(&thread->shared->mu);
      if (zipf_inputs.empty()) {
        fprintf(stderr, "No Zipf Input Available!\n");
        return;
      } 
      fname = zipf_inputs.back();
      zipf_inputs.pop_back();
    }
    fprintf(stdout, "%d: processing Zipf input %s for writes ...\n", thread->tid, fname.c_str());

    // 2. Open the input file
    int in_fd = open(fname.c_str(), O_RDONLY);
    if (in_fd < 0) {
      fprintf(stderr, "Error opening %s: %s\n", fname.c_str(), strerror(errno));
      close(in_fd);
      return;
    }
    int key_size = 16;
    int line_size = key_size + 1;
    int buf_keys = 100000; // 100K keys in buffer
    std::unique_ptr<char[]> buf(new char[key_size*line_size*buf_keys]);

    // 3. Loop over keys in input file
    bool done = false;
    int j = 0;
    while (!done) {
      // buffer up numbers
      ssize_t r = read(in_fd, buf.get(), buf_keys*line_size);
      if (r < 0) {
        fprintf(stderr, "Error reading %s: %s\n", fname.c_str(), strerror(errno));
        close(in_fd);
        return;
      } else if (r == 0) {
        done = true;
      }
      int num_keys = r / line_size;
      RandomGenerator gen;
      for (unsigned i = 0; i < num_keys; i++) {
        char* key = buf.get() + (i * line_size);
        Status s = db_->Put(write_options_, Slice(key, key_size), gen.Generate(value_size_));
        if (!s.ok()) {
          fprintf(stderr, "put error: %s\n", s.ToString().c_str());
          exit(1);
        }
        thread->stats.FinishedSingleOp(db_);
        j++;
        if (j >= reads_){
            done = true;
            break;
        }
      }
    }
    close(in_fd);
  }

  /*
  void ReadWriteZipf(ThreadState* thread) {
    if (thread->tid > 0) {
      ReadZipfInput(thread);
    } else {
      WriteZipf(thread);
    }
  }
  */

  void ReadWriteZipf(ThreadState* thread) {
    ReadOptions read_options;
    std::string value;

    // 1. Pick up and input file containing keys
    std::string fname("n/a");
    {
      MutexLock l(&thread->shared->mu);
      if (zipf_inputs.empty()) {
        fprintf(stderr, "No Zipf Input Available!\n");
        return;
      } 
      fname = zipf_inputs.back();
      zipf_inputs.pop_back();
    }
    fprintf(stdout, "%d: processing Zipf input %s for read-write ...\n", thread->tid, fname.c_str());

    // 2. Open the input file
    int in_fd = open(fname.c_str(), O_RDONLY);
    if (in_fd < 0) {
      fprintf(stderr, "Error opening %s: %s\n", fname.c_str(), strerror(errno));
      close(in_fd);
      return;
    }
    int key_size = 16;
    int line_size = key_size + 1;
    int buf_keys = 100000; // 100K keys in buffer
    std::unique_ptr<char[]> buf(new char[key_size*line_size*buf_keys]);

    // Info stuff
    int read_pct = FLAGS_read_pct * 1000;
    int write_pct = 1000 - read_pct;
    fprintf(stderr, "Starting %d/%d read/write workload ...\n", read_pct, write_pct);

    // 3. Loop over keys in input file
    bool done = false;
    int j = 0;
    while (!done) {
      // buffer up numbers
      ssize_t r = read(in_fd, buf.get(), buf_keys*line_size);
      if (r < 0) {
        fprintf(stderr, "Error reading %s: %s\n", fname.c_str(), strerror(errno));
        close(in_fd);
        return;
      } else if (r == 0) {
        done = true;
      }
      int num_keys = r / line_size;
      RandomGenerator gen;
      for (unsigned i = 0; i < num_keys; i++) {
        char* key = buf.get() + (i * line_size);
        bool read = (thread->rand.Uniform(1000)+1) <= read_pct;
        Status s;
        if (read) {
          s = db_->Get(leveldb::ReadOptions(), Slice(key, key_size), &value);
        } else {
          s = db_->Put(write_options_, Slice(key, key_size), gen.Generate(value_size_));
        }
        if (!s.ok()) {
          fprintf(stderr, "%s error: %s\n", read ? "get" : "write", s.ToString().c_str());
          exit(1);
        }
        thread->stats.FinishedSingleOp(db_);
        j++;
        if (j >= reads_){
          done = true;
          break;
        }
      }
    }
    close(in_fd);
    fprintf(stderr, "Workload done!\n");
  }

  /**
   * Implementation of a YCSB worklod. Main difference from `ReadWriteZipf` is that
   * we use READ-MODIFY-WRITE instead of blind WRITE as in `ReadWriteZipf`
   *
   * @param thread
   */
  void YCSBWorkload(ThreadState* thread) {
            ReadOptions read_options;
            std::string value;

            // 1. Pick up and input file containing keys
            std::string fname("n/a");
            {
                MutexLock l(&thread->shared->mu);
                if (zipf_inputs.empty()) {
                    fprintf(stderr, "No Zipf Input Available!\n");
                    return;
                }
                fname = zipf_inputs.back();
                zipf_inputs.pop_back();
            }
            fprintf(stdout, "%d: processing Zipf input %s for read/writes ...\n", thread->tid, fname.c_str());

            // 2. Open the input file
            int in_fd = open(fname.c_str(), O_RDONLY);
            if (in_fd < 0) {
                fprintf(stderr, "Error opening %s: %s\n", fname.c_str(), strerror(errno));
                close(in_fd);
                return;
            }
            int key_size = 16;
            int line_size = key_size + 1;
            int buf_keys = 100000; // 100K keys in buffer
            std::unique_ptr<char[]> buf(new char[key_size*line_size*buf_keys]);

            int read_pct = FLAGS_read_pct * 1000;
            int write_pct = 1000 - read_pct;
            fprintf(stderr, "Starting %d/%d read/write workload ...\n", read_pct, write_pct);

            // 3. Loop over keys in input file
            bool done = false;
            int j = 0;
            while (!done) {
                // buffer up numbers
                ssize_t r = read(in_fd, buf.get(), buf_keys*line_size);
                if (r < 0) {
                    fprintf(stderr, "Error reading %s: %s\n", fname.c_str(), strerror(errno));
                    close(in_fd);
                    return;
                } else if (r == 0) {
                    done = true;
                }
                int num_keys = r / line_size;
                RandomGenerator gen;
                for (unsigned i = 0; i < num_keys; i++) {
                    char* key = buf.get() + (i * line_size);
                    bool read_op = (thread->rand.Uniform(1000)+1) <= read_pct;
                    Status s;
                    if (read_op) {
                        s = db_->Get(read_options, Slice(key, key_size), &value);
                    } else {
                        s = db_->Get(read_options, Slice(key, key_size), &value);
                        char v_0 = value[0];
                        v_0 = (v_0 + 1) % 127;
                        value[0] = v_0;
                        Slice new_value(value.c_str(), value_size_);
                        s = db_->Put(write_options_, Slice(key, key_size), new_value);
                    }
                    if (!s.ok()) {
                        fprintf(stderr, "%s error: %s\n", read ? "get" : "write", s.ToString().c_str());
                        exit(1);
                    }
                    thread->stats.FinishedSingleOp(db_);
                    j++;
                    if (j >= reads_){
                        done = true;
                        break;
                    }
                }
            }
            close(in_fd);
        }

    /**
    * Implementation of a workload based on specs Workload E involving record scans from YCSB paper.
    * Here starting keys of scans comes from a Zipfian distribution and length of Scan is uniform in [1,100]
    * @param thread
    */
  void YCSBWorkloadScan(ThreadState* thread) {
            ReadOptions read_options;
            std::string value;
            double scan_pct = 950; // 95% Based on Cooper10 paper
            int max_scan_length = 100; // Based on Cooper10 paper


            // 1. Pick up and input file containing keys
            std::string fname("n/a");
            {
                MutexLock l(&thread->shared->mu);
                if (zipf_inputs.empty()) {
                    fprintf(stderr, "No Zipf Input Available!\n");
                    return;
                }
                fname = zipf_inputs.back();
                zipf_inputs.pop_back();
            }
            fprintf(stdout, "%d: processing Zipf input %s for read/writes ...\n", thread->tid, fname.c_str());

            // 2. Open the input file
            int in_fd = open(fname.c_str(), O_RDONLY);
            if (in_fd < 0) {
                fprintf(stderr, "Error opening %s: %s\n", fname.c_str(), strerror(errno));
                close(in_fd);
                return;
            }
            int key_size = 16;
            int line_size = key_size + 1;
            int buf_keys = 100000; // 100K keys in buffer
            std::unique_ptr<char[]> buf(new char[key_size*line_size*buf_keys]);

            fprintf(stderr, "Starting %d/%d scan/insert workload ...\n",
                    static_cast<int>(scan_pct), static_cast<int>(1000-scan_pct) );

            // 3. Loop over keys in input file
            bool done = false;
            RandomGenerator gen;
            int j = 0;
            int k = num_;
            while (!done) {
                // buffer up numbers
                ssize_t r = read(in_fd, buf.get(), buf_keys*line_size);
                if (r < 0) {
                    fprintf(stderr, "Error reading %s: %s\n", fname.c_str(), strerror(errno));
                    close(in_fd);
                    return;
                } else if (r == 0) {
                    done = true;
                }
                int num_keys = r / line_size;

                for (unsigned i = 0; i < num_keys; i++) {
                    char* key = buf.get() + (i * line_size);
                    Status s;
                    bool scan_op = (thread->rand.Uniform(1000)+1) <= scan_pct;
                    int limit = thread->rand.Uniform(max_scan_length)+1;
                    if (scan_op){
                        leveldb::Iterator* it = db_->NewIterator(leveldb::ReadOptions());
                        int scan_cnt = 0;
                        for (it->Seek(Slice(key, key_size)); it->Valid() && scan_cnt < limit; it->Next()) {
                            s = it->status();
                            if (!s.ok()) {
                                fprintf(stderr, "%s error: %s\n", "scan", s.ToString().c_str());
                                exit(1);
                            }
                            scan_cnt++;
                        }
                        delete it;
                    }
                    else{
                        k++;
                        char ikey[100];
                        snprintf(ikey, 100, "%016d", k);
                        s = db_->Put(write_options_, Slice(ikey, key_size), gen.Generate(value_size_));
                        if (!s.ok()) {
                            fprintf(stderr, "%s error: %s\n", "write", s.ToString().c_str());
                            exit(1);
                        }
                    }

                    thread->stats.FinishedSingleOp(db_);
                    j++;
                    if (j >= reads_){
                        done = true;
                        break;
                    }
                }
            }
            close(in_fd);
        }
  
  void ReadLatest(ThreadState* thread) {
    ReadOptions options;
    std::string value;
    ZipfianGenerator zipf(thread->rand, FLAGS_num);
    int found = 0;
    fprintf(stderr, "Starting ReadLatest workload ...\n");
    for (int i = 0; i < reads_; i++) {
      char key[100];
      const int k = FLAGS_num - zipf.NextInt() - 1;
      snprintf(key, sizeof(key), "%016d", k);
      if (db_->Get(options, key, &value).ok()) {
        found++;
      }
      thread->stats.FinishedSingleOp(db_);
    }
    char msg[100];
    snprintf(msg, sizeof(msg), "(%d of %d found)", found, num_);
    thread->stats.AddMessage(msg);
    fprintf(stderr, "ReadLatest Workload done!\n");
  }

  void SeekRandom(ThreadState* thread) {
    ReadOptions options;
    std::string value;
    int found = 0;
    for (int i = 0; i < reads_; i++) {
      Iterator* iter = db_->NewIterator(options);
      char key[100];
      const int k = thread->rand.Next() % FLAGS_num;
      snprintf(key, sizeof(key), "%016d", k);
      iter->Seek(key);
      if (iter->Valid() && iter->key() == key) found++;
      delete iter;
      thread->stats.FinishedSingleOp(db_);
    }
    char msg[100];
    snprintf(msg, sizeof(msg), "(%d of %d found)", found, num_);
    thread->stats.AddMessage(msg);
  }

  void DoDelete(ThreadState* thread, bool seq) {
    RandomGenerator gen;
    WriteBatch batch;
    Status s;
    for (int i = 0; i < num_; i += entries_per_batch_) {
      batch.Clear();
      for (int j = 0; j < entries_per_batch_; j++) {
        const int k = seq ? i+j : (thread->rand.Next() % FLAGS_num);
        char key[100];
        snprintf(key, sizeof(key), "%016d", k);
        batch.Delete(key);
        thread->stats.FinishedSingleOp(db_);
      }
      s = db_->Write(write_options_, &batch);
      if (!s.ok()) {
        fprintf(stderr, "del error: %s\n", s.ToString().c_str());
        exit(1);
      }
    }
  }

  void DeleteSeq(ThreadState* thread) {
    DoDelete(thread, true);
  }

  void DeleteRandom(ThreadState* thread) {
    DoDelete(thread, false);
  }

  void ReadWhileWriting(ThreadState* thread) {
    if (thread->tid > 0) {
      ReadRandom(thread);
    } else {
      // Special thread that keeps writing until other threads are done.
      RandomGenerator gen;
      while (true) {
        {
          MutexLock l(&thread->shared->mu);
          if (thread->shared->num_done + 1 >= thread->shared->num_initialized) {
            // Other threads have finished
            break;
          }
        }

        const int k = thread->rand.Next() % FLAGS_num;
        char key[100];
        snprintf(key, sizeof(key), "%016d", k);
        Status s = db_->Put(write_options_, key, gen.Generate(value_size_));
        if (!s.ok()) {
          fprintf(stderr, "put error: %s\n", s.ToString().c_str());
          exit(1);
        }
      }

      // Do not count any of the preceding work/delay in stats.
      thread->stats.Start();
    }
  }

  void Compact(ThreadState* thread) {
    db_->CompactRange(NULL, NULL);
  }

  void PrintStats(const char* key) {
    std::string stats;
    if (!db_->GetProperty(key, &stats)) {
      stats = "(failed)";
    }
    fprintf(stdout, "\n%s\n", stats.c_str());
  }

  static void WriteToFile(void* arg, const char* buf, int n) {
    reinterpret_cast<WritableFile*>(arg)->Append(Slice(buf, n));
  }

  void HeapProfile() {
    char fname[100];
    snprintf(fname, sizeof(fname), "%s/heap-%04d", FLAGS_db, ++heap_counter_);
    WritableFile* file;
    Status s = Env::Default()->NewWritableFile(fname, &file);
    if (!s.ok()) {
      fprintf(stderr, "%s\n", s.ToString().c_str());
      return;
    }
    bool ok = port::GetHeapProfile(WriteToFile, file);
    delete file;
    if (!ok) {
      fprintf(stderr, "heap profiling not supported\n");
      Env::Default()->DeleteFile(fname);
    }
  }
};

}  // namespace leveldb

int main(int argc, char** argv) {
  FLAGS_write_buffer_size = leveldb::Options().write_buffer_size;
  FLAGS_open_files = leveldb::Options().max_open_files;
  std::string default_db_path;

  for (int i = 1; i < argc; i++) {
    double d;
    int n;
    char junk;
    if (leveldb::Slice(argv[i]).starts_with("--benchmarks=")) {
      FLAGS_benchmarks = argv[i] + strlen("--benchmarks=");
    } else if (sscanf(argv[i], "--compression_ratio=%lf%c", &d, &junk) == 1) {
      FLAGS_compression_ratio = d;
    } else if (sscanf(argv[i], "--histogram=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_histogram = n;
    } else if (sscanf(argv[i], "--use_existing_db=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_use_existing_db = n;
    } else if (sscanf(argv[i], "--num=%d%c", &n, &junk) == 1) {
      FLAGS_num = n;
    } else if (sscanf(argv[i], "--reads=%d%c", &n, &junk) == 1) {
      FLAGS_reads = n;
    } else if (sscanf(argv[i], "--threads=%d%c", &n, &junk) == 1) {
      FLAGS_threads = n;
    } else if (sscanf(argv[i], "--gap_len=%d%c", &n, &junk) == 1) {
      FLAGS_gap_len = n;
    } else if (sscanf(argv[i], "--value_size=%d%c", &n, &junk) == 1) {
      FLAGS_value_size = n;
    } else if (sscanf(argv[i], "--write_buffer_size=%d%c", &n, &junk) == 1) {
      FLAGS_write_buffer_size = n;
    } else if (sscanf(argv[i], "--cache_size=%lf%c", &d, &junk) == 1) {
      FLAGS_cache_size = d;
    } else if (sscanf(argv[i], "--bloom_bits=%d%c", &n, &junk) == 1) {
      FLAGS_bloom_bits = n;
    } else if (sscanf(argv[i], "--open_files=%d%c", &n, &junk) == 1) {
      FLAGS_open_files = n;
    } else if (strncmp(argv[i], "--db=", 5) == 0) {
      FLAGS_db = argv[i] + 5;
    } else if (strncmp(argv[i], "--ssd_cache_dir=", 16) == 0) {
      FLAGS_ssd_cache_dir = argv[i] + 16;
    } else if (sscanf(argv[i], "--ssd_cache_size=%lf%c", &d, &junk) == 1) {
      FLAGS_ssd_cache_size = d;
    } else if (sscanf(argv[i], "--zipf_skew=%lf%c", &d, &junk) == 1) {
      FLAGS_zipf_skew = d;
    } else if (sscanf(argv[i], "--stats_interval=%d%c", &n, &junk) == 1) {
      FLAGS_stats_interval = n;
    } else if (sscanf(argv[i], "--warm_ratio=%lf%c", &d, &junk) == 1) {
      FLAGS_warm_ratio = d;
    } else if (sscanf(argv[i], "--use_statistics=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_use_statistics = n;
    } else if (sscanf(argv[i], "--read_pct=%lf%c", &d, &junk) == 1) {
      FLAGS_read_pct = d;
    } else {
      fprintf(stderr, "Invalid flag '%s'\n", argv[i]);
      exit(1);
    }
  }

  // Choose a location for the test database if none given with --db=<path>
  if (FLAGS_db == NULL) {
      leveldb::Env::Default()->GetTestDirectory(&default_db_path);
      default_db_path += "/dbbench";
      FLAGS_db = default_db_path.c_str();
  }

  leveldb::Benchmark benchmark;
  benchmark.Run();
  return 0;
}
