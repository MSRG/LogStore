NKEYS=100000000 
NREADS=2000000 
NTHREADS=1
NOPEN_FILES=50000
SKEW=0.9 
WARM_RATIO=0.45
COLLECT_STATS=1
VALUE_SIZE=1024
READ_PCT=0.5
LL_DB=/disks/data/logstore
DB_DIR=/disks/ssd/logstore
BLOOM_BITS_SIZE=16

# Clean
# Delete data files for old experiments
rm /disks/data/logstore/*; rm /disks/ssd/logstore/*

# Run RO Benchmark
#/usr/bin/time -v ./db_bench --db=$DB_DIR --ll_db=$LL_DB --benchmarks=fillbatch,stats,warmlogstore,pause,stats,warmcache,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats --num=$NKEYS --reads=$NREADS --threads=$NTHREADS --compression_ratio=0.99 --value_size=$VALUE_SIZE --bloom_bits=$BLOOM_BITS_SIZE --open_files=$NOPEN_FILES --stats_interval=100000 --warm_ratio=$WARM_RATIO --advise_random_on_open=1 --zipf_skew=$SKEW --use_statistics=$COLLECT_STATS

# Run RO Benchmark - No Warmlogstore
#/usr/bin/time -v ./db_bench --db=$DB_DIR --ll_db=$LL_DB --benchmarks=fillbatch,stats,warmcache,stats,pause,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats --num=$NKEYS --reads=$NREADS --threads=$NTHREADS --compression_ratio=0.99 --value_size=$VALUE_SIZE --bloom_bits=$BLOOM_BITS_SIZE --open_files=$NOPEN_FILES --stats_interval=100000 --warm_ratio=$WARM_RATIO --advise_random_on_open=1 --zipf_skew=$SKEW --use_statistics=$COLLECT_STATS


# Run RO Benchmark - No Warmlogstore - 8 runs
#/usr/bin/time -v ./db_bench --db=$DB_DIR --ll_db=$LL_DB --benchmarks=fillbatch,stats,warmcache,stats,pause,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats --num=$NKEYS --reads=$NREADS --threads=$NTHREADS --compression_ratio=0.99 --value_size=$VALUE_SIZE --bloom_bits=$BLOOM_BITS_SIZE --open_files=$NOPEN_FILES --stats_interval=100000 --warm_ratio=$WARM_RATIO --advise_random_on_open=1 --zipf_skew=$SKEW --use_statistics=$COLLECT_STATS



# 5 measured trials
#/usr/bin/time -v ./db_bench --db=$DB_DIR --ll_db=$LL_DB --benchmarks=fillbatch,stats,warmlogstore,stats,warmcache,stats,pause,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats --num=$NKEYS --reads=$NREADS --threads=$NTHREADS --compression_ratio=0.99 --value_size=$VALUE_SIZE --bloom_bits=$BLOOM_BITS_SIZE --open_files=$NOPEN_FILES --stats_interval=100000 --warm_ratio=$WARM_RATIO --advise_random_on_open=1 --zipf_skew=$SKEW --use_statistics=$COLLECT_STATS


# Run WO Benchmark
#/usr/bin/time -v ./db_bench --db=$DB_DIR --ll_db=$LL_DB --benchmarks=fillbatch,stats,pause,warmcache,stats,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,writezipf,stats,writezipf,stats,writezipf,stats,writezipf,stats,writezipf,stats,writezipf,stats,writezipf,stats,writezipf,stats,writezipf,stats,writezipf,stats,writezipf,stats,writezipf,stats,writezipf,stats,writezipf,stats,writezipf,stats,writezipf,stats --num=$NKEYS --reads=$NREADS --threads=$NTHREADS --compression_ratio=0.99 --value_size=$VALUE_SIZE --bloom_bits=$BLOOM_BITS_SIZE --open_files=$NOPEN_FILES --stats_interval=100000 --warm_ratio=$WARM_RATIO --advise_random_on_open=1 --zipf_skew=$SKEW --use_statistics=$COLLECT_STATS
# Run WO Benchmark - 5trials
/usr/bin/time -v ./db_bench --db=$DB_DIR --ll_db=$LL_DB --benchmarks=fillbatch,stats,pause,warmcache,stats,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,writezipf,stats,writezipf,stats,writezipf,stats,writezipf,stats,writezipf,stats --num=$NKEYS --reads=$NREADS --threads=$NTHREADS --compression_ratio=0.99 --value_size=$VALUE_SIZE --bloom_bits=$BLOOM_BITS_SIZE --open_files=$NOPEN_FILES --stats_interval=100000 --warm_ratio=$WARM_RATIO --advise_random_on_open=1 --zipf_skew=$SKEW --use_statistics=$COLLECT_STATS

# 8 trials
#/usr/bin/time -v ./db_bench --db=$DB_DIR --ll_db=$LL_DB --benchmarks=fillbatch,stats,pause,warmcache,stats,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,writezipf,stats,writezipf,stats,writezipf,stats,writezipf,stats,writezipf,stats,writezipf,stats,writezipf,stats,writezipf,stats --num=$NKEYS --reads=$NREADS --threads=$NTHREADS --compression_ratio=0.99 --value_size=1024 --bloom_bits=16 --open_files=$NOPEN_FILES --stats_interval=100000 --warm_ratio=$WARM_RATIO --zipf_skew=$SKEW --use_statistics=$COLLECT_STATS

# 4 trials -- debug
#/usr/bin/time -v ./db_bench --db=$DB_DIR --ll_db=$LL_DB --benchmarks=fillbatch,stats,pause,genzipfinput,writezipf,stats,genzipfinput,writezipf,stats,genzipfinput,writezipf,stats,genzipfinput,writezipf,stats --num=$NKEYS --reads=$NREADS --threads=$NTHREADS --compression_ratio=0.99 --value_size=1024 --bloom_bits=16 --open_files=$NOPEN_FILES --stats_interval=100000 --warm_ratio=$WARM_RATIO --zipf_skew=$SKEW --use_statistics=$COLLECT_STATS

# Run RW Benchmark
#/usr/bin/time -v ./db_bench --db=$DB_DIR --ll_db=$LL_DB --benchmarks=fillbatch,stats,warmlogstore,pause,stats,warmcache,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats --num=$NKEYS --reads=$NREADS --threads=$NTHREADS --compression_ratio=0.99 --value_size=$VALUE_SIZE --bloom_bits=$BLOOM_BITS_SIZE --open_files=$NOPEN_FILES --stats_interval=100000 --warm_ratio=$WARM_RATIO --advise_random_on_open=1 --zipf_skew=$SKEW --use_statistics=$COLLECT_STATS --read_pct=$READ_PCT

#No readzipf -- no warmlogstore
#/usr/bin/time -v ./db_bench --db=$DB_DIR --ll_db=$LL_DB --benchmarks=fillbatch,stats,pause,warmcache,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats --num=$NKEYS --reads=$NREADS --threads=$NTHREADS --compression_ratio=0.99 --value_size=1024 --bloom_bits=16 --open_files=$NOPEN_FILES --stats_interval=100000 --warm_ratio=$WARM_RATIO --zipf_skew=$SKEW --use_statistics=$COLLECT_STATS --read_pct=$READ_PCT

#No readzipf
#/usr/bin/time -v ./db_bench --db=$DB_DIR --ll_db=$LL_DB --benchmarks=fillbatch,stats,warmlogstore,pause,warmcache,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats --num=$NKEYS --reads=$NREADS --threads=$NTHREADS --compression_ratio=0.99 --value_size=1024 --bloom_bits=16 --open_files=$NOPEN_FILES --stats_interval=100000 --warm_ratio=$WARM_RATIO --zipf_skew=$SKEW --use_statistics=$COLLECT_STATS --read_pct=$READ_PCT

#YCSB Workloads
# Set READ_PCT to 0.5 for Workload-A, and to 0.95 for Workload-B
#/usr/bin/time -v ./db_bench --db=$DB_DIR --ll_db=$LL_DB --benchmarks=fillbatch,stats,pause,warmcache,stats,genzipfinput,ycsb,stats,genzipfinput,ycsb,stats,genzipfinput,ycsb,stats,genzipfinput,ycsb,stats,genzipfinput,ycsb,stats,genzipfinput,ycsb,stats,genzipfinput,ycsb,stats,genzipfinput,ycsb,stats,genzipfinput,ycsb,stats,genzipfinput,ycsb,stats,genzipfinput,ycsb,stats,genzipfinput,ycsb,stats,genzipfinput,ycsb,stats,genzipfinput,ycsb,stats,genzipfinput,ycsb,stats,genzipfinput,ycsb,stats --num=$NKEYS --reads=$NREADS --threads=$NTHREADS --compression_ratio=0.99 --value_size=1024 --bloom_bits=16 --open_files=$NOPEN_FILES --stats_interval=100000 --warm_ratio=$WARM_RATIO --zipf_skew=$SKEW --use_statistics=$COLLECT_STATS --read_pct=$READ_PCT

# Workload-E
#/usr/bin/time -v ./db_bench --db=$DB_DIR --ll_db=$LL_DB --benchmarks=fillbatch,stats,pause,warmcache,stats,genzipfinput,ycsbscan,stats,genzipfinput,ycsbscan,stats,genzipfinput,ycsbscan,stats,genzipfinput,ycsbscan,stats,genzipfinput,ycsbscan,stats,genzipfinput,ycsbscan,stats,genzipfinput,ycsbscan,stats,genzipfinput,ycsbscan,stats,genzipfinput,ycsbscan,stats,genzipfinput,ycsbscan,stats,genzipfinput,ycsbscan,stats,genzipfinput,ycsbscan,stats,genzipfinput,ycsbscan,stats,genzipfinput,ycsbscan,stats,genzipfinput,ycsbscan,stats,genzipfinput,ycsbscan,stats --num=$NKEYS --reads=$NREADS --threads=$NTHREADS --compression_ratio=0.99 --value_size=1024 --bloom_bits=16 --open_files=$NOPEN_FILES --stats_interval=100000 --warm_ratio=$WARM_RATIO --zipf_skew=$SKEW --use_statistics=$COLLECT_STATS --read_pct=$READ_PCT

#/usr/bin/time -v ./db_bench --db=$DB_DIR --ll_db=$LL_DB --benchmarks=fillbatch,stats,pause,warmcache,stats,genzipfinput,ycsbscan,stats,genzipfinput,ycsbscan,stats,genzipfinput,ycsbscan,stats,genzipfinput,ycsbscan,stats,genzipfinput,ycsbscan,stats --num=$NKEYS --reads=$NREADS --threads=$NTHREADS --compression_ratio=0.99 --value_size=1024 --bloom_bits=16 --open_files=$NOPEN_FILES --stats_interval=100000 --warm_ratio=$WARM_RATIO --zipf_skew=$SKEW --use_statistics=$COLLECT_STATS --read_pct=$READ_PCT

# Timeseries Latest->Zipfian
#/usr/bin/time -v ./db_bench --db=$DB_DIR --ll_db=$LL_DB --benchmarks=fillbatch,stats,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,warmcache,stats,pause,readlatest,stats,readzipfinput,stats,readzipfinput,stats,readzipfinput,stats,readzipfinput,stats,readzipfinput,stats,readzipfinput,stats,readzipfinput,stats,readzipfinput,stats --num=$NKEYS --reads=$NREADS --threads=$NTHREADS --compression_ratio=0.99 --value_size=1024 --bloom_bits=16 --open_files=$NOPEN_FILES --stats_interval=100000 --warm_ratio=$WARM_RATIO --zipf_skew=$SKEW --use_statistics=$COLLECT_STATS

# Run RANDOM Benchmark
#/usr/bin/time -v ./db_bench --db=$DB_DIR --ll_db=$LL_DB --benchmarks=fillbatch,stats,warmcache,stats,readrandom,stats,readrandom,stats,readrandom,stats,readrandom,stats,readrandom,stats,readrandom,stats --num=$NKEYS --reads=$NREADS --threads=$NTHREADS --compression_ratio=0.99 --value_size=$VALUE_SIZE --bloom_bits=$BLOOM_BITS_SIZE --open_files=$NOPEN_FILES --stats_interval=100000 --warm_ratio=$WARM_RATIO --advise_random_on_open=1 --zipf_skew=$SKEW --use_statistics=$COLLECT_STATS
