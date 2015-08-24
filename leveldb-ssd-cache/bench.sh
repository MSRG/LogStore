NKEYS=100000000 
NREADS=2000000 
NTHREADS=1 
#NOPEN_FILES=28000
NOPEN_FILES=50000
SKEW=0.9 
WARM_RATIO=0.45
COLLECT_STATS=1
LOCATION=data
READ_PCT=0.1
SSD_CACHE_DIR=/disks/ssd1/leveldb
SSD_CACHE_SIZE=107374182400.0

# Clean
sh ~/clean.sh

# Create SSD cache file
echo "fallocate -l 100G /disks/ssd1/leveldb/ssd_cache.db"
fallocate -l 100G /disks/ssd1/leveldb/ssd_cache.db

# Run RO Benchmark
#/usr/bin/time -v ./db_bench --db=/disks/$LOCATION/leveldb/ --benchmarks=fillbatch,stats,pause,warmcache,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats --num=$NKEYS --reads=$NREADS --threads=$NTHREADS --compression_ratio=0.99 --value_size=1024 --bloom_bits=16 --open_files=$NOPEN_FILES --stats_interval=100000 --warm_ratio=$WARM_RATIO --zipf_skew=$SKEW --use_statistics=$COLLECT_STATS --ssd_cache_dir=$SSD_CACHE_DIR --ssd_cache_size=$SSD_CACHE_SIZE

# Run WO Benchmark
#/usr/bin/time -v ./db_bench --db=/disks/$LOCATION/leveldb/ --benchmarks=fillbatch,stats,pause,warmcache,stats,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,writezipf,stats,writezipf,stats,writezipf,stats,writezipf,stats,writezipf,stats,writezipf,stats,writezipf,stats,writezipf,stats,writezipf,stats,writezipf,stats,writezipf,stats,writezipf,stats,writezipf,stats,writezipf,stats,writezipf,stats,writezipf,stats --num=$NKEYS --reads=$NREADS --threads=$NTHREADS --compression_ratio=0.99 --value_size=1024 --bloom_bits=16 --open_files=$NOPEN_FILES --stats_interval=100000 --warm_ratio=$WARM_RATIO --zipf_skew=$SKEW --use_statistics=$COLLECT_STATS

# Run RW Benchmark
#/usr/bin/time -v ./db_bench --db=/disks/$LOCATION/leveldb/ --benchmarks=fillbatch,stats,pause,warmcache,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats --num=$NKEYS --reads=$NREADS --threads=$NTHREADS --compression_ratio=0.99 --value_size=1024 --bloom_bits=16 --open_files=$NOPEN_FILES --stats_interval=100000 --warm_ratio=$WARM_RATIO --zipf_skew=$SKEW --use_statistics=$COLLECT_STATS --read_pct=$READ_PCT

#/usr/bin/time -v ./db_bench --db=/disks/$LOCATION/leveldb/ --benchmarks=fillbatch,stats,pause,warmcache,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,pause,pause,pause,pause,stats --num=$NKEYS --reads=$NREADS --threads=$NTHREADS --compression_ratio=0.99 --value_size=1024 --bloom_bits=16 --open_files=$NOPEN_FILES --stats_interval=100000 --warm_ratio=$WARM_RATIO --zipf_skew=$SKEW --use_statistics=$COLLECT_STATS --read_pct=$READ_PCT

# USE THIS ONE
/usr/bin/time -v ./db_bench --db=/disks/$LOCATION/leveldb/ --benchmarks=fillbatch,stats,pause,warmcache,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,pause,pause,pause,pause,stats --num=$NKEYS --reads=$NREADS --threads=$NTHREADS --compression_ratio=0.99 --value_size=1024 --bloom_bits=16 --open_files=$NOPEN_FILES --stats_interval=100000 --warm_ratio=$WARM_RATIO --zipf_skew=$SKEW --use_statistics=$COLLECT_STATS --read_pct=$READ_PCT --ssd_cache_dir=$SSD_CACHE_DIR --ssd_cache_size=$SSD_CACHE_SIZE

#/usr/bin/time -v ./db_bench --db=/disks/$LOCATION/leveldb/ --benchmarks=fillbatch,stats,pause,warmcache,stats,writerandom,stats,writerandom,stats,writerandom,stats,writerandom,stats,writerandom,stats,writerandom,stats,pause,pause,pause,pause,stats --num=$NKEYS --reads=$NREADS --threads=$NTHREADS --compression_ratio=0.99 --value_size=1024 --bloom_bits=16 --open_files=$NOPEN_FILES --stats_interval=100000 --warm_ratio=$WARM_RATIO --zipf_skew=$SKEW --use_statistics=$COLLECT_STATS --read_pct=$READ_PCT

# Periodic Benchmark
#/usr/bin/time -v ./db_bench --db=/disks/$LOCATION/leveldb/ --benchmarks=fillbatch,stats,pause,warmcache,stats,genzipfinput,writezipf,stats,genzipfinput,readzipfinput,stats,genzipfinput,writezipf,stats,genzipfinput,readzipfinput,stats,genzipfinput,writezipf,stats,genzipfinput,readzipfinput,stats,genzipfinput,writezipf,stats,genzipfinput,readzipfinput,stats,genzipfinput,writezipf,stats,genzipfinput,readzipfinput,stats,genzipfinput,writezipf,stats,genzipfinput,readzipfinput,stats,genzipfinput,writezipf,stats,genzipfinput,readzipfinput,stats,genzipfinput,writezipf,stats,genzipfinput,readzipfinput,stats --num=$NKEYS --reads=$NREADS --threads=$NTHREADS --compression_ratio=0.99 --value_size=1024 --bloom_bits=16 --open_files=$NOPEN_FILES --stats_interval=100000 --warm_ratio=$WARM_RATIO --zipf_skew=$SKEW --use_statistics=$COLLECT_STATS
