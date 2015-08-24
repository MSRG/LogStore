NKEYS=100000000 
NREADS=2000000 
NTHREADS=1
NOPEN_FILES=50000
SKEW=0.9 
WARM_RATIO=0.45
COLLECT_STATS=1
VALUE_SIZE=1024
READ_PCT=0.1

# Clean
sh ~/clean.sh

# Run RO Benchmark
#/usr/bin/time -v ./db_bench --db=/disks/ssd1/logstore/ --ll_db=/disks/data/logstore/ --benchmarks=fillbatch,stats,warmlogstore,pause,stats,warmcache,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats --num=$NKEYS --reads=$NREADS --threads=$NTHREADS --compression_ratio=0.99 --value_size=$VALUE_SIZE --bloom_bits=16 --open_files=$NOPEN_FILES --stats_interval=100000 --warm_ratio=$WARM_RATIO --advise_random_on_open=1 --zipf_skew=$SKEW --use_statistics=$COLLECT_STATS

# Run WO Benchmark
#/usr/bin/time -v ./db_bench --db=/disks/ssd1/logstore/ --ll_db=/disks/data/logstore/ --benchmarks=fillbatch,stats,pause,warmcache,stats,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,genzipfinput,writezipf,stats,writezipf,stats,writezipf,stats,writezipf,stats,writezipf,stats,writezipf,stats,writezipf,stats,writezipf,stats,writezipf,stats,writezipf,stats,writezipf,stats,writezipf,stats,writezipf,stats,writezipf,stats,writezipf,stats --num=$NKEYS --reads=$NREADS --threads=$NTHREADS --compression_ratio=0.99 --value_size=$VALUE_SIZE --bloom_bits=16 --open_files=$NOPEN_FILES --stats_interval=100000 --warm_ratio=$WARM_RATIO --advise_random_on_open=1 --zipf_skew=$SKEW --use_statistics=$COLLECT_STATS

# Run RW Benchmark
/usr/bin/time -v ./db_bench --db=/disks/ssd1/logstore/ --ll_db=/disks/data/logstore/ --benchmarks=fillbatch,stats,warmlogstore,pause,stats,warmcache,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readzipfinput,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats,genzipfinput,readwritezipf,stats --num=$NKEYS --reads=$NREADS --threads=$NTHREADS --compression_ratio=0.99 --value_size=$VALUE_SIZE --bloom_bits=16 --open_files=$NOPEN_FILES --stats_interval=100000 --warm_ratio=$WARM_RATIO --advise_random_on_open=1 --zipf_skew=$SKEW --use_statistics=$COLLECT_STATS --read_pct=$READ_PCT

# Run RANDOM Benchmark
#/usr/bin/time -v ./db_bench --db=/disks/ssd1/logstore/ --ll_db=/disks/data/logstore/ --benchmarks=fillbatch,stats,warmcache,stats,readrandom,stats,readrandom,stats,readrandom,stats,readrandom,stats,readrandom,stats,readrandom,stats --num=$NKEYS --reads=$NREADS --threads=$NTHREADS --compression_ratio=0.99 --value_size=$VALUE_SIZE --bloom_bits=16 --open_files=$NOPEN_FILES --stats_interval=100000 --warm_ratio=$WARM_RATIO --advise_random_on_open=1 --zipf_skew=$SKEW --use_statistics=$COLLECT_STATS
