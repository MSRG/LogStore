import yaml
import os, getopt
import sys
import subprocess
import psutil
import smtplib
import threading

# Color codes used when printing to STD OUTPUT
class OutputColors:
    COLOR_RED = '\x1b[31m'
    COLOR_GREEN = '\x1b[32m'
    COLOR_YELLOW = '\x1b[33m'
    COLOR_BLUE = '\x1b[34m'
    COLOR_MAGENTA = '\x1b[35m'
    COLOR_CYAN ='\x1b[36m'
    COLOR_RESET = '\x1b[0m'


class Util:
    # DB Bench Command
    LOGSTORE_BIN_DIR = './LogStore'
    LEVELDB_BIN_DIR = './leveldb-ssd-cache'
    DB_BENCH_BIN = 'db_bench'
    BENCH_OPT = 'benchmarks'

    # Benchmark
    BENCH_OPT_LOAD_PHASE = 'fillbatch,stats'
    BENCH_OPT_WARMCACHE_PHASE = 'warmcache,stats'
    BENCH_OPT_PAUSE = 'pause'
    BENCH_OPT_GEN_ZIPF_INPUT = 'genzipfinput'
    BENCH_OPT_STATS = 'stats'
    BENCH_OPT_READ_ZIPF_INPUT = 'readzipfinput,stats'

    # Output files extensions
    ERR_FILE_EXT = 'err'
    OUTPUT_FILE_EXT = 'out'

    # YAML Keys
    WORKLOAD_FILE = 'workload_spec_file'
    MOCK = 'mock'
    TAG = 'tag'
    HELP = 'help'
    SUT = 'sut'
    RAM = 'RAM'
    BENCH_NAME = 'bench_name'
    TRIAL_CNT = 'trial_cnt'
    WARMUP_PHASE = 'warmup_phase'
    WARMUP_RATIO = 'warm_ratio'
    BENCH_PHASE = 'bench_phase'
    DB_NUM = 'num'
    READ_CNT = 'reads'
    THREAD_CNT = 'threads'
    COMP_RATIO = 'compression_ratio'
    VALUE_SIZE = 'value_size'
    BLOOM_BITS = 'bloom_bits'
    OPEN_FILES = 'open_files'
    ZIPF_SKEW = 'zipf_skew'
    USE_STATS = 'use_statistics'
    STATS_INTERVAL = 'stats_interval'
    READ_PCT = 'read_pct'
    DB_DIR = 'db'
    LL_DB_DIR = 'll_db'
    SSD_CACHE_DIR = 'ssd_cache_dir'
    SSD_CACHE_SIZE = 'ssd_cache_size'
    ADVISE_RAND_ON_OPEN = 'advise_random_on_open'
    USE_SSD_CACHE = 'use_ssd_cache'

    DEFAULT_LOGSTORE_DIR_NAME = 'logstore'
    DEFAULT_LEVELDB_DIR_NAME = 'leveldb'
    DEFAULT_DB_DIR_NAME = 'default_dir_name'


def exec_cmd(specs, fname):
    cmd = build_cmd(specs)
    output_fname = '{}.out'.format(fname)
    err_fname = '{}.err'.format(fname)
    out_file = open(output_fname, 'w')
    err_file = open(err_fname, 'w')
    output = ""
    err = ""
    if is_logstore(specs[Util.SUT]):
        os.chdir(Util.LOGSTORE_BIN_DIR)
    elif is_leveldb(specs[Util.SUT]):
        os.chdir(Util.LEVELDB_BIN_DIR)

    print("Running {} trials, cmd: {}".format(specs[Util.TRIAL_CNT], cmd))
    for i in range(specs[Util.TRIAL_CNT]):
        print("Trial #{}\n".format(i))
        output += "Trial #{}\n".format(i)
        err += "Trial #{}\n".format(i)
        if specs[Util.MOCK]:
            print(cmd)
            continue

        clean_up(specs)

        if specs[Util.USE_SSD_CACHE]:
            fcmd = "fallocate -l {}G {}/ssd_cache.db".format(round(specs[Util.SSD_CACHE_SIZE] / (1024 * 1024 * 1024)),
                                                             specs[Util.SSD_CACHE_DIR])
            print("Allocating SSD Cache file:{}".format(fcmd))
            p = subprocess.Popen(fcmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, env=dict(os.environ))
            p.wait()
            print("File allocation is done")

        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, env=dict(os.environ))
        outs_, errs_ = p.communicate()
        output += outs_.decode(encoding="utf-8", errors="strict")
        err += errs_.decode(encoding="utf-8", errors="strict")
        if p.returncode < 0:
            print("Exit code < 0. Terminating now!")
            out_file.write(output)
            err_file.write(err)
            out_file.close()
            err_file.close()
            return output, err


    out_file.write(output)
    err_file.write(err)
    out_file.close()
    err_file.close()
    return output, err

def usage():
    print('Usage: run_exp.py -f <workload_file> [-t tag] [-d output directory]')

def parse_argv(argv):
    try:
        opts, args = getopt.getopt(argv, 'hmf:t:d:', ["help", "mock", "tag", "output-dir"])
    except getopt.GetoptError:
        usage()
        sys.exit(1)

    args = {}
    args[Util.WORKLOAD_FILE] = None
    args[Util.MOCK] = False
    args[Util.HELP] = False
    args[Util.TAG] = 'NoTag'
    args[Util.DEFAULT_DB_DIR_NAME] = None

    for o, a in opts:
        if o == '-f':
            args[Util.WORKLOAD_FILE] = a
        elif o == '--mock' or o == '-m':
            args[Util.MOCK] = True
        elif o == '--tag' or o == '-t':
            args[Util.TAG] = a
        elif o == '-h' or o == '--help':
            args[Util.HELP] = True
        elif o == '-d' or o == '--db-dir':
            args[Util.DEFAULT_DB_DIR_NAME] = a

    return args

def is_logstore(sut_name):
    if sut_name.find('Logstore') >= 0:
        return True
    else: return False

def is_leveldb(sut_name):
    if sut_name.find('LevelDB') >= 0:
        return True
    else: return False

def create_filename(_args, _specs):

    res = '{}_{}_{}GB_{}_{}'.format(
        _specs[Util.BENCH_NAME],
        _specs[Util.SUT],
        _specs[Util.RAM],
        _specs[Util.TRIAL_CNT],
        _args[Util.TAG]
    )
    return res


def load_workload_conf_file(args):
    specs = {}
    RAM = round(psutil.virtual_memory().total / (1024 * 1024 * 1024))
    with open(args[Util.WORKLOAD_FILE], 'r') as s_file:
        specs = yaml.safe_load(s_file)
    specs[Util.RAM] = RAM
    if args[Util.DEFAULT_DB_DIR_NAME]:
        specs[Util.DEFAULT_DB_DIR_NAME] = args[Util.DEFAULT_DB_DIR_NAME]
    else:
        if is_logstore(specs[Util.SUT]):
            specs[Util.DEFAULT_DB_DIR_NAME] = Util.DEFAULT_LOGSTORE_DIR_NAME
        elif is_leveldb(specs[Util.SUT]):
            specs[Util.DEFAULT_DB_DIR_NAME] = Util.DEFAULT_LEVELDB_DIR_NAME
    specs[Util.MOCK] = args[Util.MOCK]
    if Util.USE_SSD_CACHE not in specs.keys():
        specs[Util.USE_SSD_CACHE] = False
    return specs


def build_cmd(specs):
    cmd = './db_bench '
    cmd += '--{}={}/{}/ '.format(Util.DB_DIR, specs[Util.DB_DIR], specs[Util.DEFAULT_DB_DIR_NAME])

    if is_logstore(specs[Util.SUT]):
        cmd += '--{}={}/{}/ '.format(Util.LL_DB_DIR, specs[Util.LL_DB_DIR], specs[Util.DEFAULT_DB_DIR_NAME])

    cmd += '--{}='.format(Util.BENCH_OPT)
    cmd += '{}'.format(Util.BENCH_OPT_LOAD_PHASE)
    cmd += ',{}'.format(Util.BENCH_OPT_WARMCACHE_PHASE)
    cmd += ',{}'.format(Util.BENCH_OPT_PAUSE)
    cmd += ',{}'.format(Util.BENCH_OPT_GEN_ZIPF_INPUT)
    cmd += ',{},{}'.format(specs[Util.BENCH_PHASE], Util.BENCH_OPT_STATS)
    cmd += ' '

    cmd += '--{}={} '.format(Util.DB_NUM, specs[Util.DB_NUM])
    cmd += '--{}={} '.format(Util.READ_CNT, specs[Util.READ_CNT])
    cmd += '--{}={} '.format(Util.THREAD_CNT, specs[Util.THREAD_CNT])
    cmd += '--{}={} '.format(Util.COMP_RATIO, specs[Util.COMP_RATIO])
    cmd += '--{}={} '.format(Util.VALUE_SIZE, specs[Util.VALUE_SIZE])
    cmd += '--{}={} '.format(Util.BLOOM_BITS, specs[Util.BLOOM_BITS])
    cmd += '--{}={} '.format(Util.OPEN_FILES, specs[Util.OPEN_FILES])
    cmd += '--{}={} '.format(Util.STATS_INTERVAL, specs[Util.STATS_INTERVAL])
    cmd += '--{}={} '.format(Util.WARMUP_RATIO, specs[Util.WARMUP_RATIO])
    cmd += '--{}={} '.format(Util.READ_PCT, specs[Util.READ_PCT])

    if is_logstore(specs[Util.SUT]):
        cmd += '--{}={} '.format(Util.ADVISE_RAND_ON_OPEN, 1 if specs[Util.ADVISE_RAND_ON_OPEN] else 0)

    cmd += '--{}={} '.format(Util.ZIPF_SKEW, specs[Util.ZIPF_SKEW])
    cmd += '--{}={} '.format(Util.USE_STATS, 1 if specs[Util.USE_STATS] else 0)

    if is_leveldb(specs[Util.SUT]) and specs[Util.USE_SSD_CACHE]:
        cmd += '--{}={} '.format(Util.SSD_CACHE_DIR, specs[Util.SSD_CACHE_DIR])
        cmd += '--{}={} '.format(Util.SSD_CACHE_SIZE, specs[Util.SSD_CACHE_SIZE])

    return cmd.strip()


def clean_up(specs):
    #TODO(tq): use file locks to ensure that no process is running or use psutil to kill existing processes

    cmd = 'rm -rf {}/{}/*'.format(specs[Util.DB_DIR], specs[Util.DEFAULT_DB_DIR_NAME])
    if specs[Util.MOCK]:
        print(cmd)
        return
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, env=dict(os.environ))
    p.wait()
    if is_logstore(specs[Util.SUT]):
        cmd = 'rm -rf {}/{}/*'.format(specs[Util.LL_DB_DIR], specs[Util.DEFAULT_DB_DIR_NAME])
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, env=dict(os.environ))
        p.wait()
    if is_leveldb(specs[Util.SUT]) and specs[Util.USE_SSD_CACHE]:
        cmd = 'rm -rf {}/*'.format(specs[Util.SSD_CACHE_DIR], specs[Util.DEFAULT_DB_DIR_NAME])
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, env=dict(os.environ))
        p.wait()


def main():
    args = parse_argv(sys.argv[1:])

    if args[Util.HELP]:
        usage()
        sys.exit(0)

    if args[Util.WORKLOAD_FILE] is None:
        usage()
        sys.exit(1)

    specs = load_workload_conf_file(args)

    filename = create_filename(args, specs)
    print("Writing output files with name: {}".format(filename))

    # Clean up and Run
    exec_cmd(specs, filename)

if __name__ == '__main__':
    main()
