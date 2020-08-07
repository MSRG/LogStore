import unittest
import run_exp
from run_exp import Util

class BasicTests(unittest.TestCase):

    def test_file_name(self):
        cmd_line = 'run_exp.py -f ro_wl.yaml'
        args = run_exp.parse_argv(cmd_line.split(sep=" ")[1:])
        self.assertEqual(args[Util.WORKLOAD_FILE], 'ro_wl.yaml')

    def test_help(self):
        cmd_line = 'run_exp.py -f ro_wl.yaml'
        args = run_exp.parse_argv(cmd_line.split(sep=" ")[1:])
        self.assertEqual(args[Util.HELP], False)

        cmd_line = 'run_exp.py -h'
        args = run_exp.parse_argv(cmd_line.split(sep=" ")[1:])
        self.assertEqual(args[Util.HELP], True)

    def test_mock(self):
        cmd_line = 'run_exp.py -f ro_wl.yaml --mock'
        args = run_exp.parse_argv(cmd_line.split(sep=" ")[1:])
        self.assertEqual(args[Util.MOCK], True)

        cmd_line = 'run_exp.py -f ro_wl.yaml'
        args = run_exp.parse_argv(cmd_line.split(sep=" ")[1:])
        self.assertEqual(args[Util.MOCK], False)

    def test_basic(self):
        cmd_line = 'run_exp.py -f ro_wl.yaml --mock -t N2-4tb'
        args = run_exp.parse_argv(cmd_line.split(sep=" ")[1:])
        self.assertEqual(args[Util.WORKLOAD_FILE], 'ro_wl.yaml')
        self.assertEqual(args[Util.HELP], False)
        self.assertEqual(args[Util.MOCK], True)
        self.assertEqual(args[Util.TAG], 'N2-4tb')

    def test_load_conf_file(self):
        cmd_line = 'run_exp.py -f ro_wl.yaml'
        args = run_exp.parse_argv(cmd_line.split(sep=" ")[1:])
        specs = run_exp.load_workload_conf_file(args)
        self.assertEqual(specs[Util.BENCH_NAME], 'RO')

    def test_create_file(self):
        cmd_line = 'run_exp.py -f ro_wl_logstore_test.yaml'
        args = run_exp.parse_argv(cmd_line.split(sep=" ")[1:])
        specs = run_exp.load_workload_conf_file(args)
        exp_file_name = run_exp.create_filename(args, specs)
        import psutil
        RAM = int(psutil.virtual_memory().total / (1024 * 1024 * 1024))
        test_fname = 'RO_Logstore-NS_{}GB_16_NoTag'.format(RAM)
        self.assertEqual(exp_file_name, test_fname)

    def test_cmd_logstore(self):
        self.maxDiff = None
        cmd_line = 'run_exp.py -f ro_wl_logstore_test.yaml'
        args = run_exp.parse_argv(cmd_line.split(sep=" ")[1:])
        specs = run_exp.load_workload_conf_file(args)
        cmd = run_exp.build_cmd(specs)
        print(cmd)
        test_cmd = './db_bench --db=/disks/ssd/logstore/ --ll_db=/disks/data/logstore/ --benchmarks=fillbatch,stats,warmcache,stats,pause,genzipfinput,readzipfinput,stats --num=100000000 --reads=2000000 --threads=1 --compression_ratio=0.99 --value_size=1024 --bloom_bits=16 --open_files=50000 --stats_interval=100000 --warm_ratio=0.45 --advise_random_on_open=1 --zipf_skew=0.9 --use_statistics=1'
        self.assertEqual(cmd, test_cmd)


if __name__ == '__main__':
    unittest.main()
