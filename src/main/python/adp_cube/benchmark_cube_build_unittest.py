__author__ = 'Lei'

import unittest

import adp_cube.benchmark_cube_build as adpb

from pyspark import SparkConf, SparkContext
from adp_cube.benchmark_cube_build import Calculator


class BuildTest(unittest.TestCase):
    def test_get_12_months(self):
        expect = ['201506', '201505', '201504', '201503', '201502', '201501'
                  , '201412', '201411', '201410', '201409', '201408', '201407']
        self.assertEqual(adpb.get_12_months('2015Q2'), expect)

    def test_build_key_c1(self):
        combo = '1,0,0,0'
        line = 'person,11-1011,43,MA,H'
        expect = '11-1011,,,'
        self.assertEqual(adpb.build_key(combo, line), expect)

    def test_build_key_c2(self):
        combo = '1,0,1,0'
        line = 'person,11-1011,43,MA,H'
        expect = '11-1011,,MA,'
        self.assertEqual(adpb.build_key(combo, line), expect)

    def test_build_key_c3(self):
        combo = '1,1,0,0'
        line = 'person,11-1011,43,MA,H'
        expect = '11-1011,43,,'
        self.assertEqual(adpb.build_key(combo, line), expect)

    def test_build_key_c5(self):
        combo = '0,0,1,0'
        line = 'person,11-1011,43,MA,H'
        expect = ',,MA,'
        self.assertEqual(adpb.build_key(combo, line), expect)

    def test_count_distinct(self):
        conf = SparkConf().setMaster('local')
        sc = SparkContext(conf=conf)
        data = sc.textFile('data/data.csv')
        combo = '0,0,0,1'
        quarter = '2015Q2'
        all_months = all_months = adpb.get_12_months(quarter)
        header = data.first()

        data = data\
                .filter(lambda x: x != header) \
                .filter(lambda line: line.strip().split(",")[-1] in all_months) \
                .filter(lambda line: line.strip().split(",")[3] in adpb.all_states) \
                .map(lambda line: (",".join(line.strip().split(",")[:5]),
                           line.strip().split(",")[3]))

        result = Calculator(combo=combo).count_distinct(data=data)

        self.assertEqual(result.first()[1], 51)


if __name__ == '__main__':
    unittest.main()
