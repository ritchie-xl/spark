__author__ = 'Lei'

import unittest
import adp_cube.benchmark_cube_build as adpb

class BuildTest(unittest.TestCase):
    def test_get_12_months(self):
        expect = ['201506', '201505', '201504', '201503', '201502', '201501'
                  , '201412', '201411', '201410', '201409', '201408', '201407']
        self.assertEqual(adpb.get_12_months('2015Q2'), expect)

    def test_build_key_c1(self):
        combo = '1,1,0,0,0'
        line = 'person,11-1011,43,MA,H'
        expect = '1,11-1011,,,'
        self.assertEqual(adpb.build_key(combo, line), expect)

    def test_build_key_c2(self):
        combo = '2,1,0,1,0'
        line = 'person,11-1011,43,MA,H'
        expect = '2,11-1011,,MA,'
        self.assertEqual(adpb.build_key(combo, line), expect)

    def test_build_key_c3(self):
        combo = '3,1,1,0,0'
        line = 'person,11-1011,43,MA,H'
        expect = '3,11-1011,43,,'
        self.assertEqual(adpb.build_key(combo, line), expect)

    def test_build_key_c5(self):
        combo = '5,0,0,1,0'
        line = 'person,11-1011,43,MA,H'
        expect = '5,,,MA,'
        self.assertEqual(adpb.build_key(combo, line), expect)

if __name__ == '__main__':
    unittest.main()
