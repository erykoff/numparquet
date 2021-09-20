import unittest
import tempfile
import os
import shutil
import numpy.testing as testing
import numpy as np

import numparquet


class IoTestCase(unittest.TestCase):
    def test_writeread_simpletable(self):
        """
        Test writing and reading of a simple table.
        """
        self.test_dir = tempfile.mkdtemp(dir='./', prefix='TestAstroParquet-')

        recarray = np.zeros(10, dtype=[('a', np.int32),
                                       ('b', np.int64),
                                       ('c', np.float32),
                                       ('d', np.float64),
                                       ('e', 'U20')])

        recarray['a'][:] = np.arange(10)
        recarray['b'][:] = np.arange(10)
        recarray['c'][:] = np.arange(10)
        recarray['d'][:] = np.arange(10)

        recarray['e'][0] = 'aaa'
        recarray['e'][1] = 'bbb'
        recarray['e'][2] = 'ccc'

        fname = os.path.join(self.test_dir, 'test_simple.parquet')

        numparquet.write_numparquet(fname, recarray)

        recarray2 = numparquet.read_numparquet(fname)

        for name in recarray.dtype.names:
            if name == 'e':
                testing.assert_array_equal(recarray2[name], recarray[name])
            else:
                testing.assert_almost_equal(recarray2[name], recarray[name])
            self.assertEqual(recarray2[name].dtype, recarray[name].dtype)

    def setUp(self):
        self.test_dir = None

    def tearDown(self):
        if self.test_dir is not None:
            if os.path.exists(self.test_dir):
                shutil.rmtree(self.test_dir, True)


if __name__ == '__main__':
    unittest.main()
