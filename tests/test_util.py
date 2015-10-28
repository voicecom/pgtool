# -*- coding: utf-8 -*-
"""Unit tests for internal functions in the 'util' module"""

from __future__ import unicode_literals

import unittest

from pgtool.util import pretty_size


class UtilTest(unittest.TestCase):
    def test_pretty_size(self):
        testcases = {
            0: '0b',
            42: '42b',
            1000: '1000b',
            1023: '1023b',
            1024: '1.00k',
            1025: '1.00k',
            1030: '1.01k',
            142857: '140k',
            1024 ** 2 - 1: '1024k',
            1024 ** 2: '1.00M',
            (1024 ** 3) * 2: '2.00G',
            1024 ** 8: '1.00Y',
            10 ** 27: '827Y',
        }
        for key, value in testcases.items():
            self.assertEqual(value, pretty_size(key))


if __name__ == '__main__':
    unittest.main()
