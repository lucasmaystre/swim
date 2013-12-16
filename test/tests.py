#!/usr/bin/env python
import swim
import unittest
# Resources:
# - http://docs.python-guide.org/en/latest/writing/tests/
# - http://docs.python.org/2/library/unittest.html
# - https://nose.readthedocs.org/en/latest/
# - http://www.voidspace.org.uk/python/mock/
# - https://github.com/patrys/httmock

# How to proceed: go over code an think about:
# - what would be interesting to test
# - what could possibly fail
# - what would be hard to test
# - how is the module going to be used in practice


class FetcherTest(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_something(self):
        self.assertEqual(1+1, 2, "Maybe we should give a call to Peano")


if __name__ == '__main__':
    unittest.main()
