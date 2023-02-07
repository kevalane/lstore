import unittest

if __name__ == '__main__':
    test_suite = unittest.defaultTestLoader.discover('tests', pattern='*_spec.py')
    unittest.TextTestRunner().run(test_suite)