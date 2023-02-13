import unittest
import coverage

if __name__ == '__main__':
    cov = coverage.Coverage()
    cov.start()

    test_suite = unittest.defaultTestLoader.discover('tests', pattern='*_spec.py')
    unittest.TextTestRunner().run(test_suite)

    cov.stop()
    cov.save()
    cov.report()