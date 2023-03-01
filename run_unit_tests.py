import unittest
import coverage

if __name__ == '__main__':
    cov = coverage.Coverage(source=['lstore'], omit=['*/tests/*', '*/__init__.py', '*/run_unit_tests.py'])
    cov.start()

    test_suite = unittest.defaultTestLoader.discover('tests', pattern='query_spec.py')
    unittest.TextTestRunner().run(test_suite)

    cov.stop()
    cov.save()
    cov.report()