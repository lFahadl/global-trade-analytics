#!/usr/bin/env python
import unittest
import sys

# Discover and run all tests
if __name__ == '__main__':
    # Find all test files in the tests directory
    test_suite = unittest.defaultTestLoader.discover('tests')
    
    # Run the tests
    test_runner = unittest.TextTestRunner(verbosity=2)
    result = test_runner.run(test_suite)
    
    # Return non-zero exit code if tests failed
    sys.exit(not result.wasSuccessful())
