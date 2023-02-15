run:
	python3 m1_tester.py && python3 __main__.py

test:
	python3 run_unit_tests.py

clean:
	rm -rf htmlcov/ .coverage */__pycache__/
