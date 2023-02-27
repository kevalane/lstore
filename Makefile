PYTHON=python

run:
	streamlit run app.py
	
test:
	echo "\n*** Running unit tests... ***\n" && \
	$(PYTHON) run_unit_tests.py && \
	echo "\n*** Running integration tests... ***\n" && \
	echo "Running m1_tester.py..." && \
	$(PYTHON) m1_tester.py && \
	echo "\nRunning __main__.py..." && \
	$(PYTHON) __main__.py && \
	echo "\nRunning m2_tester_part1.py..." && \
	$(PYTHON) m2_tester_part1.py && \
	echo "\nRunning m2_tester_part2.py..." && \
	$(PYTHON) m2_tester_part2.py

clean:
	rm -rf htmlcov/ .coverage */__pycache__/ data/ ECS165/
