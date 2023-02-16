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
	$(PYTHON) __main__.py \


clean:
	rm -rf htmlcov/ .coverage */__pycache__/
