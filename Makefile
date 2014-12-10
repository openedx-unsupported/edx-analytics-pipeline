
# If a wheel repository is defined, then have pip use that.  But don't require the use of wheel.
ifdef WHEEL_PYVER
    PIP_INSTALL = pip install --use-wheel --find-links=$$WHEEL_URL/Python-$$WHEEL_PYVER --allow-external mysql-connector-python
else
    PIP_INSTALL = pip install --allow-external mysql-connector-python
endif

.PHONY:	requirements test test-requirements .tox

uninstall:
	while pip uninstall -y edx.analytics.tasks; do true; done
	python setup.py clean

install: requirements uninstall
	python setup.py install --force

develop: requirements
	python setup.py develop

system-requirements:
	sudo apt-get update -q
	# This is not great, we can't use these libraries on slave nodes using this method.
	sudo apt-get install -y -q libmysqlclient-dev libatlas3gf-base

requirements:
	$(PIP_INSTALL) -U -r requirements/default.txt

test-requirements: requirements
	$(PIP_INSTALL) -U -r requirements/test.txt

test-local:
	# TODO: when we have better coverage, modify this to actually fail when coverage is too low.
	rm -rf .coverage
	LUIGI_CONFIG_PATH='config/test.cfg' python -m coverage run --rcfile=./.coveragerc -m nose --with-xunit --xunit-file=unittests.xml -A 'not acceptance'

test: test-requirements develop test-local


test-acceptance: test-requirements
	LUIGI_CONFIG_PATH='config/test.cfg' python -m coverage run --rcfile=./.coveragerc -m nose --nocapture --with-xunit -A acceptance $(ONLY_TESTS)

coverage-local: test-local
	python -m coverage html
	python -m coverage xml -o coverage.xml
	diff-cover coverage.xml --html-report diff_cover.html

	# Compute quality
	diff-quality --violations=pep8 --html-report diff_quality_pep8.html
	diff-quality --violations=pylint --html-report diff_quality_pylint.html

	# Compute style violations
	pep8 edx > pep8.report || echo "Not pep8 clean"
	pylint -f parseable edx > pylint.report || echo "Not pylint clean"

coverage: test coverage-local


todo:
	pylint --disable=all --enable=W0511 edx

jenkins: .tox
	virtualenv ./venv
	./venv/bin/pip install -U tox
	./venv/bin/tox -v --recreate
