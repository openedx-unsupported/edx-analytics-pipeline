
# If a wheel repository is defined, then have pip use that.  But don't require the use of wheel.
ifneq ($(strip $(WHEEL_URL)),)
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

bootstrap: uninstall
	$(PIP_INSTALL) -U -r requirements/pre.txt
	$(PIP_INSTALL) -U -r requirements/base.txt
	python setup.py install --force

develop: requirements develop-local

develop-local: uninstall
	python setup.py develop
	python setup.py install_data

system-requirements:
ifeq (,$(wildcard /usr/bin/yum))
	sudo apt-get update -q
	# This is not great, we can't use these libraries on slave nodes using this method.
	sudo apt-get install -y -q libmysqlclient-dev libatlas3gf-base libpq-dev python-dev libffi-dev libssl-dev libxml2-dev libxslt1-dev
else
	sudo yum update -q -y
	sudo yum install -y -q postgresql-devel libffi-devel
endif

requirements:
	$(PIP_INSTALL) -U -r requirements/pre.txt
	$(PIP_INSTALL) -U -r requirements/default.txt
	$(PIP_INSTALL) -U -r requirements/extra.txt

test-requirements: requirements
	$(PIP_INSTALL) -U -r requirements/test.txt

test-local:
	# TODO: when we have better coverage, modify this to actually fail when coverage is too low.
	rm -rf .coverage
	LUIGI_CONFIG_PATH='config/test.cfg' python -m coverage run --rcfile=./.coveragerc -m nose --with-xunit --xunit-file=unittests.xml -A 'not acceptance'

test: test-requirements develop test-local


test-acceptance: test-requirements
	LUIGI_CONFIG_PATH='config/test.cfg' python -m coverage run --rcfile=./.coveragerc -m nose --nocapture --with-xunit -A acceptance $(ONLY_TESTS)

test-acceptance-local:
	REMOTE_TASK=$(shell which remote-task) LUIGI_CONFIG_PATH='config/test.cfg' ACCEPTANCE_TEST_CONFIG="/var/tmp/acceptance.json" python -m coverage run --rcfile=./.coveragerc -m nose --nocapture --with-xunit -A acceptance --stop -v $(ONLY_TESTS)

coverage-local: test-local
	python -m coverage html
	python -m coverage xml -o coverage.xml
	diff-cover coverage.xml --html-report diff_cover.html

	# Compute pep8 quality
	diff-quality --violations=pep8 --html-report diff_quality_pep8.html
	pep8 edx > pep8.report || echo "Not pep8 clean"

	# Compute pylint quality
	pylint -f parseable edx --msg-template "{path}:{line}: [{msg_id}({symbol}), {obj}] {msg}" > pylint.report || echo "Not pylint clean"
	diff-quality --violations=pylint --html-report diff_quality_pylint.html pylint.report

coverage: test coverage-local

docs-requirements:
	$(PIP_INSTALL) -U -r requirements/docs.txt
	python setup.py install --force

docs-local:
	sphinx-build -b html docs/source docs

docs-clean:
	rm -rf docs/*.* docs/_*

docs: docs-requirements docs-local

todo:
	pylint --disable=all --enable=W0511 edx
