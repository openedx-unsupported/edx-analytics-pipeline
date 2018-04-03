
.PHONY:	requirements test test-requirements .tox upgrade

uninstall:
	while pip uninstall -y edx.analytics.tasks; do true; done
	python setup.py clean

install: requirements uninstall
	python setup.py install --force

bootstrap: uninstall
	pip install -U -r requirements/pre.txt
	pip install -U -r requirements/base.txt --no-cache-dir
	python setup.py install --force

develop: requirements develop-local

develop-local: uninstall
	python setup.py develop
	python setup.py install_data

docker-build:
	docker build -t edxops/analytics-pipeline:latest .

docker-shell:
	docker run -v `(pwd)`:/edx/app/analytics-pipeline -it edxops/analytics-pipeline:latest bash

system-requirements:
ifeq (,$(wildcard /usr/bin/yum))
	# This is not great, we can't use these libraries on slave nodes using this method.
	sudo apt-get install -y -q libmysqlclient-dev libpq-dev python-dev libffi-dev libssl-dev libxml2-dev libxslt1-dev
else
	sudo yum install -y -q postgresql-devel libffi-devel
endif

requirements:
	pip install -U -r requirements/pre.txt
	pip install -U -r requirements/default.txt --no-cache-dir --upgrade-strategy only-if-needed
	pip install -U -r requirements/extra.txt --no-cache-dir --upgrade-strategy only-if-needed

test-requirements: requirements
	pip install -U -r requirements/test.txt --no-cache-dir --upgrade-strategy only-if-needed

upgrade: ## update the requirements/*.txt files with the latest packages satisfying requirements/*.in
	pip-compile --upgrade -o requirements/base.txt requirements/base.in
	pip-compile --upgrade -o requirements/default.txt requirements/default.in requirements/base.in
	pip-compile --upgrade -o requirements/docs.txt requirements/docs.in requirements/default.in requirements/base.in
	pip-compile --upgrade -o requirements/test.txt requirements/test.in requirements/default.in requirements/base.in
	echo "-r extra.txt" >> requirements/docs.txt

test-docker:
	docker run -v `(pwd)`:/edx/app/analytics-pipeline -it edxops/analytics-pipeline:latest make develop-local test-local

test-local:
	# TODO: when we have better coverage, modify this to actually fail when coverage is too low.
	rm -rf .coverage
	LUIGI_CONFIG_PATH='config/test.cfg' python -m coverage run --rcfile=./.coveragerc -m nose --with-xunit --xunit-file=unittests.xml -A 'not acceptance'

test: test-requirements develop test-local

test-acceptance: test-requirements
	LUIGI_CONFIG_PATH='config/test.cfg' python -m coverage run --rcfile=./.coveragerc -m nose --nocapture --with-xunit -A acceptance $(ONLY_TESTS)

test-acceptance-local:
	REMOTE_TASK=$(shell which remote-task) LUIGI_CONFIG_PATH='config/test.cfg' ACCEPTANCE_TEST_CONFIG="/var/tmp/acceptance.json" python -m coverage run --rcfile=./.coveragerc -m nose --nocapture --with-xunit -A acceptance --stop -v $(ONLY_TESTS)

test-acceptance-local-all:
	REMOTE_TASK=$(shell which remote-task) LUIGI_CONFIG_PATH='config/test.cfg' ACCEPTANCE_TEST_CONFIG="/var/tmp/acceptance.json" python -m coverage run --rcfile=./.coveragerc -m nose --nocapture --with-xunit -A acceptance -v

quality-docker:
	docker run -v `(pwd)`:/edx/app/analytics-pipeline -it edxops/analytics-pipeline:latest isort --check-only --recursive edx/
	docker run -v `(pwd)`:/edx/app/analytics-pipeline -it edxops/analytics-pipeline:latest pycodestyle edx

coverage-docker:
	docker run -v `(pwd)`:/edx/app/analytics-pipeline -it edxops/analytics-pipeline:latest coverage xml

coverage-local: test-local
	python -m coverage html
	python -m coverage xml -o coverage.xml
	diff-cover coverage.xml --html-report diff_cover.html

	isort --check-only --recursive edx/

	# Compute pep8 quality
	diff-quality --violations=pycodestyle --html-report diff_quality_pep8.html
	pycodestyle edx > pep8.report || echo "Not pep8 clean"

	# Compute pylint quality
	pylint -f parseable edx --msg-template "{path}:{line}: [{msg_id}({symbol}), {obj}] {msg}" > pylint.report || echo "Not pylint clean"
	diff-quality --violations=pylint --html-report diff_quality_pylint.html pylint.report

coverage: test coverage-local

docs-requirements:
	pip install -U -r requirements/pre.txt
	pip install -U -r requirements/docs.txt --no-cache-dir --upgrade-strategy only-if-needed
	python setup.py install --force

docs-local:
	sphinx-build -b html docs/source docs

docs-clean:
	rm -rf docs/*.* docs/_*

docs: docs-requirements docs-local

todo:
	pylint --disable=all --enable=W0511 edx

### Docker commands

dev.build.permissions:
	chmod +x scripts/hadoop-docker-command.sh scripts/provision.sh

dev.build.analyticstack: dev.build.permissions
	docker build -t edxops/analyticstack:3.0 .

dev.provision: ## Provision all services with local mounted directories
	DOCKER_COMPOSE_FILES="-f docker-compose.yml " ./scripts/provision.sh

dev.reset:
	rm -rf .dev

dev.up:
	docker-compose up -d

dev.stop:
	docker-compose stop

dev.down:
	docker-compose down

shell:
	docker exec -it edx.devstack.analytics_pipeline /bin/bash
