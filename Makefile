.PHONY: clean clean-test clean-pyc clean-build docs help
.DEFAULT_GOAL := help
define BROWSER_PYSCRIPT
import os, webbrowser, sys
try:
	from urllib import pathname2url
except:
	from urllib.request import pathname2url

webbrowser.open("file://" + pathname2url(os.path.abspath(sys.argv[1])))
endef
export BROWSER_PYSCRIPT

define PRINT_HELP_PYSCRIPT
import re, sys

for line in sys.stdin:
	match = re.match(r'^([a-zA-Z_-]+):.*?## (.*)$$', line)
	if match:
		target, help = match.groups()
		print("%-20s %s" % (target, help))
endef
export PRINT_HELP_PYSCRIPT
BROWSER := python -c "$$BROWSER_PYSCRIPT"

DOCSRC		= _src_doc
VENV		= /tmp/combtest_$(CURRENT_REV)


build: ## build this
	python setup.py build

help:
	@python -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

clean: clean-build clean-pyc clean-test ## remove all build, test, coverage and Python artifacts

clean-build: ## remove build artifacts
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +

clean-pyc: ## remove Python file artifacts
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

lint:
	pylint combtest

test: ## run tests quickly with the default Python
	python -m unittest discover combtest.test

docker-base:
	docker build --file Dockerfile.base -t docker.west.isilon.com/combtest_base . &&
	docker push -t docker.west.isilon.com/combtest_base

docs-clean:
	$(MAKE) -C $(DOCSRC) clean

# TODO: autodoc / API docs
docs: ## generate Sphinx HTML documentation, including API docs
	$(MAKE) -C $(DOCSRC) html
	$(BROWSER) docs/index.html

venv:
	virtualenv --system-site-packages $(VENV)
	$(VENV)/bin/pip install -r .

#release: clean ## package and upload a release
#	python setup.py sdist upload # TODO
#	python setup.py bdist_wheel upload #TODO
#
#dist: clean ## builds source and wheel package
#	python setup.py sdist
#	python setup.py bdist_wheel
#	ls -l dist


install: clean ## install the package to the active Python's site-packages
	pip install .

