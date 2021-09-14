REQUIREMENTS := src/requirements.txt
TEST_REQUIREMENTS := tests/requirements.txt
SOURCES := $(wildcard src/./*.py src/./**/*.py)
PYTHON := python3.8
BUILDDIR := $(PWD)/build/
DISTDIR := $(PWD)/dist/
REPORTSDIR := $(PWD)/reports/

.PHONY: clean build lint lint-report test test-report dist .lint-setup .test-setup

clean:
	rm -fr -- .venv || :
	rm -fr -- "$(BUILDDIR)" || :
	rm -fr -- "$(DISTDIR)" || :
	rm -fr -- "$(REPORTSDIR)" || :

build:
	[ -e .venv ] || $(PYTHON) -mvenv .venv
	[ -e "$(BUILDDIR)" ] || mkdir -p "$(BUILDDIR)"
	.venv/bin/pip install -qq --target "$(BUILDDIR)" -r $(REQUIREMENTS)
	rsync -R $(SOURCES) "$(BUILDDIR)"

.lint-setup: build
	.venv/bin/pip install -qq -r $(REQUIREMENTS)
	[ -e .venv/bin/pylint ] || .venv/bin/pip install -qq pylint
lint: .lint-setup
	.venv/bin/pylint $(SOURCES)
lint-report: .lint-setup
	[ -e "$(REPORTSDIR)" ] || mkdir -p "$(REPORTSDIR)"
	.venv/bin/pip install -qq pylint_junit
	.venv/bin/pylint --output-format=pylint_junit.JUnitReporter $(SOURCES) > "$(REPORTSDIR)/pylint.xml"

.test-setup: build
	.venv/bin/pip install -qq -r $(REQUIREMENTS)
	.venv/bin/pip install -qq -r $(TEST_REQUIREMENTS)
test: .test-setup
	.venv/bin/pytest -v tests/
test-report: .test-setup
	[ -e "$(REPORTSDIR)" ] || mkdir -p "$(REPORTSDIR)"
	.venv/bin/pytest --junitxml="$(REPORTSDIR)/pytest.xml" tests/

dist: build
	[ -e "$(DISTDIR)" ] || mkdir -p "$(DISTDIR)"
	cd "$(BUILDDIR)" && zip -yr "$(DISTDIR)/dynamodbStreamEvents.zip" *
