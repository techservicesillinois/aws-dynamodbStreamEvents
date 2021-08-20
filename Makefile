REQUIREMENTS := src/requirements.txt
TEST_REQUIREMENTS := tests/requirements.txt
SOURCES := $(wildcard src/./**/*.py)
PYTHON := python3.8
BUILDDIR := $(PWD)/build/
DISTDIR := $(PWD)/dist/

.PHONY: clean build lint test dist .venv .deps .src .builddir .distdir

.builddir:
	mkdir -p "$(BUILDDIR)"

.distdir:
	mkdir -p "$(DISTDIR)"

.deps: | .venv .builddir
	.venv/bin/pip install --target "$(BUILDDIR)" -r $(REQUIREMENTS)

.src: | .builddir
	rsync -R $(SOURCES) "$(BUILDDIR)"

.venv:
	[ -e "$@" ] || $(PYTHON) -mvenv $@

clean:
	rm -fr -- .venv
	rm -fr -- "$(BUILDDIR)"
	rm -fr -- "$(DISTDIR)"

build: .deps .src

lint: build
	[ -e .venv/bin/pylint ] || .venv/bin/pip install pylint
	.venv/bin/pylint $(SOURCES)

test: build
	.venv/bin/pip install -r $(TEST_REQUIREMENTS)
	.venv/bin/pytest -v tests/

dist: build | .distdir
	cd "$(BUILDDIR)" && zip -r "$(DISTDIR)/dynamodbStreamEvents.zip" *
