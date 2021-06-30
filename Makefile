REQUIREMENTS := src/requirements.txt
TEST_REQUIREMENTS := tests/requirements.txt
SOURCES := $(wildcard src/./**/*.py)
PYTHON := python3.8

.PHONY: clean build lint test dist .venv .deps .src

.builddir:
	mkdir -p build

.deps: | .venv .builddir
	.venv/bin/pip install --target build/ -r $(REQUIREMENTS)

.src: | .builddir
	rsync -R $(SOURCES) build/

.venv:
	[ -e "$@" ] || $(PYTHON) -mvenv $@

build: .deps .src

lint: build
	[ -e .venv/bin/pylint ] || .venv/bin/pip install pylint
	.venv/bin/pylint $(SOURCES)

test: build
	.venv/bin/pip install -r $(TEST_REQUIREMENTS)
	.venv/bin/pytest -v tests/dynamo_stream_events

dist: build
	cd build && zip -r ../dist/dynamoStreamEvents.zip *
