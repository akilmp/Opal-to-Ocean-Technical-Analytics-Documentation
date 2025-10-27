PYTHON ?= python

.PHONY: ingest marts analyze app test lint ci

ingest:
	PYTHONPATH=src $(PYTHON) -m opal_ocean.cli ingest

marts:
	PYTHONPATH=src $(PYTHON) -m opal_ocean.cli marts

analyze:
	PYTHONPATH=src $(PYTHON) -m opal_ocean.cli analyze

app:
	PYTHONPATH=src $(PYTHON) -m opal_ocean.cli app

lint:
	ruff check src tests

test:
	pytest

ci: lint test
