PYTHON ?= python

.PHONY: ingest marts analyze app test lint ci

ingest:
	$(PYTHON) -m opal_ocean.cli ingest

marts:
	$(PYTHON) -m opal_ocean.cli marts

analyze:
	$(PYTHON) -m opal_ocean.cli analyze

app:
	$(PYTHON) -m opal_ocean.cli app

lint:
	ruff check src tests

test:
	pytest

ci: lint test
