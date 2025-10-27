.PHONY: marts

marts:
	PYTHONPATH=. python -m src.transform.run_marts
