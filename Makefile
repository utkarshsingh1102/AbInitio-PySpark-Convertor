.PHONY: install dev infra-up infra-down test lint typecheck clean

install:
	python3 -m pip install -e .

dev:
	python3 -m pip install -e ".[dev]"

infra-up:
	docker compose up -d neo4j ollama

infra-down:
	docker compose down

test:
	python3 -m pytest

lint:
	python3 -m ruff check ibm_network tests

typecheck:
	python3 -m mypy ibm_network

clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name "*.egg-info" -exec rm -rf {} +
