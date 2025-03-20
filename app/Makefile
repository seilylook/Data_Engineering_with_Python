.PHONY: build start stop test clean init_postgres init_elasticsearch

IMAGE_NAME = python-app
DEFAULT_TAG = latest

_requirements:
	@echo "=============================================="
	@echo "Exporting Python dependencies to requirements.txt..."
	@echo "=============================================="
	poetry export -f requirements.txt --output requirements.txt --without-hashes --with dev
	@echo "\n"

build: _requirements
	@echo "=============================================="
	@echo "Building Docker image $(IMAGE_NAME):$(DEFAULT_TAG)..."
	@echo "=============================================="
	docker build -t $(IMAGE_NAME):$(DEFAULT_TAG) .
	@echo "\n"
	@echo "=============================================="
	@echo "Constructing Docker Containers..."
	@echo "=============================================="
	docker compose up -d
	@echo "\n"
	@echo "=============================================="
	@echo "Waiting for PostgreSQL to start..."
	@echo "=============================================="
	@sleep 10
	@make init_postgresql
	@echo "\n"
	@echo "=============================================="
	@echo "Waiting for Elasticsearch to start..."
	@echo "=============================================="
	@sleep 10
	@make init_elasticsearch
	@echo "\n"

start:
	@echo "========================="
	@echo "Starting the application..."
	@echo "========================="
	python -m src.main


stop:
	@echo "=========================="
	@echo "Stopping the application..."
	@echo "=========================="
	docker compose down
	@echo "\n"

rebuild: _requirements
	@echo "=========================="
	@echo "Rebuilding the application..."
	@echo "=========================="
	docker compose up -d --build
	@echo "\n"

test:
	@echo "======================="
	@echo "Running tests with pytest..."
	@echo "======================="
	mkdir -p target
	docker run --rm -v $(PWD)/target:/app/target $(IMAGE_NAME):$(DEFAULT_TAG) /bin/bash -c \
		'for test_file in $$(find tests -name "*.py" ! -name "__init__.py"); do \
			base_name=$$(basename $$test_file .py); \
			pytest $$test_file --junitxml=target/$$base_name.xml; \
		done'
	@echo "\n"

clean:
	@echo "====================================="
	@echo "Cleaning up local generated files..."
	@echo "====================================="
	rm -f requirements.txt
	@echo "Removed requirements.txt"
	@echo "\n"

init_postgresql:
	@echo "====================================="
	@echo "Initializing PostgreSQL..."
	@echo "====================================="
	chmod +x ./scripts/init_postgresql.sh
	./scripts/init_postgresql.sh
	@echo "\n"

init_elasticsearch:
	@echo "====================================="
	@echo "Initializing Elasticsearch..."
	@echo "====================================="
	chmod +x ./scripts/init_elasticsearch.sh
	./scripts/init_elasticsearch.sh
	@echo "\n"