DOCKER_NAME ?= kints-pg
POSTGRES_IMAGE ?= postgres:17.5
POSTGRES_PORT ?= 5432
POSTGRES_USER ?= postgres
POSTGRES_PASSWORD ?= postgres
POSTGRES_DB ?= postgres

.PHONY: db-up db-down db-logs db-restart test-e2e tidy

db-up:
	@echo "Starting PostgreSQL $(POSTGRES_IMAGE) on port $(POSTGRES_PORT) ..."
	@docker rm -f $(DOCKER_NAME) >/dev/null 2>&1 || true
	@docker run -d --name $(DOCKER_NAME) \
		-e POSTGRES_USER=$(POSTGRES_USER) \
		-e POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) \
		-e POSTGRES_DB=$(POSTGRES_DB) \
		-p $(POSTGRES_PORT):5432 \
		$(POSTGRES_IMAGE)
	@echo "Waiting for PostgreSQL to be ready..."
	@for i in $$(seq 1 30); do \
		if docker exec $(DOCKER_NAME) pg_isready -U $(POSTGRES_USER) >/dev/null 2>&1; then \
			echo "PostgreSQL is ready"; \
			exit 0; \
		fi; \
		sleep 1; \
		echo "..."; \
	done; \
	echo "PostgreSQL did not become ready in time"; exit 1

db-down:
	@echo "Stopping and removing container $(DOCKER_NAME) ..."
	@docker rm -f $(DOCKER_NAME) >/dev/null 2>&1 || true

db-logs:
	@docker logs -f $(DOCKER_NAME)

db-restart: db-down db-up

test-e2e:
	PGHOST=127.0.0.1 PGPORT=$(POSTGRES_PORT) PGUSER=$(POSTGRES_USER) PGPASSWORD=$(POSTGRES_PASSWORD) PGDATABASE=$(POSTGRES_DB) \
		go test ./e2e -v

tidy:
	go mod tidy


