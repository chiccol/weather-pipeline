# Convenience targets. Run `make help` to see them.

.DEFAULT_GOAL := help

.PHONY: help
help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "} {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

.PHONY: init
init:  ## First-time setup: create .env from example
	@test -f .env || cp .env.example .env
	@mkdir -p logs plugins
	@echo "Edit .env and set OPENWEATHER_API_KEY, then run: make up"

.PHONY: up
up:  ## Build images and start the whole stack
	docker compose up -d --build

.PHONY: down
down:  ## Stop the stack (keeps the data volume)
	docker compose down

.PHONY: nuke
nuke:  ## Stop stack AND delete the postgres volume (fresh start)
	docker compose down -v

.PHONY: logs
logs:  ## Tail logs from all services
	docker compose logs -f

.PHONY: ps
ps:  ## Show service status
	docker compose ps

.PHONY: trigger
trigger:  ## Manually trigger the DAG once
	docker compose exec airflow-scheduler airflow dags trigger weather_pipeline

.PHONY: dbt-shell
dbt-shell:  ## Open a shell in the scheduler with dbt available
	docker compose exec airflow-scheduler bash -c "cd /opt/airflow/dbt && bash"

.PHONY: psql
psql:  ## Connect to the analytics warehouse as the weather user
	docker compose exec postgres psql -U weather -d weather

.PHONY: dbt-docs
dbt-docs:  ## Generate and serve dbt docs on http://localhost:8081
	docker compose exec airflow-scheduler bash -c \
	  "cd /opt/airflow/dbt && dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir . --port 8081 --host 0.0.0.0"
