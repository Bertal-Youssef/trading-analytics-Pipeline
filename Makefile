SHELL := /bin/bash
.ONESHELL:

up:
	docker compose --env-file .env up -d

down:
	docker compose down -v

logs:
	docker compose logs -f --tail=200

topics:
	docker compose exec kafka kafka-topics --bootstrap-server kafka:9092 --list || true

airflow-open:
	@echo "Airflow: http://localhost:8087 (admin / admin)"

superset-open:
	@echo "Superset: http://localhost:8088 (admin / admin)"

metabase-open:
	@echo "Metabase: http://localhost:3000"
