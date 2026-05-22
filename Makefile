.PHONY: up down dev logs migrate seed install

up:
	docker compose up --build

down:
	docker compose down -v

dev:
	docker compose -f docker-compose.dev.yml up -d
	@echo "Postgres + Redis up. Now run services locally:"
	@echo "  cd ingestion && uvicorn app.main:app --port 4000 --reload"
	@echo "  cd ingestion && celery -A app.worker worker -l info"
	@echo "  cd web       && uvicorn app.main:app --port 3000 --reload"

dev-down:
	docker compose -f docker-compose.dev.yml down

logs:
	docker compose logs -f

migrate:
	cd ingestion && alembic upgrade head

seed:
	cd ingestion && python -m app.seed

install:
	cd sdk       && pip install -e .
	cd ingestion && pip install -r requirements.txt
	cd web       && pip install -r requirements.txt

publish-sdk:
	cd sdk && python -m build
	cd sdk && twine upload dist/llm_obs-$${VERSION:-0.1.3}*
