.PHONY: run test clean-local

run:
	docker compose down
	docker compose build
	docker compose up

test:
	python -m pytest tests/ -v

clean-local:
	python clean_local.py example_lay/ex1.lay
