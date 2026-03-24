.PHONY: run test clean-local report

run:
	docker compose down
	docker compose build
	docker compose up

test:
	python -m pytest tests/ -v

clean-local:
	python clean_local.py example_lay/ex1.lay

report:
	PROCESS_MODE=report python process.py
