install:
	. .venv/bin/activate; pip install -Ur requirements.txt

activate:
	. .venv/bin/activate

install_venv:
	python3 -m venv .venv
	. .venv/bin/activate; python -m pip install --upgrade pip
	. .venv/bin/activate; python -m pip install -r dev-requirements.txt

formatter:
	. .venv/bin/activate; command black --line-length 125 .

check_format:
	. .venv/bin/activate; command black --line-length 125 . --check

test:
	. .venv/bin/activate; command cd src; command python -m pytest

start_detached:
	docker compose up --build -d

start-test:
	docker compose up --build

start:
	docker compose up --build

stop:
	docker compose stop