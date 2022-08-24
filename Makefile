.PHONY: venv

build: init setup start

init:
	python3 -m venv venv

setup: requirements.txt venv
	. venv/bin/activate; pip install -r requirements.txt

lint: .flake8 venv
	. venv/bin/activate; flake8

start: venv
	. venv/bin/activate; python main.py --blocks=5 --address="747 Howard St, San Francisco, CA 94103"

debug: venv
	. venv/bin/activate; python main.py --blocks=5 --address="747 Howard St, San Francisco, CA 94103" --logging=debug

clean: venv
	rm -rf venv