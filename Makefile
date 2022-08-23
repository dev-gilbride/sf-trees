build: requirements.txt
	python3 -m venv venv
	. venv/bin/activate
	pip install -r requirements.txt

lint: .flake8
	. venv/bin/activate
	flake8

start:
	. venv/bin/activate
	python main.py --blocks=5 --address="747 Howard St, San Francisco, CA 94103"

debug:
	. venv/bin/activate
	python main.py --blocks=5 --address="747 Howard St, San Francisco, CA 94103" --logging=debug

clean: 
	rm -rf venv