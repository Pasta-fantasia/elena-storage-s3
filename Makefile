install:
	pip install --upgrade pip
	pip install -U setuptools wheel
	pip install -e ../elena
	pip install -r requirements.txt
	pip install -r requirements_test.txt

setup: install
	pre-commit install

.PHONY: lint
lint:
	pre-commit run --all-files

.PHONY: lint-changed
lint-changed:
	git status --porcelain | egrep -v '^(D |RM|R )' | cut -b 4- | xargs pre-commit run --files

.PHONY: test
test:
	python -m pytest test/
