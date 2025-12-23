.PHONY: .uv install rebuil-lockfile erase-coverage fast-test only-test report-coverage xml-coverage test
.SILENT: .uv install rebuil-lockfile erase-coverage fast-test only-test report-coverage xml-coverage test

.uv:
	uv -V || echo 'Please install uv: https://docs.astral.sh/uv/getting-started/installation/'

install: .uv
	uv sync --frozen --all-extras --all-groups --python 3.10
	uv run pre-commit install --install-hooks

rebuild-lockfiles: .uv
	uv lock --upgrade

erase-coverage: .uv
	uv run coverage erase

fast-test: .uv erase-coverage
	uv run coverage run -m pytest tests/unit
	uv run coverage report --omit 'repid/connections/**'

only-test: .uv
	uv run coverage run -m pytest tests/

report-coverage: .uv
	uv run coverage report --no-skip-covered --show-missing

xml-coverage: .uv
	uv run coverage xml --fail-under=0

test: .uv erase-coverage only-test xml-coverage report-coverage
