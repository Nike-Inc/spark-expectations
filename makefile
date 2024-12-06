ruff-check:
	@hatch run ruff-check

fmt:
	@hatch fmt

mypy:
	@hatch run mypy-check

check: ruff-check mypy
	@hatch run dev:check

kafka-cluster-start:
    ifeq ($(UNIT_TESTING_ENV), spark_expectations_unit_testing_on_github_actions)
	    . ./src/spark_expectations/examples/docker_scripts/kafka_cluster_start.sh
	    sleep 30
	    . /src/spark_expectations/examples/docker_scripts/delete_kafka_topic.sh
	    . /src/spark_expectations/examples/docker_scripts/create_kafka_topic.sh
    endif

kafka-cluster-stop:
    ifeq ($(UNIT_TESTING_ENV), spark_expectations_unit_testing_on_github_actions)
	     . ./src/spark_expectations/examples/docker_scripts/delete_kafka_topic.sh
	     . ./src/spark_expectations/examples/docker_scripts/kafka_cluster_stop.sh
	     rm -rf /tmp/kafka-logs
     endif

#TODO
cov: kafka-cluster-start
	-make check
	@hatch test -c

	make kafka-cluster-stop

dev:
	@hatch shell dev
	@hatch run dev:pre-commit install
	@hatch run dev:pre-commit install --hook-type pre-push

deploy_env_setup:
	@hatch shell dev

#TODO
test: kafka-cluster-start
	@hatch test --cover-quiet -a -vvv

	make kafka-cluster-stop

build:
	@hatch build

coverage: check test

#TODO
.PHONY: docs
docs:
	@hatch run docs:deploy-and-serve-dev

#TODO
deploy-docs:
	@poetry run mike deploy --push --update-aliases $(version) latest

#TODO - switch to hatch install, create alias called setup
poetry-install:
	@pip install --upgrade setuptools && pip install poetry && poetry self add "poetry-dynamic-versioning[plugin]"

get-version:
	@poetry version

requirements:
	@hatch run dev:uv pip freeze > requirements.txt

clean:
	@echo "\033[1mCleaning up:\033[0m\n\033[35m This will remove all local caches and build artifacts\033[0m"
	@rm -rf `find . -name __pycache__`
	@rm -f `find . -type f -name '*.py[co]'`
	@rm -f `find . -type f -name '*~'`
	@rm -f `find . -type f -name '.*~'`
	@rm -rf .run
	@rm -rf .venv
	@rm -rf .venvs
	@rm -rf .cache
	@rm -rf .pytest_cache
	@rm -rf .ruff_cache
	@rm -rf htmlcov
	@rm -rf *.egg-info
	@rm -f .coverage
	@rm -f .coverage.*
	@rm -rf build
	@rm -rf dist
	@rm -rf site
	@rm -rf docs/_build
	@rm -rf docs/.changelog.md docs/.version.md docs/.tmp_schema_mappings.html
	@rm -rf fastapi/test.db
	@rm -rf coverage.xml


# TODO - add sync command to sync dependencies in dev env
