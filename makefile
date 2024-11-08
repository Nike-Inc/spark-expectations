black-check:
	@poetry run black --check spark_expectations

fmt:
	@poetry run black spark_expectations/

check: black-check mypy
	@poetry run prospector --no-autodetect --profile prospector.yaml

mypy:
	@poetry run mypy -p spark_expectations --exclude venv --exclude dist --exclude .idea

kafka-cluster-start:
                   ifeq ($(UNIT_TESTING_ENV), spark_expectations_unit_testing_on_github_actions)
	                   . ./spark_expectations/examples/docker_scripts/kafka_cluster_start.sh
	                   sleep 30
	                   . ./spark_expectations/examples/docker_scripts/delete_kafka_topic.sh
	                   . ./spark_expectations/examples/docker_scripts/create_kafka_topic.sh
                   endif

kafka-cluster-stop:
                  ifeq ($(UNIT_TESTING_ENV), spark_expectations_unit_testing_on_github_actions)
	                   . ./spark_expectations/examples/docker_scripts/delete_kafka_topic.sh
	                   . ./spark_expectations/examples/docker_scripts/kafka_cluster_stop.sh
	                   rm -rf /tmp/kafka-logs
                   endif

cov: kafka-cluster-start
	-make check
	@poetry run coverage run --source=spark_expectations --omit "spark_expectations/examples/*" -m pytest -v -x && \
	poetry run coverage report -m && \
	poetry run coverage xml

	make kafka-cluster-stop

dev:
	@poetry install --all-extras --with dev
	@poetry run pre-commit install
	@poetry run pre-commit install --hook-type pre-push

deploy_env_setup:
	@poetry install --all-extras --with dev

test: kafka-cluster-start
	@poetry run coverage run --source=spark_expectations --omit "spark_expectations/examples/*" -m pytest && \
	poetry run coverage report -m && \
	poetry run coverage html

	make kafka-cluster-stop

build:
	@poetry build

poetry-lock-no-update:
	@poetry lock --no-update

poetry-lock:
	@poetry lock

poetry:
	@poetry install --with dev

#poetry:
#	@poetry lock
#	@poetry install --with dev

coverage: check test

docs:
	@poetry run mike deploy -u dev latest
	@poetry run mike set-default latest
	@poetry run mike serve

deploy-docs:
	@poetry run mike deploy --push --update-aliases $(version) latest

poetry-install:
	@pip install --upgrade setuptools && pip install poetry && poetry self add "poetry-dynamic-versioning[plugin]"

get-version:
	@poetry version

requirements:
	@poetry export -f requirements.txt --output requirements.txt --with dev --without-hashes

.PHONY: docs