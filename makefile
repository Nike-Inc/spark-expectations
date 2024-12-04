ruff-check:
	@hatch run ruff-check

black-check:
	@echo "We've switched to ruff, please use ruff instead of black"
	@make ruff-check

fmt:
	@hatch fmt

mypy:
	@hatch run mypy-check

check: ruff-check mypy
	@hatch run check

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

#TODO
cov: kafka-cluster-start
	-make check
	@poetry run coverage run --source=src --omit "spark_expectations/examples/*" -m pytest -v -x && \
	poetry run coverage report -m && \
	poetry run coverage xml

	make kafka-cluster-stop

dev:
	@hatch shell dev
	@hatch run dev:pre-commit install
	@hatch run dev:pre-commit install --hook-type pre-push

deploy_env_setup:
	@hatch shell dev

#TODO
test: kafka-cluster-start
	@poetry run coverage run --source=spark_expectations --omit "spark_expectations/examples/*" -m pytest && \
	poetry run coverage report -m && \
	poetry run coverage html

	make kafka-cluster-stop

build:
	@hatch build

coverage: check test

#TODO
.PHONY: docs
docs:
	@poetry run mike deploy -u dev latest
	@poetry run mike set-default latest
	@poetry run mike serve

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

