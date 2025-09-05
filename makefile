export HATCH_DATA_DIR=.venv
DEFAULT_HATCH_ENV=dev.py3.12
GIT_REMOTE=upstream
LOCAL_ARGS="-d --build --remove-orphans"
HATCH_FEATURES=$(shell hatch env show dev --json | jq -r '.["dev.py3.12"].features | join(",")')

black-check:
    @hatch run $(DEFAULT_HATCH_ENV):fmt-check

build:
	@hatch build

check: black-check mypy
	@hatch run $(DEFAULT_HATCH_ENV):analysis

cov: kafka-cluster-start
	-make check
	@hatch run dev:coverage-failfast
	make kafka-cluster-stop

coverage: check test

deploy-docs:
	@VERSION=$(version) hatch run $(DEFAULT_HATCH_ENV):deploy-docs

deploy_env_setup:
	@hatch env create
	@hatch env create dev
	@hatch run $(DEFAULT_HATCH_ENV):uv pip install --upgrade uv pip
	@hatch run $(DEFAULT_HATCH_ENV):uv pip install .[$(HATCH_FEATURES)]
	@hatch run $(DEFAULT_HATCH_ENV):uv pip freeze

dev:
	@echo "DEFAULT ENV DIRECTORY: $$HATCH_DATA_DIR"
	@echo "DEFAULT_PYTHON_ENV: $(DEFAULT_HATCH_ENV)"
	@hatch env create
	@hatch env show dev
	@hatch run $(DEFAULT_HATCH_ENV):uv pip install --upgrade uv pip
	@hatch run $(DEFAULT_HATCH_ENV):uv pip install .[$(HATCH_FEATURES)]
	@hatch run dev:uv pip freeze
	@hatch run $(DEFAULT_HATCH_ENV):setup-hooks

docs:
	@hatch run $(DEFAULT_HATCH_ENV):deploy-and-serve-docs

env-remove-all:
	@echo "Removing all hatch environments..."
	@hatch env prune

env-remove-default:
	@echo "Removing $(DEFAULT_HATCH_ENV) hatch environments..."
	@hatch env remove $(DEFAULT_HATCH_ENV)

fmt:
	@hatch run $(DEFAULT_HATCH_ENV):fmt

generate-mailserver-certs:
	@echo "Generating mail server certificates..."
	@cd containers/certs && openssl req -x509 -newkey rsa:4096 -keyout mailpit.key -out mailpit.crt -days 365 -nodes -subj "/CN=localhost"

get-version:
	@echo "Print Version Build tool will use for building and publishing"
	@git fetch $(GIT_REMOTE) --tags
	@hatch version

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

LOCAL_SE_CONTAINER_ARGS='--build'
local-se-server-start:
	@echo "Starting local SparkExpectation environment (kafka, jupyterlab, mail server)..."
	@echo "Use 'make local-se-server-start ARGS=\"--build -d\"' to override default start arguments"
	@ARGS="$(ARGS)"; \
	if [ "$$ARGS" = "" ]; then ARGS=$(LOCAL_SE_CONTAINER_ARGS); fi; \
	docker compose -f containers/compose.yaml up $$ARGS

local-se-server-stop:
	@echo "Stopping local SparkExpectation environment..."
	@docker compose -f containers/compose.yaml down	

local-kafka-cluster-start:
	@echo "Starting local Kafka cluster..."
	. ./spark_expectations/examples/docker_scripts/docker_kafka_start_script.sh

mypy:
	hatch run $(DEFAULT_HATCH_ENV):type-check

python-versions:
	@echo "Python versions available in hatch:"
	@hatch python show

set-git-remote:
	@echo "Setting git remote to $(GIT_REMOTE)"
	@git remote add $(GIT_REMOTE) git@github.com:nike-inc/spark-expectations.git

test: kafka-cluster-start
	@hatch run $(DEFAULT_HATCH_ENV):coverage-ignore-failure
	make kafka-cluster-stop

# make test-arg TEST=tests/sinks/utils/test_collect_statistics.py::test_collect_stats_on_success_failure 
test-arg:
	@hatch -e $(DEFAULT_HATCH_ENV) run pytest -ra -vv $(TEST)

.PHONY: black-check build check cov coverage deploy-docs deploy_env_setup dev docs env-remove-all env-remove-default fmt generate-mailserver-certs get-version kafka-cluster-start kafka-cluster-stop local-kafka-cluster-start local-se-server-start local-se-server-stop mypy python-versions set-git-remote test test-arg