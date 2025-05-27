export HATCH_DATA_DIR=.venv
DEFAULT_HATCH_ENV=dev.py3.12
GIT_REMOTE=upstream
HATCH_FEATURES=$(shell hatch env show dev --json | jq -r '.["dev.py3.12"].features | join(",")')

black-check:
	@hatch run $(DEFAULT_HATCH_ENV):fmt-check

fmt:
	@hatch run $(DEFAULT_HATCH_ENV):fmt

check: black-check mypy
	@hatch run $(DEFAULT_HATCH_ENV):analysis

mypy:
	hatch run $(DEFAULT_HATCH_ENV):type-check

local-kafka-cluster-start:
	@echo "Starting local Kafka cluster..."
	. ./spark_expectations/examples/docker_scripts/docker_kafka_start_script.sh

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
	@hatch run dev:coverage-failfast

	make kafka-cluster-stop

python-versions:
	@echo "Python versions available in hatch:"
	@hatch python show

dev:
	@echo "DEFAULT ENV DIRECTORY: $$HATCH_DATA_DIR"
	@echo "DEFAULT_PYTHON_ENV: $(DEFAULT_HATCH_ENV)"
	@hatch env create
	@hatch env show dev
	@hatch run $(DEFAULT_HATCH_ENV):uv pip install --upgrade uv pip
	@hatch run $(DEFAULT_HATCH_ENV):uv pip install .[$(HATCH_FEATURES)]
	@hatch run dev:uv pip freeze
	@hatch run $(DEFAULT_HATCH_ENV):setup-hooks

env-remove-default:
	@echo "Removing $(DEFAULT_HATCH_ENV) hatch environments..."
	@hatch env remove $(DEFAULT_HATCH_ENV)
env-remove-all:
	@echo "Removing all hatch environments..."
	@hatch env prune

deploy_env_setup:
	@hatch env create dev
	@hatch run dev:uv pip install --upgrade uv pip
	@hatch run dev:uv pip install .[$(HATCH_FEATURES)]
	@hatch run dev:uv pip freeze

test: kafka-cluster-start
	@hatch run $(DEFAULT_HATCH_ENV):coverage-ignore-failure
	make kafka-cluster-stop

# make test-arg TEST=tests/sinks/utils/test_collect_statistics.py::test_collect_stats_on_success_failure 
test-arg:
	@hatch -e $(DEFAULT_HATCH_ENV) run pytest -ra -vv $(TEST)

build:
	@hatch build

coverage: check test

docs:
	@hatch run $(DEFAULT_HATCH_ENV):deploy-and-serve-docs

deploy-docs:
	@VERSION=$(version) hatch run $(DEFAULT_HATCH_ENV):deploy-docs

set-git-remote:
	@echo "Setting git remote to $(GIT_REMOTE)"
	@git remote add $(GIT_REMOTE) git@github.com:nike-inc/spark-expectations.git
get-version:
	@echo "Print Version Build tool will use for building and publishing"
	@git fetch $(GIT_REMOTE) --tags
	@hatch version

.PHONY: docs