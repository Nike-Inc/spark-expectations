## Prerequisites

### Python
- **Supported versions:** 3.9, 3.10, 3.11, 3.12 (recommended: latest 3.12.x)
- **Check version:**
```sh
    python3 --version
```
- **Install Python:** You can install Python by downloading it from [python.org](https://www.python.org/downloads/) or by using a version manager such as [pyenv](https://github.com/pyenv/pyenv).

### Java
- **Supported versions:** 8, 11, 17 (recommended: latest 17.x)
- **Recommendation:** Use a JDK version manager such as [SDKMAN!](https://sdkman.io/) or [OpenJDK](https://openjdk.org/)(Linux/macOS) or an equivalent tool for your operating system to install and manage Java versions.
- **JAVA_HOME:** If your tools or environment require it, set the `JAVA_HOME` environment variable to point to your installed JDK. Refer to your JDK version manager’s documentation for instructions.

### IDE
- **Recommended:** [Visual Studio Code](https://code.visualstudio.com/)
- **Other options:** [PyCharm](https://www.jetbrains.com/pycharm/)
- **Recommended VS Code Extensions:**
  - Python
  - Pylance
  - Black Formatter
  - YAML
  - Docker
  - Markdown

### Docker
- **Requirement:** A container engine is required for running Kafka and some integration tests. You can use Docker, Podman, containerd, Rancher, or any equivalent container runtime for your operating system.


## Github Configuration
- **Required:** A GitHub account to access the source code and documentation.

### Create GPG and SSH Keys
- **GPG:** Use GPG for signing commits and tags.  
  See [GitHub Docs – Generate a GPG Key](https://docs.github.com/en/authentication/managing-commit-signature-verification/generating-a-new-gpg-key).
- **SSH:** Use SSH for connecting to and interacting with remote repositories.  
  See [GitHub Docs – Generate an SSH Key](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent).
- **Clone the repository:**  
```sh
    git clone git@github.com:Nike-Inc/spark-expectations.git
```


## Environment Setup
- **Create a virtual environment:**
```sh
    python3 -m venv .venv
```
- **Activate virtual environment:**
  - **Linux/macOS:**
```sh
    source .venv/bin/activate
```
- **Install Dependencies:**
All required and optional dependencies are managed via `pyproject.toml`. If you use the provided make commands (`make dev` or `make deploy_env_setup`), all Python dependencies and dev tools will be installed automatically. You only need to manually install system-level dependencies (Python and Java) and VS Code extensions.

### Hatch Installation
This project uses [Hatch](https://hatch.pypa.io/latest/) for Python environment and dependency management.  
You must install Hatch before running the development setup commands.
**To install Hatch:**
```sh
pip3 install --user hatch
```
**To view Hatch Environments:**
This shows which environments are available for development, testing, and other workflows, and how they are configured for your project. 
```sh
hatch show env
```
 
### Development Environment 
Before running any tests, make sure your development environment is set up and all dependencies are installed:
  ```sh
make dev
  ```
or 
  ```sh
  make deploy_env_setup
  ```


## Running Kafka with Docker
This project provides a Docker container for running Spark-Expectations in a controlled environment. The script below will build and start a Kafka service in a Docker container, making it available for integration tests or local development.

```sh
sh ./spark_expectations/examples/docker_scripts/docker_kafka_start_script.sh  
```

This script will:
- Build the required Docker image (if it does not already exist)
- Start a Kafka service in a Docker container
- Make Kafka available for integration tests or local development

**Note:**
- Ensure Docker is installed and running on your system before executing this script.
- The script automates both the build and run steps for Kafka, so you do not need to run `docker build` or `docker run` manually.

### Adding Certificates
To enable trusted SSL/TLS communication during Spark-Expectations testing, you may need to provide custom Certificate Authority (CA) certificates. Place any required `.crt` files in the `spark_expectations/examples/docker_scripts/certs` directory. During test container startup, all certificates in this folder will be automatically imported into the container’s trusted certificate store, ensuring that your Spark jobs and dependencies can establish secure connections as needed.


## Running Tests
To run the test suite for the Spark-Expectations project, follow these steps:

**To run the full test suite:**
```sh
make test
```
**To run all tests and generate a coverage report:**
```sh
make cov
```

**Troubleshooting:**
If you encounter issues, try cleaning and recreating the environment.
```sh
make env-remove-all
make dev
```


## Configure SMTP Notifications

To enable email notifications (such as alerts for data quality failures) in Spark-Expectations, you need to configure SMTP settings. 
You can reference the `user_config.py` file in the `spark_expectations/config` directory to access / setup the SMTP configuration parameters. This file should contain the necessary SMTP/email notification settings for Spark-Expectations.

### Verifying SMTP Parameters
Before using SMTP notifications, verify that the following parameters are set correctly in your configuration (see `user_config.py` for the exact constant names):

- SMTP server host
- SMTP server port
- SMTP username
- SMTP password
- Sender email address (`from`)
- Recipient email address(es) (`to`)
- Enable/disable SMTP authentication and TLS as needed

### Script to Send a Test Email
You can use the following Python script to test your SMTP configuration. This script will send a test email using the configured SMTP settings. Make sure to replace the placeholders with your actual SMTP configuration values.
```python
from email.mime.text import MIMEText

smtp_host = "smtp.example.com"
smtp_port = 587
smtp_user = "your_email@example.com"
smtp_password = "your_password"
smtp_from = "your_email@example.com"
smtp_to = "recipient@example.com"

msg = MIMEText("This is a test email from Spark-Expectations SMTP setup.")
msg["Subject"] = "Spark-Expectations SMTP Test"
msg["From"] = smtp_from
msg["To"] = smtp_to

with smtplib.SMTP(smtp_host, smtp_port) as server:
    server.starttls()
    server.login(smtp_user, smtp_password)
    server.sendmail(smtp_from, [smtp_to], msg.as_string())

print("Test email sent successfully.")
```
**Note:**
Never commit sensitive credentials (like SMTP passwords) to version control. Use environment variables or a secure secrets manager.
Make sure your SMTP server allows connections from your environment (some providers may require app passwords or special settings).


## Testing SMTP Notifications
To test the email notifications, you can use the provided `make` commands to start and stop a local mail server. 
```sh
make local-mail-server-start
make local-mail-server-stop
```


## Library Installation

The library is available in the Python Package Index (PyPi) and can be installed in your environment using the below command or 
add the library "spark-expectations" as a dependency into the `requirements.txt` of your project, or as per your project management tool requires (e.g. poetry, hatch, uv).

```shell
pip install -U spark-expectations
```


## Required Tables

There are two tables that need to be created for spark-expectations to run seamlessly and integrate with a spark job.
The below SQL statements used three namespaces which works with Databricks Unity Catalog, but if you are using hive
please update the namespaces accordingly and also provide necessary table metadata.

### Rules Table

We need to create a rules tables which contains all the data quality rules. Please use the below template to create
your rules table for your project.

```sql
create table if not exists `catalog`.`schema`.`{product}_rules` (
    product_id STRING,  -- (1)!
    table_name STRING,  -- (2)!
    rule_type STRING,  -- (3)!
    rule STRING,  -- (4)!
    column_name STRING,  -- (5)!
    expectation STRING,  -- (6)!
    action_if_failed STRING,  -- (7)!
    tag STRING,  -- (8)!
    description STRING,  -- (9)!
    enable_for_source_dq_validation BOOLEAN,  -- (10)! 
    enable_for_target_dq_validation BOOLEAN,  -- (11)!
    is_active BOOLEAN,  -- (12)!
    enable_error_drop_alert BOOLEAN,  -- (13)!
    error_drop_threshold INT,  -- (14)!
    query_dq_delimiter STRING,  -- (15)!
    enable_querydq_custom_output BOOLEAN,  -- (16)!
);
```

1. `product_id` A unique name at the level of dq rules execution
2. `table_name` The table for which the rule is being defined for
3. `rule_type` 3 different type of rules. They are 'row_dq', 'agg_dq' and 'query_dq'
4. `rule` Short description of the rule 
5. `column_name` The column name for which the rule is defined for. This only applies for row_dq. For agg_dq and query_dq, use blank/empty value. 
6. `expectation` Provide the DQ rule condition 
7. `action_if_failed` There are 3 different types of actions. These are 'ignore', 'drop', and 'fail'. 
    Ignore: The rule is run and the output is logged. No action is performed regardless of whether the rule has succeeded or failed. Applies for all 3 rule types. 
    Drop: The rows that fail the rule get dropped from the dataset. Applies for only row_dq rule type.
    Fail: job fails if the rule fails. Applies for all 3 rule types.
8. `tag` provide some tag name to dq rule example:  completeness, validity, uniqueness etc. 
9. `description`  Long description for the rule
10. `enable_for_source_dq_validation` flag to run the agg rule
11. `enable_for_target_dq_validation` flag to run the query rule
12. `is_active` true or false to indicate if the rule is active or not. 
13. `enable_error_drop_alert` true or false. This determines if an alert notification should be sent out if row(s) is(are) dropped from the data set
14. `error_drop_threshold` Threshold for the alert notification that gets triggered when row(s) is(are) dropped from the data set
15. `query_dq_delimiter` segregate custom queries delimiter ex: $, @ etc. By default it is @. Users can override it with any other delimiter based on the need. The same delimiter mentioned here has to be used in the custom query.
16. `enable_querydq_custom_output` required custom query output in separate table


The Spark Expectation process consists of three phases:
1. When enable_for_source_dq_validation is true, execute agg_dq and query_dq on the source Dataframe
2. If the first step is successful, proceed to run row_dq
3. When enable_for_target_dq_validation is true, exeucte agg_dq and query_dq on the Dataframe resulting from row_dq

### Rule Type For Rules

The rules column has a column called "rule_type". It is important that this column should only accept one of 
these three values - `[row_dq, agg_dq, query_dq]`. If other values are provided, the library may cause unforeseen errors.
Please run the below command to add constraints to the above created rules table

```sql
ALTER TABLE `catalog`.`schema`.`{product}_rules` 
ADD CONSTRAINT rule_type_action CHECK (rule_type in ('row_dq', 'agg_dq', 'query_dq'));
```

### Examples of Supported Rule Types

- **Row DQ**: 
```sql
INSERT INTO `catalog`.`schema`.`{product}_rules` VALUES ('your_product', 'your_table', 'row_dq', 'check_nulls', 'column_name', 'is not null', 'drop', 'completeness', 'Check for null values in column_name', true, true, true, false, 0);
```

- **Aggregation DQ**: 
```sql
INSERT INTO `catalog`.`schema`.`{product}_rules` VALUES ('your_product', 'your_table', 'agg_dq', 'check_row_count', '', 'COUNT(*) > 0', 'fail', 'completeness', 'Ensure the table has at least one row', true, true, true, false, 0);
```

- **Query DQ**:
```sql
INSERT INTO `catalog`.`schema`.`{product}_rules` VALUES ('your_product', 'your_table', 'query_dq', 'check_custom_query', '', 'SELECT COUNT(*) FROM your_table WHERE column_name IS NULL', 'ignore', 'validity', 'Custom query to check for null values in column_name', false, true, true, false, 0);
```

### Action If Failed For Row, Aggregation and Query Data Quality Rules

The rules column has a column called "action_if_failed". It is important that this column should only accept one of 
these values - `[fail, drop or ignore]` for `'rule_type'='row_dq'` and `[fail, ignore]` for `'rule_type'='agg_dq' and 'rule_type'='query_dq'`. 
If other values are provided, the library may cause unforeseen errors.
Please run the below command to add constraints to the above created rules table

```sql
ALTER TABLE apla_nd_dq_rules ADD CONSTRAINT action CHECK 
((rule_type = 'row_dq' and action_if_failed IN ('ignore', 'drop', 'fail')) or 
(rule_type = 'agg_dq' and action_if_failed in ('ignore', 'fail')) or 
(rule_type = 'query_dq' and action_if_failed in ('ignore', 'fail')));
```

### DQ Stats Table

In order to collect the stats/metrics for each data quality job run, the spark-expectations job will
automatically create the stats table if it does not exist. The below SQL statement can be used to create the table
if you want to create it manually, but it is not recommended.

```sql
create table if not exists `catalog`.`schema`.`dq_stats` (
    product_id STRING,  -- (1)!
    table_name STRING,  -- (2)!
    input_count LONG,  -- (3)!
    error_count LONG,  -- (4)!
    output_count LONG,  -- (5)!
    output_percentage FLOAT,  -- (6)!
    success_percentage FLOAT,  -- (7)!
    error_percentage FLOAT,  -- (8)!
    source_agg_dq_results array<map<string, string>>,  -- (9)!
    final_agg_dq_results array<map<string, string>>,  -- (10)!
    source_query_dq_results array<map<string, string>>,  -- (11)!
    final_query_dq_results array<map<string, string>>,  -- (12)!
    row_dq_res_summary array<map<string, string>>,  -- (13)!
    row_dq_error_threshold array<map<string, string>>,  -- (14)!
    dq_status map<string, string>,  -- (15)!
    dq_run_time map<string, float>,  -- (16)!
    dq_rules map<string, map<string,int>>,  -- (17)!
    meta_dq_run_id STRING,  -- (18)!
    meta_dq_run_date DATE,  -- (19)!
    meta_dq_run_datetime TIMESTAMP,  -- (20)!
    dq_env STRING,  -- (21)!
);
```

1. `product_id` A unique name at the level of dq rules execution
2. `table_name` The table for which the rule is being defined for
3. `input_count` total input row count of given dataframe
4. `error_count` total error count for all row_dq rules
5. `output_count` total count of records that passed the row_dq rules or configured to be ignored when they fail
6. `output_percentage` percentage of total count of records that passed the row_dq rules or configured to be ignored when they fail
7. `success_percentage` percentage of total count of records that passed the row_dq rules
8. `error_percentage` percentage of total count of records that failed the row_dq rules
9. `source_agg_dq_results` results for agg dq rules are stored after row_dq rules executed in an array with each entry as a map\<string, string\> containing the following keys: `description`, `tag`, `column_name`, `rule`, `rule_type`, `status`, `action_if_failed`
10. `final_agg_dq_results` results for agg dq rules are stored after row_dq rules executed in an array with each entry as a map\<string, string\> containing the following keys: `description`, `tag`, `column_name`, `rule`, `rule_type`, `status`, `action_if_failed`
11. `source_query_dq_results` results for query dq rules are stored after row_dq rules executed in an array with each entry as a map\<string, string\> containing the following keys: `description`, `tag`, `column_name`, `rule`, `rule_type`, `status`, `action_if_failed`
12. `final_query_dq_results` results for query dq rules are stored after row_dq rules executed in an array with each entry as a map\<string, string\> containing the following keys: `description`, `tag`, `column_name`, `rule`, `rule_type`, `status`, `action_if_failed`
13. `row_dq_res_summary` summary of row dq results are stored in an array with each entry as a map\<string, string\> containing the following keys: `description`, `tag`, `column_name`, `rule`, `rule_type`, `failed_row_count`, `action_if_failed`
14. `row_dq_error_threshold` error threshold results for rules defined in the rules table for row_dq rules are stored in an array with each entry as a map\<string, string\> containing the following keys: `description`, `column_name` ,`error_drop_threshold`, `rule_type`, `action_if_failed`, `rule_name`, `error_drop_percentage`
15. `dq_status`  stores the status of the rule execution.
16. `dq_run_time` time taken by the rules
17. `dq_rules` how many dq rules are executed in this run
18. `meta_dq_run_id` unique id generated for this run
19. `meta_dq_run_date` date on which rule is executed
20. `meta_dq_run_datetime` date and time on which rule is executed
21. `dq_env` environment value passed from the user_config.se_dq_rules_params

### DQ Detailed Stats Table

This table provides detailed stats of all the expectations along with the status provided in the stats table in a relational format.
This table need not be created. It gets auto created with "_detailed " to the dq stats table name. This is optional and only get's created if the config is set to have the detailed stats table.
Below is the schema


```sql
create table if not exists `catalog`.`schema`.`dq_stats_detailed` (
run_id string,  -- (1)!    
product_id string,  -- (2)!  
table_name string,  -- (3)!  
rule_type string,  -- (4)!  
rule string,  -- (5)!
source_expectations string,  -- (6)!
tag string,  -- (7)!
description string,  -- (8)!
source_dq_status string,  -- (9)!
source_dq_actual_outcome string,  -- (10)!
source_dq_expected_outcome string,  -- (11)!
source_dq_actual_row_count string,  -- (12)!
source_dq_error_row_count string,  -- (13)!
source_dq_row_count string,  -- (14)!
source_dq_start_time string,  -- (15)!
source_dq_end_time string,  -- (16)!
target_expectations string,  -- (17)!
target_dq_status string,  -- (18)!
target_dq_actual_outcome string,  -- (19)!
target_dq_expected_outcome string,  -- (20)!
target_dq_actual_row_count string,  -- (21)!
target_dq_error_row_count string,  -- (22)!
target_dq_row_count string,  -- (23)!
target_dq_start_time string,  -- (24)!
target_dq_end_time string,  -- (25)!
dq_date date,  -- (26)!
dq_time string,  -- (27)!
dq_job_metadata_info string,  -- (28)!
);
```

1. `run_id` Run Id for a specific run 
2. `product_id` Unique product identifier 
3. `table_name` The target table where the final data gets inserted
4. `rule_type` Either row/query/agg dq
5. `rule`  Rule name
6. `source_expectations` Actual Rule to be executed on the source dq
7. `tag` completeness,uniqueness,validity,accuracy,consistency,
8. `description` Description of the Rule
9. `source_dq_status` Status of the rule execution in the Source dq
10. `source_dq_actual_outcome` Actual outcome of the Source dq check
11. `source_dq_expected_outcome` Expected outcome of the Source dq check
12. `source_dq_actual_row_count` Number of rows of the source dq
13. `source_dq_error_row_count` Number of rows failed in the source dq
14. `source_dq_row_count` Number of rows of the source dq
15. `source_dq_start_time` source dq start timestamp
16. `source_dq_end_time` source dq end timestamp
17. `target_expectations` Actual Rule to be executed on the target dq
18. `target_dq_status` Status of the rule execution in the Target dq
19. `target_dq_actual_outcome` Actual outcome of the Target dq check
20. `target_dq_expected_outcome` Expected outcome of the Target dq check
21. `target_dq_actual_row_count` Number of rows of the target dq
22. `target_dq_error_row_count` Number of rows failed in the target dq
23. `target_dq_row_count` Number of rows of the target dq
24. `target_dq_start_time` target dq start timestamp
25. `target_dq_end_time` target dq end timestamp
26. `dq_date` Dq executed date
27. `dq_time` Dq executed timestamp
28. `dq_job_metadata_info` dq job metadata
