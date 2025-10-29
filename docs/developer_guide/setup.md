## Prerequisites

### Python
- **Supported versions:** 3.9, 3.10, 3.11, 3.12 (recommended: latest 3.12.x)
- **Check version:**
```sh
    python3 --version
```
- **Install Python:** You can install Python by downloading it from [python.org](https://www.python.org/downloads/) or by using a version manager such as [pyenv](https://github.com/pyenv/pyenv).

### Hatch
- **Install Hatch**
```sh
  # MacOS
  brew install hatch
```

### Java
- **Supported versions:** 8, 11, 17 (recommended: latest 17.x)
- **Recommendation:** Use a JDK version manager such as [SDKMAN!](https://sdkman.io/) or [OpenJDK](https://openjdk.org/)(Linux/macOS) or an equivalent tool for your operating system to install and manage Java versions.
- **JAVA_HOME:** If your tools or environment require it, set the `JAVA_HOME` environment variable to point to your installed JDK. Refer to your JDK version manager’s documentation for instructions.

### IDE
- **Recommended:** [Visual Studio Code](https://code.visualstudio.com/)
- **Other options:** [PyCharm](https://www.jetbrains.com/pycharm/)

### Docker
- **Requirement:** A container engine is required for running integration tests. You can use Docker, Podman, containerd, Rancher, or any equivalent container runtime for your operating system.


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

We recommend using version manager tool like:

 - [pyenv](https://github.com/pyenv/pyenv) - Python version manager
 - [asdf](https://github.com/asdf-vm/asdf) - Multiple language version manager
    - [supported plugins](https://github.com/asdf-vm/asdf-plugins)


**Create a virtual environment:**
```sh
    pyenv install -l | less # List available versions
    pyenv install 3.12.11 # Install specific python version
    pyenv versions # List installed versions
    pyenv prefix # Display full path to the current installed python directory
    pyenv global <version>   # sets default for all shells
    pyenv local <version>    # sets version for current directory (creates .python-version)
```

**Install Dependencies:**

This project uses [Hatch](https://hatch.pypa.io/latest/) for Python environment and dependency management.  

All required and optional dependencies are managed via `pyproject.toml`. 

To initialize dev environment (python virtual environments) and install dependencies run following makefile target:

```sh
  # Configures  Hatch dev Environment 
  # Initializes Python virtual environments ( 3.10,3.11,3.12)
  # Installs Dependencies
  make dev
```

**To view Hatch Environments:**

This shows which environments are available for development, testing, and other workflows, and how they are configured for your project. 
```sh
hatch env show
```

Since `Hatch` is used as dependency management tool output of this command will be creation of multiple python virtual environments stored under project root `.venv/env/virtual/<dev.py3.X>`


**Troubleshooting Hatch Environments:**

If you encounter issues, try cleaning and recreating the environment.
```sh
make env-remove-all
# Manual option would be to delete <project_root>./venv/ directory to delete hatch python virtual environments

# Run following to re-create environments
make dev
```

### Running Tests

To execture all spark-expectations tests run following command: 

```bash
make cov
```

!!! warning
    Previous command will spin up docker container needed to execute some tests so make sure docker is running.
    Checkout `./containers/kafka/scripts` and test fixtures to better understand what is being spin up.
    For Example
    ```sh
      sh ./containers/kafka/scripts/docker_kafka_start_script.sh  
    ```
    This script will:

    - Build the required Docker image
    - Start a Kafka service in a Docker container
    - Make Kafka available for integration tests or local development

#### IDE Debugging

??? note "VSCode Interpreter and Launch Config"
    
    ??? note "settings.json"

        Setting default python interpreter be project created virtual environment
        ```json 
        {
            "python.testing.pytestEnabled": true,
            "python.testing.autoTestDiscoverOnSaveEnabled": true,
            "python.defaultInterpreterPath": ".venv/env/virtual/dev.py3.12/bin/python",
            "terminal.integrated.profiles.osx": {
                "Hatch Shell": {
                    "path": "hatch",
                    "args": ["shell"],
                    "icon": "terminal"
                }
            },
            "terminal.integrated.defaultProfile.osx": "zsh",
            "python.testing.pytestArgs": [
                "--maxfail=1",
                "--disable-warnings"
            ]
        }
        ```
    ??? note "launch.json"
        debug launch configurations
        ```json
        {
            "version": "0.2.0",
            "configurations": [
                {
                    "name": "Coverage: Pytest (All files)",
                    "type": "debugpy",
                    "request": "launch",
                    "module": "coverage",
                    "args": [
                        "run",
                        "--source=spark_expectations",
                        "--omit=examples/*",
                        "-m", "pytest",
                        "-v",
                        "-x"
                    ],
                    "console": "integratedTerminal",
                    "justMyCode": false
                },
                {
                    "name": "Coverage: Pytest (Selected File)",
                    "type": "debugpy",
                    "request": "launch",
                    "module": "coverage",
                    "args": [
                        "run",
                        "--source=spark_expectations",
                        "--omit=examples/*",
                        "-m", "pytest",
                        "-v",
                        "-x",
                        "${file}"
                    ],
                    "console": "integratedTerminal",
                    "justMyCode": false
                }
            ]
        }
        ```


### Adding Certificates

To enable trusted SSL/TLS communication during Spark-Expectations testing, you may need to provide custom Certificate Authority (CA) certificates.

Place any required `.crt` files in the `containers/certs` directory. 

During test container startup, all certificates in this folder will be automatically imported into the container’s trusted certificate store, ensuring that your Spark jobs and dependencies can establish secure connections as needed.



### Deploying the Docs site locally

When updating the project documnetation, it is good idea to test your changes locally. You could deploy the server locally following these steps:

```sh
# Installs Dependencies
make dev

# Deploy the Docs server locally
make docs 
```
