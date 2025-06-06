# How to contribute

There are a few guidelines that we need contributors to follow so that we are able to process requests as efficiently as possible. If you have any questions or concerns please feel free to contact us at [opensource@nike.com](mailto:opensource@nike.com).

## Getting Started

* Review our [Code of Conduct](https://github.com/Nike-Inc/nike-inc.github.io/blob/master/CONDUCT.md)
* Submit the [Individual Contributor License Agreement](https://www.clahub.com/agreements/Nike-Inc/fastbreak)
* Make sure you have a [GitHub account](https://github.com/signup/free)
* Submit a ticket for your issue, assuming one does not already exist.
    * Clearly describe the issue including steps to reproduce when it is a bug.
    * Make sure you fill in the earliest version that you know has the issue.
* Fork the repository on GitHub

## Making Changes

* Create a feature branch off of `main` before you start your work.
    * Please avoid working directly on the `main` branch.
* Setup the required package manager [hatch](#-package-manager)
* Setup the dev environment [see below](#-dev-environment-setup)
* Make commits of logical units.
    * You may be asked to squash unnecessary commits down to logical units.
* Check for unnecessary whitespace with `git diff --check` before committing.
* Write meaningful, descriptive commit messages.
* Please follow existing code conventions when working on a file
* Make sure to check the standards on the code [see below](#-linting-and-standards)
* Make sure to test the code before you push changes [see below](#-testing)

## 🤝 Submitting Changes

* Push your changes to a topic branch in your fork of the repository.
* Submit a pull request to the repository in the Nike-Inc organization.
* After feedback has been given we expect responses within two weeks. After two weeks we may close the pull request 
if it isn't showing any activity.
* Bug fixes or features that lack appropriate tests may not be considered for merge.
* Changes that lower test coverage may not be considered for merge.

### 📦 Package manager

We use `make` for managing different steps of setup and maintenance in the project. You can install make by following
the instructions [here](https://formulae.brew.sh/formula/make)

We use `hatch` as our package manager. 
Checkout https://hatch.pypa.io/latest/install/ for reccomended installation method

Please DO NOT use pip or conda to install the dependencies. Instead, use hatch:

```bash
# One option for Mac users (refer to official docs)
brew install hatch
```

### 📌 Dev Environment Setup

To ensure our standards, make sure to install the required packages.

```bash
make dev
```

### 🧹 Linting and Standards

We use `pylint`, `black` and `mypy` to maintain standards in the codebase

```bash
make check
```

Make sure that the linter does not report any errors or warnings before submitting a pull request.

### 🧪 Testing

We use `pytest` to test our code. You can run the tests by running the following command:

```bash
make test
```

Make sure that all tests pass before submitting a pull request.

## 🚀 Release Process

At the moment, the release process is manual. We try to make frequent releases. Usually, we release a new version when we have a new feature or bugfix. A developer with admin rights to the repository will create a new release on GitHub, and then publish the new version to PyPI.

# Additional Resources

* [General GitHub documentation](https://help.github.com/)
* [GitHub pull request documentation](https://help.github.com/send-pull-requests/)
* [Nike's Code of Conduct](https://github.com/Nike-Inc/nike-inc.github.io/blob/master/CONDUCT.md)
* [Nike's Individual Contributor License Agreement](https://www.clahub.com/agreements/Nike-Inc/fastbreak)
* [Nike OSS](https://nike-inc.github.io/)