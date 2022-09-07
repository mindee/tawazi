# Contributing to tawazi

Everything you need to know to contribute efficiently to the project.



## Codebase structure

- [tawazi](https://github.com/mindee/tawazi/tree/master/tawazi) - The package codebase
- [tests](https://github.com/mindee/tawazi/tree/master/tests) - Python unit tests


## Continuous Integration

This project uses the following integrations to ensure proper codebase maintenance:

- [Github Worklow](https://help.github.com/en/actions/configuring-and-managing-workflows/configuring-a-workflow) - run jobs for package build and coverage

As a contributor, you will only have to ensure coverage of your code by adding appropriate unit testing of your code.



## Feedback

### Feature requests & bug report

Whether you encountered a problem, or you have a feature suggestion, your input has value and can be used by contributors to reference it in their developments. For this purpose, we advise you to use GitHub [issues](https://github.com/mindee/tawazi/issues).

First, check whether the topic wasn't already covered in an open / closed issue. If not, feel free to open a new one! When doing so, use issue templates whenever possible and provide enough information for other contributors to jump in.

### Questions

If you are wondering how to do something with tawazi, or a more general question, you should consider checking out GitHub [discussions](https://github.com/mindee/tawazi/discussions). See it as a Q&A forum, or the tawazi-specific StackOverflow!


## Developing tawazi

### Developer mode installation

Install all additional dependencies with the following command:

```shell
poetry install --with dev
```

and hook pre-commit

```shell
poetry run pre-commit install
```

### Commits

- **Code**: ensure to provide docstrings to your Python code. In doing so, please follow [Google-style](https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html) so it can ease the process of documentation later.
- **Commit message**: please follow [Udacity guide](http://udacity.github.io/git-styleguide/)


### Unit tests

In order to run the same unit tests as the CI workflows, you can run unittests locally:

```shell
poetry run pytest
```

### Code quality

To run all quality checks together (static type-checking, linting, formatting, import order, and additional quality checks)

```shell
poetry run pre-commit run -a
```
