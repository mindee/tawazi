# Changelog

## v0.2.0 (2022-10-17)

### New
* :green_heart: CI with tests on 6 different environments. (Coverage: **99%**)
* :white_check_mark: Tested on python 3.6, 3.7, 3.8, 3.9, 3.10.
* :white_check_mark: Tested on Windows and Linux.
* :recycle: Change the default DAG interface creation to make it ergonomic.
* :zap: choose Nodes that have more successors over Nodes that no successors.
* :sparkles: execute the DAG safely without using the scheduler.
* :sparkles: test the code in the README.
* :wrench: :technologist: Dynamic configuration of the library with custom environment variables.
* :memo: :technologist: Static general documentation with mkdocs.
* :label: Repository passes mypy (not tested on the `--strict` option which is too coercive at the moment).
* :loud_sound: Changed logging to loguru by default.
* :sparkles: Implementation of the subgraph execution feature.
* :green_heart: Dependabot integration.
* :arrow_up: Switched from requirements to poetry for dependency management.
* :art: Revamp of the repository.
* :technologist: :green_heart: Better pre-commit configuration for ease of development.

## v0.1.0 (2022-07-20)

### New
* :sparkles: first functional iteration of the DAG
