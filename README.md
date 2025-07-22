# Electronic Monitoring Data Integratio Tests
This repository contains the integration testing suite for Electronic Monitoring Data Integraton Tests.

## Prerequisites 
The framework uses the following core technologies:

-   Python 3.12
-   uv

## Getting Started
To get started with the testing framework, you need to have Python 3.12 and UV installed.

- Follow the instalation steps for [Microsoft SQL Server 18](https://learn.microsoft.com/th-th/sql/connect/odbc/linux-mac/install-microsoft-odbc-driver-sql-server-macos?view=sql-server-2018)

- Ensure you have a `.env` file has the following environment variables populated 
    - `ENV`
    - `ACCOUNT_ID`
    - `REGION`
    > **NOTE:** Environment variables below are required for the DMS Test.
    - `RDS_HOST`
    - `PORT`
    - `HOST_NAME`
    - `USER_NAME`
    - `PASSWORD`

- run the following command to run all of the tests `uv run pytest` or if you want to run a specific file `uv run pytest {path to step file}`

## Linting
This repo uses `pre-commit` and `ruff` to run pre-commit before you `commit` run the following command `uv run pre-commit run --all-files`. The linting rules are defined in the `pyproject.toml`.

