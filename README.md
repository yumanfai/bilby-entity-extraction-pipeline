## Airflow Setup

First, git clone this repo.

```sh
git clone <REPONAME>
```

Then, **_in this project's directory_**, run the following command:

```sh
cp .env.example .env
echo "AIRFLOW_HOME=$(pwd)/" >> .env
```

To start the airflow standalone server, run:

```sh
uv run --env-file .env airflow standalone
```

If the Airflow files are created somewhere else other this repository, you can unset AIRFLOW_HOME first with:

```sh
unset AIRFLOW_HOME
```
