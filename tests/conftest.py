import pytest
from pytest_postgresql.janitor import DatabaseJanitor

PROFILES = """
default:
  target: test
  outputs:
    test:
      type: postgres
      host: {host}
      user: {user}
      port: {port}
      password: {password}
      dbname: {dbname}
      schema: public
"""

PROJECT = """
name: test
profile: default
config-version: 2
version: 1.0.0
"""

MODEL1 = """
SELECT
  123 AS field1,
  'abc' AS field2
"""

MODEL2 = """
SELECT
  123 AS field1,
  'abc' AS field2
"""


@pytest.fixture(scope="session")
def database(postgresql_proc):
    janitor = DatabaseJanitor(
        postgresql_proc.user,
        postgresql_proc.host,
        postgresql_proc.port,
        postgresql_proc.dbname,
        postgresql_proc.version,
        postgresql_proc.password,
    )
    janitor.init()
    yield postgresql_proc
    janitor.drop()


@pytest.fixture(scope="session")
def profiles_file(tmp_path_factory, database):
    p = tmp_path_factory.mktemp(".dbt") / "profiles.yml"
    profiles_content = PROFILES.format(
        host=database.host,
        user=database.user,
        port=database.port,
        password=database.password,
        dbname=database.dbname,
    )
    p.write_text(profiles_content)
    return p


@pytest.fixture(scope="session")
def dbt_project_dir(tmp_path_factory):
    d = tmp_path_factory.mktemp("project")
    return d


@pytest.fixture(scope="session")
def dbt_project_file(dbt_project_dir):
    p = dbt_project_dir / "dbt_project.yml"
    p.write_text(PROJECT)
    return p


@pytest.fixture(scope="session")
def model_files(dbt_project_dir):
    d = dbt_project_dir / "models"
    d.mkdir(exist_ok=True)
    m1 = d / "model1.sql"
    m1.write_text(MODEL1)
    m2 = d / "model2.sql"
    m2.write_text(MODEL2)
    return [m1, m2]
