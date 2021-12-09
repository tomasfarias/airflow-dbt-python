"""Conftest file including setting common fixtures.

Common fixtures include a connection to a postgres database, a set of sample model and
 seed files, dbt configuration files, and temporary directories for everything.
"""
import boto3
import pytest
from moto import mock_s3
from pytest_postgresql.janitor import DatabaseJanitor

from airflow_dbt_python.hooks.dbt import DbtHook

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

PROJECT += """
dispatch:
  - macro_namespace: dbt_utils
    search_order: [dbt_utils]
"""

MODEL_1 = """
{{ config(
    materialized="ephemeral",
) }}

SELECT
  123 AS field1,
  'abc' AS field2
"""

MODEL_2 = """
{{ config(
    materialized="table",
    schema="another"
) }}

SELECT
  123 AS field1,
  '{{ 'a' * 3  }}' AS field2
"""

MODEL_3 = """
{{ config(
    materialized="incremental",
    schema="a_schema"
) }}

SELECT
  123 AS field1,
  NOW() AS field2

{% if is_incremental() %}

  WHERE NOW() > (SELECT MAX(field2) FROM {{ this }})

{% endif %}
"""

MODEL_4 = """
{{ config(
    materialized="view",
) }}
{% set l = ("a", "b", "c") %}

SELECT
  field1 AS field1,
  field2 AS field2,
  SUM(CASE WHEN 'd' IN {{ l }} THEN 1 ELSE 0 END) AS field3
FROM
  {{ ref('model_1') }}
GROUP BY
  field1, field2
"""

MODELS = [MODEL_1, MODEL_2, MODEL_3, MODEL_4]

SOURCES = """\
version: 2

sources:
  - name: a_source
    schema: public
    tables:
      - name: my_source_1

  - name: another_source
    schema: public
    tables:
      - name: my_source_2
"""

SEED_1 = """\
country_code,country_name
US,United States
CA,Canada
GB,United Kingdom
"""

SEED_2 = """\
id,name
1,Harmony Jeans
2,Lelah Small
3,Bok Hogsett
4,Micaela Talton
5,Roxy Hellyer
6,Wilfredo Carwell
7,Zola Blocker
8,Junita Do
9,Tarra Delsignore
10,Ward Fugitt
"""


@pytest.fixture(scope="session")
def database(postgresql_proc):
    """Initialize a test postgres database."""
    janitor = DatabaseJanitor(
        postgresql_proc.user,
        postgresql_proc.host,
        postgresql_proc.port,
        postgresql_proc.dbname,
        postgresql_proc.version,
        postgresql_proc.password,
    )
    janitor.init()

    with janitor.cursor() as cur:
        cur.execute(
            """\
        CREATE TABLE my_source_1 (id serial PRIMARY KEY, num integer);
        CREATE TABLE my_source_2 (id serial PRIMARY KEY, num integer);
        """
        )

    yield postgresql_proc
    janitor.drop()


@pytest.fixture(scope="session")
def profiles_file(tmp_path_factory, database):
    """Create a profiles.yml file for testing."""
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
    """A temporary directory to store dbt test files."""
    d = tmp_path_factory.mktemp("project")
    return d


@pytest.fixture(scope="session")
def dbt_project_file(dbt_project_dir):
    """Create a test dbt_project.yml file."""
    p = dbt_project_dir / "dbt_project.yml"
    p.write_text(PROJECT)
    return p


@pytest.fixture(scope="session")
def model_files(dbt_project_dir):
    """Create test model files."""
    d = dbt_project_dir / "models"
    d.mkdir(exist_ok=True)

    paths = []
    for n, model in enumerate(MODELS):
        m = d / "model_{0}.sql".format(n + 1)
        m.write_text(model)
        paths.append(m)
    return paths


@pytest.fixture(scope="session")
def sources_file(model_files, database):
    """Create test source file."""
    m = model_files[0].parent / "my_sources.yml"
    m.write_text(SOURCES)
    return m


@pytest.fixture(scope="session")
def seed_files(dbt_project_dir):
    """Create test seed files."""
    d = dbt_project_dir / "seeds"
    d.mkdir(exist_ok=True)
    s1 = d / "seed_1.csv"
    s1.write_text(SEED_1)
    s2 = d / "seed_2.csv"
    s2.write_text(SEED_2)
    return [s1, s2]


@pytest.fixture(scope="session")
def compile_dir(dbt_project_file):
    """Return a path to the directory with compiled files."""
    d = dbt_project_file.parent
    return d / "target" / "compiled" / "test" / "models"


@pytest.fixture
def mocked_s3_res():
    """Return a mocked s3 resource."""
    with mock_s3():
        yield boto3.resource("s3")


@pytest.fixture
def s3_bucket(mocked_s3_res):
    """Return a mocked s3 bucket for testing."""
    bucket = "airflow-dbt-test-s3-bucket"
    mocked_s3_res.create_bucket(Bucket=bucket)
    return bucket


BROKEN_SQL = """
SELECT
  field1 AS field1
FROM
  non_existent_table
WHERE
  field1 > 1
"""


@pytest.fixture
def broken_file(dbt_project_dir):
    """Create a malformed SQL file for testing."""
    d = dbt_project_dir / "models"
    m = d / "broken.sql"
    m.write_text(BROKEN_SQL)
    yield m
    m.unlink()


@pytest.fixture(scope="function")
def dbt_packages_dir(dbt_project_file):
    """Create a dbt_packages dir to install packages."""
    d = dbt_project_file.parent
    return d / "dbt_packages"


PACKAGES = """
packages:
  - package: dbt-labs/dbt_utils
    version: 0.8.0
"""


@pytest.fixture(scope="session")
def packages_file(dbt_project_file):
    """Create a test packages.yml file."""
    d = dbt_project_file.parent
    packages = d / "packages.yml"
    packages.write_text(PACKAGES)
    return packages


@pytest.fixture
def hook():
    """Provide a DbtHook."""
    return DbtHook()


@pytest.fixture
def pre_compile(hook, dbt_project_file, profiles_file):
    """Fixture to run a dbt compile task."""
    import shutil

    factory = hook.get_config_factory("run")
    config = factory.create_config(
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
    )
    hook.run_dbt_task(config)
    yield
    target_dir = dbt_project_file.parent / "target"
    shutil.rmtree(target_dir, ignore_errors=True)


GENERIC_TESTS = """
version: 2

models:
  - name: model_2
    columns:
      - name: field1
        tests:
          - unique
          - not_null
          - accepted_values:
              values: ['123', '456']
      - name: field2
        tests:
          - unique
          - not_null
"""


@pytest.fixture(scope="session")
def generic_tests_files(dbt_project_dir):
    """Create a dbt generic test YAML file."""
    d = dbt_project_dir / "models"
    d.mkdir(exist_ok=True)

    schema = d / "schema.yml"
    schema.write_text(GENERIC_TESTS)

    return [schema]


SINGULAR_TEST_1 = """
SELECT *
FROM {{ ref('model_2' )}}
WHERE field1 != 123
"""

SINGULAR_TEST_2 = """
SELECT *
FROM {{ ref('model_4' )}}
WHERE field1 != 123
"""


@pytest.fixture(scope="session")
def singular_tests_files(dbt_project_dir):
    """Create singular test files."""
    d = dbt_project_dir / "tests"
    d.mkdir(exist_ok=True)

    test1 = d / "singular_test_1.sql"
    test1.write_text(SINGULAR_TEST_1)

    test2 = d / "singular_test_2.sql"
    test2.write_text(SINGULAR_TEST_2)

    return [test1, test2]


SNAPSHOT_1 = """
{% snapshot test_snapshot %}

{{
    config(
      target_database='test',
      target_schema='test',
      unique_key='id_field',

      strategy='timestamp',
      updated_at='time_field',
    )
}}

SELECT
  1 AS id_field,
  'abc' AS value_field,
  NOW() AS time_field

{% endsnapshot %}
"""


@pytest.fixture(scope="session")
def snapshot_files(dbt_project_dir):
    """Create dbt snapshot files."""
    d = dbt_project_dir / "snapshots"
    d.mkdir(exist_ok=True)

    snap = d / "snapshot_1.sql"
    snap.write_text(SNAPSHOT_1)

    return [snap]


MACRO = """
{% macro my_macro(an_arg) %}
{% set sql %}
  SELECT {{ an_arg }} as the_arg;
{% endset %}

{% do run_query(sql) %}
{% endmacro %}
"""


@pytest.fixture
def macro_file(dbt_project_dir):
    """Create a dbt macro file."""
    d = dbt_project_dir / "macros"
    d.mkdir(exist_ok=True)
    m = d / "my_macro.sql"
    m.write_text(MACRO)
    return m
