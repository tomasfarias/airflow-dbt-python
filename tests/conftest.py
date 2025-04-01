"""Conftest file including setting common fixtures.

Common fixtures include a connection to a postgres database, a set of sample model and
 seed files, dbt configuration files, and temporary directories for everything.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import TYPE_CHECKING, Generator, List
from unittest.mock import patch

import boto3
import pytest
from airflow import settings
from airflow.models.connection import Connection
from mockgcp.storage.client import MockClient as MockStorageClient
from moto import mock_aws
from pytest_postgresql.janitor import DatabaseJanitor

from airflow_dbt_python.hooks.dbt import DbtHook

if TYPE_CHECKING:
    from _pytest.fixtures import SubRequest


PROFILES = """
flags:
  send_anonymous_usage_stats: false

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

LOG_PATH_CONFIG = """
log-path: {log_path}
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
    schema="another",
    tags=["deprecated"],
) }}

SELECT
  123 AS field1,
  '{{ 'a' * 3  }}' AS field2
"""

MODEL_3 = """
{{ config(
    materialized="incremental",
    schema="a_schema",
    tags=["hourly"],
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
    tags=["hourly"],
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
        user=postgresql_proc.user,
        host=postgresql_proc.host,
        port=postgresql_proc.port,
        dbname=postgresql_proc.dbname,
        version=postgresql_proc.version,
        password=postgresql_proc.password,
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
def profiles_file_with_env(tmp_path_factory):
    """Create a profiles.yml file that relies on env variables for db connection."""
    p = tmp_path_factory.mktemp(".dbt_with_env") / "profiles.yml"
    profiles_content = PROFILES.format(
        host="\"{{ env_var('DBT_HOST') }}\"",
        user="\"{{ env_var('DBT_USER') }}\"",
        port="\"{{ env_var('DBT_PORT') | int }}\"",
        password="\"{{ env_var('DBT_ENV_SECRET_PASSWORD') }}\"",
        dbname="\"{{ env_var('DBT_DBNAME') }}\"",
    )
    p.write_text(profiles_content)
    return p


@pytest.fixture(scope="session")
def airflow_conns(database):
    """Create Airflow connections for testing.

    We create them by setting AIRFLOW_CONN_{CONN_ID} env variables. Only postgres
    connections are set for now as our testing database is postgres.
    """
    uris = (
        f"postgres://{database.user}:{database.password}@{database.host}:{database.port}/public?dbname={database.dbname}",
    )
    ids = ("dbt_test_postgres_1",)
    session = settings.Session()

    connections = []
    for conn_id, uri in zip(ids, uris):
        existing = session.query(Connection).filter_by(conn_id=conn_id).first()
        if existing is not None:
            # Connections may exist from previous test run.
            session.delete(existing)
            session.commit()
        connections.append(Connection(conn_id=conn_id, uri=uri))

    session.add_all(connections)

    session.commit()

    yield ids

    for conn in connections:
        session.delete(conn)

    session.commit()
    session.close()


@pytest.fixture(scope="session")
def private_key() -> tuple[str, str]:
    """Generate a private key for testing."""
    import secrets
    import string

    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    password_length = 12
    characters = string.ascii_letters + string.digits + string.punctuation
    password = "".join(secrets.choice(characters) for _ in range(password_length))

    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
    )
    pem_private_key = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.BestAvailableEncryption(
            password.encode(encoding="utf-8")
        ),
    )
    return pem_private_key.decode(encoding="utf-8"), password


@pytest.fixture
def profile_conn_id(request: SubRequest) -> Generator[str, None, None]:
    """Create an Airflow connection by conn_id."""
    conn_id = request.param
    session = settings.Session()
    existing = session.query(Connection).filter_by(conn_id=conn_id).first()
    if existing is not None:
        # Connections may exist from previous test run.
        session.delete(existing)
        session.commit()

    conn_json_path = Path(__file__).parent / "profiles" / f"{conn_id}.json"
    conn_kwargs = json.loads(conn_json_path.read_text())
    conn = Connection(conn_id=conn_id, **conn_kwargs)

    session.add(conn)

    session.commit()

    yield conn_id

    session.delete(conn)

    session.commit()
    session.close()


@pytest.fixture(scope="session")
def dbt_project_dir(tmp_path_factory):
    """A temporary directory to store dbt test files."""
    d = tmp_path_factory.mktemp("project")
    return d


@pytest.fixture(scope="function")
def logs_dir(tmp_path):
    """Create a directory to persist dbt logs."""
    d = tmp_path / "logs"
    d.mkdir(exist_ok=True)
    yield d
    shutil.rmtree(d)


@pytest.fixture(scope="function")
def dbt_project_file(dbt_project_dir, logs_dir, request):
    """Create a test dbt_project.yml file."""
    p = dbt_project_dir / "dbt_project.yml"
    PROJECT_CONTENT = PROJECT

    p.write_text(PROJECT_CONTENT)

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


@pytest.fixture(scope="function")
def compile_dir(dbt_project_file):
    """Return a path to the directory with compiled files."""
    d = dbt_project_file.parent
    return d / "target" / "compiled" / "test" / "models"


@pytest.fixture
def mocked_s3_res():
    """Return a mocked s3 resource."""
    with mock_aws():
        yield boto3.resource("s3")


@pytest.fixture
def s3_hook():
    """Provide an S3 for testing."""
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    return S3Hook()


@pytest.fixture
def s3_bucket(mocked_s3_res, s3_hook):
    """Return a mocked s3 bucket for testing.

    Bucket is cleaned after every use.
    """
    bucket = "airflow-dbt-test-s3-bucket"
    mocked_s3_res.create_bucket(Bucket=bucket)

    yield bucket

    keys = s3_hook.list_keys(
        bucket,
        "",
    )
    if keys is not None and len(keys) > 0:
        s3_hook.delete_objects(
            bucket,
            keys,
        )
        keys = s3_hook.list_keys(
            bucket,
            "",
        )
    assert keys is None or len(keys) == 0


@pytest.fixture
def gcp_conn_id():
    """Provide a GCS connection for testing."""
    from airflow.providers.google.cloud.hooks.gcs import GCSHook

    conn_id = GCSHook.default_conn_name

    session = settings.Session()
    existing = session.query(Connection).filter_by(conn_id=conn_id).first()
    if existing is not None:
        # Connections may exist from previous test run.
        session.delete(existing)
        session.commit()

    conn = Connection(conn_id=conn_id, conn_type=GCSHook.conn_type)

    session.add(conn)

    session.commit()

    yield conn_id

    session.delete(conn)

    session.commit()
    session.close()


@pytest.fixture
def mocked_gcs_client():
    """Provide mock Google Storage Client for testing."""
    with patch("google.cloud.storage.client.Client", MockStorageClient):
        yield MockStorageClient(project="test-project")


@pytest.fixture
def gcs_hook(gcp_conn_id):
    """Provide an GCS for testing."""
    from airflow_dbt_python.hooks.fs.gcs import DbtGCSFSHook

    with patch(
        "airflow.providers.google.cloud.hooks.gcs.GCSHook.get_credentials_and_project_id",
        lambda x: ({}, "test-project"),
    ):
        with patch("google.cloud.storage.Client", MockStorageClient):
            yield DbtGCSFSHook()


@pytest.fixture
def gcs_bucket(mocked_gcs_client, gcs_hook):
    """Return a mocked gcs bucket for testing.

    Bucket is cleaned after every use.
    """
    bucket_name = "airflow-dbt-test-gcs-bucket"
    bucket = mocked_gcs_client.create_bucket(bucket_name)

    yield bucket_name

    bucket.delete()


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


@pytest.fixture(scope="function")
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
def pre_compile(hook, model_files, seed_files, dbt_project_file, profiles_file):
    """Fixture to run a dbt compile task."""
    target_dir = dbt_project_file.parent / "target"

    hook.run_dbt_task(
        "compile",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        upload_dbt_project=True,
        delete_before_upload=True,
        full_refresh=True,
    )

    yield

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
def macro_name(dbt_project_dir):
    """Create a dbt macro file."""
    d = dbt_project_dir / "macros"
    d.mkdir(exist_ok=True)
    m = d / "my_macro.sql"
    m.write_text(MACRO)
    return "my_macro"


NON_ARG_MACRO = """
{% macro one() %}
  1
{% endmacro %}
"""


@pytest.fixture
def non_arg_macro_name(dbt_project_dir):
    """Create a dbt macro file."""
    d = dbt_project_dir / "macros"
    d.mkdir(exist_ok=True)
    m = d / "my_non_arg_macro.sql"
    m.write_text(NON_ARG_MACRO)
    return "one"


@pytest.fixture
def test_files(tmp_path_factory, dbt_project_file):
    """Create test files for backends."""
    d = tmp_path_factory.mktemp("test_files_dir")
    seed_dir = d / "seeds"
    seed_dir.mkdir(exist_ok=True)
    f1 = seed_dir / "a_seed.csv"

    with open(f1, "w+") as f:
        f.write("col1,col2\n1,2")

    models_dir = d / "models"
    models_dir.mkdir(exist_ok=True)
    f2 = models_dir / "a_model.sql"
    with open(f2, "w+") as f:
        f.write("SELECT 1")
    f3 = models_dir / "another_model.sql"
    with open(f3, "w+") as f:
        f.write("SELECT 2")

    shutil.copyfile(dbt_project_file, d / "dbt_project.yml")

    yield [f1, f2, f3]

    f1.unlink()
    f2.unlink()
    f3.unlink()


def pytest_addoption(parser):
    """Adds option to run integration tests with pytest."""
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="run integration tests",
    )


def pytest_collection_modifyitems(config, items):
    """Allows skipping integration tests when flag missing."""
    if config.getoption("--run-integration"):
        return

    skip_integration = pytest.mark.skip(
        reason="need --run-integration to run integration tests"
    )
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip_integration)


@pytest.fixture(scope="session")
def assert_dir_contents():
    """Helper function to assert contents of dir_url contain expected."""
    from airflow_dbt_python.utils.url import URL, URLLike

    def wrapper(dir_url: URLLike, expected: List[URL], exact: bool = True):
        """Assert file URLs under dir_url match expected."""
        url = URL(dir_url)
        dir_contents = [f for f in url if not f.is_dir()]

        if exact is True:
            assert sorted(dir_contents, key=lambda u: u.name) == sorted(
                expected, key=lambda u: u.name
            )
        else:
            missing_contents = [exp for exp in expected if exp not in dir_contents]
            assert (
                len(missing_contents) == 0
            ), f"Missing dir contents: {missing_contents}"

    return wrapper
