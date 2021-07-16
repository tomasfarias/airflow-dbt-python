import pytest
from dbt.version import __version__ as DBT_VERSION
from packaging.version import parse
from pytest_postgresql.janitor import DatabaseJanitor

DBT_VERSION = parse(DBT_VERSION)
IS_DBT_VERSION_0_20 = DBT_VERSION.minor == 20 and DBT_VERSION.major == 0

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

if IS_DBT_VERSION_0_20:
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

    paths = []
    for n, model in enumerate(MODELS):
        m = d / "model_{0}.sql".format(n + 1)
        m.write_text(model)
        paths.append(m)
    return paths


@pytest.fixture(scope="session")
def seed_files(dbt_project_dir):
    d = dbt_project_dir / "data"
    d.mkdir(exist_ok=True)
    s1 = d / "seed_1.csv"
    s1.write_text(SEED_1)
    s2 = d / "seed_2.csv"
    s2.write_text(SEED_2)
    return [s1, s2]


@pytest.fixture(scope="session")
def compile_dir(dbt_project_file):
    d = dbt_project_file.parent
    return d / "target" / "compiled" / "test" / "models"
