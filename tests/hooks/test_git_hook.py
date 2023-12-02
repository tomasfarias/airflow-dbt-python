"""Unit test module for DbtGitRemoteHook."""
import multiprocessing
import os
import platform
import shutil

import pytest
from dulwich.repo import Repo
from dulwich.server import DictBackend, TCPGitServer

from airflow_dbt_python.hooks.git import DbtGitRemoteHook
from airflow_dbt_python.utils.url import URL

JAFFLE_SHOP = os.getenv("GIT_TEST_REPO", "tomasfarias/jaffle_shop")
JAFFLE_SHOP_PRIVATE = os.getenv(
    "GIT_PRIVATE_TEST_REPO", "tomasfarias/jaffle_shop_private"
)
GITHUB = "github.com"
GITLAB = "gitlab.com"

no_git_local_server = pytest.mark.skipif(
    platform.system() != "Linux",
    reason="Git local server may not work on platforms other than Linux",
)


@pytest.mark.parametrize(
    "repo_url",
    (
        f"https://{GITHUB}/{JAFFLE_SHOP}",
        f"http://{GITHUB}/{JAFFLE_SHOP}",
    ),
)
def test_download_dbt_project_from_http_public_github_repo(
    tmp_path, repo_url, assert_dir_contents
):
    """Test downloading dbt project from GitHub public fork of dbt-lab's jaffle-shop.

    In this test we use an HTTP/HTTPS connection to access GitHub. No credentials are
    required as the test repo is public.
    """
    remote = DbtGitRemoteHook()
    source = URL(repo_url)
    local_repo_path = remote.download_dbt_project(source, tmp_path)

    expected = [
        URL(local_repo_path / "dbt_project.yml"),
        URL(local_repo_path / "models" / "customers.sql"),
        URL(local_repo_path / "models" / "orders.sql"),
        URL(local_repo_path / "seeds" / "raw_customers.csv"),
        URL(local_repo_path / "seeds" / "raw_orders.csv"),
    ]

    assert local_repo_path.exists()

    assert_dir_contents(local_repo_path, expected, exact=False)


@pytest.mark.xfail(
    strict=False,
    reason="Attempting to clone from GitHub may fail for missing keys.",
)
@pytest.mark.parametrize(
    "repo_url",
    (
        f"ssh://{GITHUB}:{JAFFLE_SHOP}",
        f"git+ssh://{GITHUB}:{JAFFLE_SHOP}",
    ),
)
def test_download_dbt_project_from_ssh_public_github_repo(
    tmp_path, repo_url, assert_dir_contents
):
    """Test downloading dbt project from GitHub public fork of dbt-lab's jaffle-shop.

    In this test we use an SSH connection to access GitHub. Currently, this requires an
    SSH key to be setup in the host, so the tests are flaky by design. Future tests will
    rely on Airflow connections and test SSH keys instead.
    """
    remote = DbtGitRemoteHook()
    source = URL(repo_url)
    local_repo_path = remote.download_dbt_project(source, tmp_path)

    expected = [
        URL(local_repo_path / "dbt_project.yml"),
        URL(local_repo_path / "models" / "customers.sql"),
        URL(local_repo_path / "models" / "orders.sql"),
        URL(local_repo_path / "seeds" / "raw_customers.csv"),
        URL(local_repo_path / "seeds" / "raw_orders.csv"),
    ]

    assert local_repo_path.exists()

    assert_dir_contents(local_repo_path, expected, exact=False)


@pytest.mark.parametrize(
    "repo_url",
    (
        f"https://{GITLAB}/{JAFFLE_SHOP}",
        f"http://{GITLAB}/{JAFFLE_SHOP}",
    ),
)
def test_download_dbt_project_from_http_public_gitlab_repo(
    tmp_path, repo_url, assert_dir_contents
):
    """Test downloading dbt project from GitLab public fork of dbt-lab's jaffle-shop.

    In this test we use an HTTP/HTTPS connection to access GitLab. No credentials are
    required as the test repo is public.
    """
    remote = DbtGitRemoteHook()
    source = URL(repo_url)
    local_repo_path = remote.download_dbt_project(source, tmp_path)

    expected = [
        URL(local_repo_path / "dbt_project.yml"),
        URL(local_repo_path / "models" / "customers.sql"),
        URL(local_repo_path / "models" / "orders.sql"),
        URL(local_repo_path / "seeds" / "raw_customers.csv"),
        URL(local_repo_path / "seeds" / "raw_orders.csv"),
    ]

    assert local_repo_path.exists()

    assert_dir_contents(local_repo_path, expected, exact=False)


@pytest.mark.skipif(
    not os.environ.get("GITHUB_READ_TOKEN"),
    reason="Missing Github read token in environment.",
)
@pytest.mark.parametrize(
    "repo_url",
    (f"https://{{username}}:{{token}}@{GITHUB}/{JAFFLE_SHOP_PRIVATE}",),
)
def test_download_dbt_project_from_https_private_github_repo_using_token(
    tmp_path, repo_url, assert_dir_contents
):
    """Test downloading dbt project from Github private fork of dbt-lab's jaffle-shop.

    In this test we use an HTTPS connection to access Github. As the repo is
    private, we need to authenticate. In this test, we are authenticating with a Github
    Personal Access Token. Said token will be fetched from the GITHUB_READ_TOKEN env
    variable. If missing, this test is skipped.

    The user the token represents should have access to the test Github repo. We
    have no way to check this though. Modify the JAFFLE_SHOP_PRIVATE variable with your
    own private fork.
    """
    username, token = os.environ["GITHUB_USERNAME"], os.environ["GITHUB_READ_TOKEN"]

    remote = DbtGitRemoteHook()
    source = URL(repo_url.format(username=username, token=token))
    local_repo_path = remote.download_dbt_project(source, tmp_path)

    expected = [
        URL(local_repo_path / "dbt_project.yml"),
        URL(local_repo_path / "models" / "customers.sql"),
        URL(local_repo_path / "models" / "orders.sql"),
        URL(local_repo_path / "seeds" / "raw_customers.csv"),
        URL(local_repo_path / "seeds" / "raw_orders.csv"),
    ]

    assert local_repo_path.exists()

    assert_dir_contents(local_repo_path, expected, exact=False)


@pytest.mark.xfail(
    strict=False,
    reason="Attempting to clone from GitLab may fail for missing keys.",
)
@pytest.mark.parametrize(
    "repo_url",
    (
        f"ssh://{GITLAB}:{JAFFLE_SHOP}",
        f"git+ssh://{GITLAB}:{JAFFLE_SHOP}",
    ),
)
def test_download_dbt_project_from_ssh_public_gitlab_repo(
    tmp_path, repo_url, assert_dir_contents
):
    """Test downloading dbt project from GitLab public fork of dbt-lab's jaffle-shop.

    In this test we use an SSH connection to access GitLab. Currently, this requires an
    SSH key to be setup in the host, so the tests are flaky by design. Future tests will
    rely on Airflow connections and test SSH keys instead.
    """
    remote = DbtGitRemoteHook()
    source = URL(repo_url)
    local_repo_path = remote.download_dbt_project(source, tmp_path)

    expected = [
        URL(local_repo_path / "dbt_project.yml"),
        URL(local_repo_path / "models" / "customers.sql"),
        URL(local_repo_path / "models" / "orders.sql"),
        URL(local_repo_path / "seeds" / "raw_customers.csv"),
        URL(local_repo_path / "seeds" / "raw_orders.csv"),
    ]

    assert local_repo_path.exists()

    assert_dir_contents(local_repo_path, expected, exact=False)


@pytest.mark.skipif(
    not os.environ.get("GITLAB_READ_TOKEN"),
    reason="Missing GitLab read token in environment.",
)
@pytest.mark.parametrize(
    "repo_url",
    (f"https://oauth2:{{token}}@{GITLAB}/{JAFFLE_SHOP_PRIVATE}",),
)
def test_download_dbt_project_from_https_private_gitlab_repo_using_token(
    tmp_path, repo_url, assert_dir_contents
):
    """Test downloading dbt project from GitLab private fork of dbt-lab's jaffle-shop.

    In this test we use an HTTPS connection to access GitLab. As the repo is
    private, we need to authenticate. In this test, we are authenticating with a GitLab
    Personal Access Token. Said token will be fetched from the GITLAB_READ_TOKEN env
    variable. If missing, this test is skipped.

    The user the token represents should have access to the test GitLab repo. We
    have no way to check this though. Modify the JAFFLE_SHOP_PRIVATE variable with your
    own private fork.
    """
    token = os.environ["GITLAB_READ_TOKEN"]

    remote = DbtGitRemoteHook()
    source = URL(repo_url.format(token=token))
    local_repo_path = remote.download_dbt_project(source, tmp_path)

    expected = [
        URL(local_repo_path / "dbt_project.yml"),
        URL(local_repo_path / "models" / "customers.sql"),
        URL(local_repo_path / "models" / "orders.sql"),
        URL(local_repo_path / "seeds" / "raw_customers.csv"),
        URL(local_repo_path / "seeds" / "raw_orders.csv"),
    ]

    assert local_repo_path.exists()

    assert_dir_contents(local_repo_path, expected, exact=False)


@pytest.mark.skipif(
    any(
        env_var not in os.environ for env_var in ("GITLAB_USERNAME", "GITLAB_PASSWORD")
    ),
    reason="Missing GitLab credentials in environment.",
)
@pytest.mark.parametrize(
    "repo_url",
    (f"https://{{username}}:{{password}}@{GITLAB}/{JAFFLE_SHOP_PRIVATE}",),
)
def test_download_dbt_project_from_https_private_gitlab_repo_using_credentials(
    tmp_path, repo_url, assert_dir_contents
):
    """Test downloading dbt project from GitLab private fork of dbt-lab's jaffle-shop.

    In this test we use an HTTPS connection to access GitLab. As the repo is
    private, we need to authenticate. In this test, we are authenticating with GitLab
    credentials (username and password). Said credentials will be fetched from the
    GITLAB_USERNAME and GITLAB_PASSWORD env variables. If missing, this test is skipped.

    The user the credentials represent should have access to the test GitLab repo. We
    have no way to check this though. Modify the JAFFLE_SHOP_PRIVATE variable with your
    own private fork.
    """
    username, password = os.environ["GITLAB_USERNAME"], os.environ["GITLAB_PASSWORD"]

    remote = DbtGitRemoteHook()
    source = URL(repo_url.format(username=username, password=password))
    local_repo_path = remote.download_dbt_project(source, tmp_path)

    expected = [
        URL(local_repo_path / "dbt_project.yml"),
        URL(local_repo_path / "models" / "customers.sql"),
        URL(local_repo_path / "models" / "orders.sql"),
        URL(local_repo_path / "seeds" / "raw_customers.csv"),
        URL(local_repo_path / "seeds" / "raw_orders.csv"),
    ]

    assert local_repo_path.exists()

    assert_dir_contents(local_repo_path, expected, exact=False)


@pytest.fixture
def repo_name():
    """A testing local git repo name."""
    return "test/test_shop"


@pytest.fixture
def repo_dir(tmp_path):
    """A testing local git repo directory."""
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    return repo_dir


@pytest.fixture
def repo(repo_dir, dbt_project_file, test_files, profiles_file):
    """Initialize a git repo with some dbt test files."""
    repo = Repo.init(repo_dir)
    shutil.copyfile(dbt_project_file, repo_dir / "dbt_project.yml")
    repo.stage("dbt_project.yml")

    shutil.copyfile(profiles_file, repo_dir / "profiles.yml")
    repo.stage("profiles.yml")

    for test_file in test_files:
        remote_subdir = repo_dir / test_file.parent.name
        remote_subdir.mkdir(exist_ok=True)
        shutil.copyfile(test_file, remote_subdir / test_file.name)

        repo.stage(f"{test_file.parent.name}/{test_file.name}")

    repo.do_commit(b"Test first commit", committer=b"Test user <test@user.com>")

    yield repo

    repo.close()


@pytest.fixture
def git_server(repo, repo_name):
    """A testing local TCP git server."""
    backend = DictBackend({repo_name.encode(): repo})
    dul_server = TCPGitServer(backend, b"localhost", 0)

    proc = multiprocessing.Process(target=dul_server.serve)
    proc.start()

    server_address, server_port = dul_server.socket.getsockname()

    yield server_address, server_port

    proc.terminate()


@no_git_local_server
def test_download_dbt_project_with_local_server(
    tmp_path, git_server, repo_name, assert_dir_contents
):
    """Test downloading a dbt project from a local git server."""
    local_path = tmp_path / "local"
    remote = DbtGitRemoteHook()
    server_address, server_port = git_server
    source = URL(f"git://{server_address}:{server_port}/{repo_name}")
    local_repo_path = remote.download_dbt_project(source, local_path)

    expected = [
        URL(local_repo_path / "dbt_project.yml"),
        URL(local_repo_path / "models" / "a_model.sql"),
        URL(local_repo_path / "models" / "another_model.sql"),
        URL(local_repo_path / "seeds" / "a_seed.csv"),
    ]

    assert local_repo_path.exists()
    assert_dir_contents(local_repo_path, expected, exact=False)


@pytest.fixture
def pre_run(hook, repo_dir):
    """Fixture to run a dbt run task."""
    import shutil

    hook.run_dbt_task(
        "run",
        project_dir=repo_dir,
        profiles_dir=repo_dir,
        upload_dbt_project=True,
    )

    yield

    target_dir = repo_dir / "target"
    shutil.rmtree(target_dir, ignore_errors=True)


@no_git_local_server
def test_upload_dbt_project_with_local_server(
    git_server, repo_dir, assert_dir_contents, pre_run, tmp_path, repo_name
):
    """Test uploading a dbt project to a local git server."""

    def upload_only_target(u: URL):
        if "target" in u.path.parts:
            return True
        return False

    remote = DbtGitRemoteHook(upload_filter=upload_only_target)
    server_address, server_port = git_server
    destination = URL(f"git://{server_address}:{server_port}/{repo_name}")

    remote.upload_dbt_project(str(repo_dir), destination)

    new_repo_path = tmp_path / "new_repo"
    remote.download_dbt_project(destination, new_repo_path)

    expected = [
        URL(new_repo_path / "dbt_project.yml"),
        URL(new_repo_path / "models" / "a_model.sql"),
        URL(new_repo_path / "models" / "another_model.sql"),
        URL(new_repo_path / "seeds" / "a_seed.csv"),
        URL(new_repo_path / "target" / "run_results.json"),
        URL(new_repo_path / "target" / "manifest.json"),
    ]

    assert new_repo_path.exists()
    assert_dir_contents(new_repo_path, expected, exact=False)
