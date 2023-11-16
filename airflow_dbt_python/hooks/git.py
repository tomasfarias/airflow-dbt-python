"""A concrete DbtRemoteHook for git repositories with dulwich."""
import datetime as dt
from typing import Callable, Optional, Tuple, Union

from airflow.providers.ssh.hooks.ssh import SSHHook
from dulwich.client import HttpGitClient, SSHGitClient, TCPGitClient
from dulwich.contrib.paramiko_vendor import ParamikoSSHVendor
from dulwich.objectspec import parse_reftuples
from dulwich.porcelain import Error, active_branch, check_diverged
from dulwich.protocol import ZERO_SHA
from dulwich.repo import Repo

from airflow_dbt_python.hooks.remote import DbtRemoteHook
from airflow_dbt_python.utils.url import URL

GitClients = Union[HttpGitClient, SSHGitClient, TCPGitClient]


def no_filter(_: URL) -> bool:
    """A no-op filter."""
    return True


class DbtGitRemoteHook(SSHHook, DbtRemoteHook):
    """A dbt remote implementation for git repositories.

    This concrete remote class implements the DbtRemote interface by using any git
    repository to upload and download dbt files to and from.

    The DbtGitRemoteHook subclasses Airflow's SSHHook to interact with to utilize its
    defined methods to operate with SSH connections. However, SSH connections are not
    the only ones supported for interacting with git repositories: HTTP (http:// or
    https://) and plain TCP (git://) may be used.
    """

    conn_name_attr = "git_conn_id"
    default_conn_name = "git_default"
    conn_type = "git"
    hook_name = "dbt git Remote"

    def __init__(
        self,
        git_conn_id: Optional[str] = None,
        commit_author: str = "Airflow dbt <>",
        commit_msg: str = "Airflow dbt committed on {ts: Y%-%m-%d H%:%M:%S}",
        username: str = "git",
        upload_branch: Optional[str] = None,
        upload_filter: Callable[[URL], bool] = no_filter,
        remote_host: str = "localhost",
        **kwargs,
    ):
        """Initialize a dbt remote for git via SSH or HTTP."""
        self.git_conn_id = git_conn_id
        self.commit_author = commit_author
        self.commit_msg = commit_msg
        self.upload_branch = upload_branch
        self.upload_filter = upload_filter
        super().__init__(
            ssh_conn_id=git_conn_id,
            username=username,
            remote_host=remote_host,
            **kwargs,
        )

    def upload(
        self,
        source: URL,
        destination: URL,
        replace: bool = False,
        delete_before: bool = False,
    ) -> None:
        """Upload source git repository to a remote git repository.

        Args:
            source: A local git repository URL.
            destination: A destination URL where to upload to.
            replace: Not used.
            delete_before: Not used.
        """
        if destination.is_archive():
            raise ValueError(
                f"Cannot upload archive to remote git repository: {source}"
            )

        repo = Repo(str(source))

        for f in source:
            if self.upload_filter(f) is False:
                continue

            repo.stage(str(f.relative_to(source)))

        ts = dt.datetime.utcnow()
        repo.do_commit(
            self.commit_msg.format(ts=ts).encode(), self.commit_author.encode()
        )

        selected_refs = []
        remote_changed_refs = {}

        refspecs = [active_branch(repo)]

        def update_refs(refs):
            selected_refs.extend(
                parse_reftuples(repo.refs, refs, refspecs, force=False)
            )
            new_refs = {}

            for lh, rh, force_ref in selected_refs:
                if lh is None:
                    new_refs[rh] = ZERO_SHA
                    remote_changed_refs[rh] = None
                else:
                    try:
                        localsha = repo.refs[lh]
                    except KeyError as exc:
                        raise Error("No valid ref %s in local repository" % lh) from exc
                    if not force_ref and rh in refs:
                        check_diverged(repo, refs[rh], localsha)
                        new_refs[rh] = localsha
                        remote_changed_refs[rh] = localsha
            return new_refs

        client, path = self.get_git_client_path(destination)

        client.send_pack(
            path,
            update_refs,
            generate_pack_data=repo.generate_pack_data,
        )

    def download(
        self,
        source: URL,
        destination: URL,
        replace: bool = False,
        delete_before: bool = False,
    ):
        """Download a remote git repository.

        Args:
            source: A git remote URL.
            destination: A destination URL where to download the objects to.
            replace: Not used.
            delete_before: Not used.
        """
        if destination.is_archive():
            # Perhaps this should be implemented to download release artifacts.
            # For the time being, it's not supported.
            raise ValueError(
                f"Cannot download archive from remote git repository: {source}"
            )

        client, path = self.get_git_client_path(source)

        client.clone(path, str(destination), mkdir=not destination.exists())

    def get_git_client_path(self, url: URL) -> Tuple[GitClients, str]:
        """Initialize a dulwich git client according to given URL's scheme."""
        if url.scheme == "git":
            client: GitClients = TCPGitClient(url.hostname, url.port)
            path = str(url.path)

        elif url.scheme in ("git+ssh", "ssh"):
            vendor_kwargs = dict(
                timeout=self.conn_timeout,
                compress=self.compress,
                sock=self.host_proxy,
                look_for_keys=self.look_for_keys,
                banner_timeout=self.banner_timeout,
            )

            if self.pkey:
                vendor_kwargs["pkey"] = self.pkey

            if self.key_file:
                vendor_kwargs["key_filename"] = self.key_file

            vendor = ParamikoSSHVendor(**vendor_kwargs)

            client = SSHGitClient(
                host=url.hostname,
                port=self.port,
                username=url.username or self.username,
                vendor=vendor,
            )
            path = f"{url.netloc.split(':')[1]}/{str(url.path)}"

        elif url.scheme in ("http", "https"):
            base_url = url.hostname

            if url.port:
                base_url = f"{base_url}:{url.port}"

            auth_params = {}
            if url.authentication.username and url.authentication.password:
                auth_params = {
                    "username": url.authentication.username,
                    "password": url.authentication.password,
                }
                base_url = f"{url.scheme}://{base_url}"
            elif url.authentication.username:
                base_url = f"{url.scheme}://{url.authentication.username}@{base_url}"

            client = HttpGitClient(base_url, **auth_params)

            path = str(url.path)

        else:
            raise ValueError(f"Unsupported scheme: {url.scheme}")

        return client, path
