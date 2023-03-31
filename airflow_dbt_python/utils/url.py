"""A URL class to locate dbt resources."""
from __future__ import annotations

import os
import tarfile
from enum import Enum
from pathlib import Path
from tarfile import TarFile
from typing import Generator, NamedTuple, Optional, Union
from urllib.parse import ParseResult, urljoin, urlparse, urlunparse
from zipfile import ZipFile

URLLike = Union["URL", str, Path]


class HTTPAuthentication(NamedTuple):
    """HTTP Basic Authentication parameters."""

    username: Optional[str]
    password: Optional[str]

    def __repr__(self) -> str:
        """Return a representation of this HTTPAuthentication.

        We hide username and password since both may contain sensitive values.
        """
        return "HTTPAuthentication(username='***', password='***')"


class SupportedArchives(str, Enum):
    """Enumeration of supported archive formats."""

    ZIP = "zip"
    TAR = "tar"


class URL:
    """A URL indicating where find one or more dbt files.

    This class applies some of the functionality of pathlib.Path on the path component
    of a URL, ensuring it stays valid. We require this as different dbt remots work with
    different representations of where a resource is located, and we need to harmonize
    things between them and the ultimate destination of the files.

    For example, S3 backend relies on URLs with the 's3' scheme. But any keys are
    downloaded to a regular local path, which is going to depend on the OS. This class
    helps us determine the destination local path ensuring the URL remains a valid S3
    URL.

    We utilize urlparse as it supports multiple schemes, including no scheme at all.

    Attributes:
        _parsed: Contains the parsed string as returned by addresslib.parse.urlparse.
    """

    def __init__(self, u: URLLike):
        """Initialize a URL from any URLLike.

        >>> URL("/local/path/to/project.zip")._parsed
        ... # doctest: +NORMALIZE_WHITESPACE
        ParseResult(scheme='', netloc='', path='/local/path/to/project.zip', params='',
        query='', fragment='')
        >>> URL("s3://s3-bucket/path/to/project.zip")._parsed
        ... # doctest: +NORMALIZE_WHITESPACE
        ParseResult(scheme='s3', netloc='s3-bucket', path='/path/to/project.zip',
        params='', query='', fragment='')
        >>> URL(Path("/local/path/to/project.zip"))._parsed
        ... # doctest: +NORMALIZE_WHITESPACE
        ParseResult(scheme='', netloc='', path='/local/path/to/project.zip', params='',
        query='', fragment='')
        >>> url_1 = URL("/local/path/to/project.zip")
        >>> url_2 = URL(url_1)
        >>> url_2._parsed
        ... # doctest: +NORMALIZE_WHITESPACE
        ParseResult(scheme='', netloc='', path='/local/path/to/project.zip', params='',
        query='', fragment='')
        >>> url_1 is not url_2
        True
        """
        if isinstance(u, URL):
            parsed = u._parsed
        else:
            parsed = urlparse(str(u))

        self.path = Path(parsed.path)

        if self.path.is_absolute() and parsed.netloc != "":
            self.path = self.path.relative_to("/")

        self._parsed: ParseResult = parsed

    @classmethod
    def from_parts(
        cls,
        scheme: str = "",
        netloc: str = "",
        path: str = "",
        params: str = "",
        query: str = "",
        fragment: str = "",
    ) -> URL:
        """Construct a new URL by unparsing the parts returned by urlparse."""
        return cls(urlunparse((scheme, netloc, path, params, query, fragment)))

    def relative_to(self, base: Union[str, "URL"]) -> "URL":
        """Return a new URL with a path relative to base.

        A URL's path does not include its netloc.

        >>> URL("/local/path/to/project.zip").relative_to("/local/path")
        URL("to/project.zip")
        >>> URL("s3://airflow-dbt-test-s3-bucket/project/data/").relative_to("project")
        URL("s3://airflow-dbt-test-s3-bucket/data")
        """
        if isinstance(base, URL):
            new_path = self.path.relative_to(base.path)
        else:
            new_path = self.path.relative_to(base)

        new_parsed = self._parsed._replace(path=str(new_path))

        return URL.from_parts(
            scheme=new_parsed.scheme,
            netloc=new_parsed.netloc,
            path=new_parsed.path,
            params=new_parsed.params,
            query=new_parsed.query,
            fragment=new_parsed.fragment,
        )

    def is_relative_to(self, base: Union[str, "URL"]) -> bool:
        """Check whether this URL is relative to base.

        >>> URL("/local/path/to/project.zip").is_relative_to("/local/path")
        True
        >>> URL("/local/path/to/project.zip").is_relative_to("/etc")
        False
        >>> URL("s3://test-s3-bucket/project/data/").is_relative_to("project")
        True
        >>> URL("s3://test-s3-bucket/project/data/").is_relative_to("data")
        False
        >>> URL("s3://test-s3-bucket/project/data/").is_relative_to("different")
        False
        """
        is_relative = True
        try:
            # is_relative_to was added in Python 3.9 and we have to support 3.7 and 3.8.
            if isinstance(base, URL):
                self.path.relative_to(base.path)
            else:
                self.path.relative_to(base)

        except ValueError:
            is_relative = False

        return is_relative

    def join(self, relative: str) -> "URL":
        """Return a new URL by joining this with relative.

        The resulting URL will not necessarilly be a concatenation self with relative.
        Use the / operator to concatenate URLs.

        >>> URL("/local/path/to/").join("project.zip")
        URL("/local/path/to/project.zip")
        """
        new_address = urljoin(self._parsed.geturl(), relative)
        return URL(new_address)

    def is_absolute(self) -> bool:
        """Check whether current local path is absolute.

        Remote paths will always be not absolute (False).
        """
        return self.path.is_absolute()

    def is_dir(self) -> bool:
        """Check whether the current local path points to a directory.

        Paths must exist and we cannot check whether remotes do, so
        this method only applies to local paths.
        """
        return self.path.is_dir()

    def is_zipfile(self) -> bool:
        """Check whether this URL points to a zip file.

        This is different from the method found in stdlib's zipfile as
        we support also checking for remote files by looking only at the
        suffix. Of course, this means there could be false positives.

        >>> URL("s3://s3-bucket/path/to/my/profiles.yml").is_zipfile()
        False
        >>> URL("s3://s3-bucket/path/to/my/project.zip").is_zipfile()
        True
        >>> URL("path/to/my/project.zip").is_zipfile()
        True
        >>> URL("/path/to/my/project.zip").is_zipfile()
        True
        >>> URL("project.zip").is_zipfile()
        True
        """
        return self.suffix == ".zip"

    def is_tarfile(self) -> bool:
        """Check whether this URL points to a zip file.

        This is different from the method found in stdlib's tarfile as
        we support also checking for remote files by looking only at the
        suffix. Of course, this means there could be false positives.

        >>> URL("s3://s3-bucket/path/to/my/profiles.yml").is_tarfile()
        False
        >>> URL("s3://s3-bucket/path/to/my/project.tar").is_tarfile()
        True
        >>> URL("s3://s3-bucket/path/to/my/project.tar.gz").is_tarfile()
        True
        """
        split = self.name.split(".")
        return "tar" in split and len(split) > 1

    def is_archive(self) -> bool:
        """Check whether URL points to a supported archive format.

        >>> URL("s3://s3-bucket/path/to/my/profiles.yml").is_archive()
        False
        >>> URL("s3://s3-bucket/path/to/my/project.zip").is_archive()
        True
        >>> URL("path/to/my/project.zip").is_archive()
        True
        >>> URL("/path/to/my/project.zip").is_archive()
        True
        >>> URL("project.zip").is_archive()
        True
        >>> URL("s3://s3-bucket/path/to/my/profiles.yml").is_archive()
        False
        >>> URL("s3://s3-bucket/path/to/my/project.tar").is_archive()
        True
        >>> URL("s3://s3-bucket/path/to/my/project.tar.gz").is_archive()
        True
        """
        return self.is_zipfile() or self.is_tarfile()

    def is_local(self) -> bool:
        """Check whether URL points to a local file."""
        return self.netloc == ""

    def exists(self) -> bool:
        """Check whether URL points to a local file that exists."""
        return self.path.exists()

    def extract(self) -> None:
        """Extract an archive represented by this URL."""
        if not self.is_archive():
            raise ValueError("Cannot extract non-archived file located with this URL.")

        if self.is_zipfile():
            extract_zip_url(self)
        elif self.is_tarfile():
            extract_tar_url(self)
        else:
            raise ValueError(f"Unsupported archive format: {self.suffix}")

    def archive(
        self,
        output: Union[URLLike, SupportedArchives] = SupportedArchives.ZIP,
    ) -> URL:
        """Archive the contents of this URL.

        Args:
            output: Output the archive to a different directory. By default (None)
                resulting archive will be saved with the stem "output".
        """
        try:
            if isinstance(output, SupportedArchives):
                extension = output.value
            else:
                extension = SupportedArchives[str(output).upper()].value
            archive_name = f"output.{extension}"

        except KeyError:
            archive_url = URL(output)

        else:
            if self.is_dir():
                archive_url = self / archive_name
            else:
                archive_url = self.parent / archive_name

        if archive_url.is_zipfile():
            zip_dir_from_url(self, archive_url)
        elif archive_url.is_tarfile():
            tar_dir_from_url(self, archive_url)
        else:
            raise ValueError(f"Unsupported archive format: {archive_url.suffix}")

        return archive_url

    def unlink(self, missing_ok: bool = False) -> None:
        """Delete a local file addressed by this URL."""
        if self.is_local() is False:
            raise ValueError("Cannot unlink remote file.")

        try:
            self.path.unlink()
        except FileNotFoundError:
            # In python 3.8, the missing_ok parameter was added to ignore these
            # exceptions. Once we drop Python 3.7 support, we can remove this block.
            if missing_ok:
                raise

    def mkdir(self, parents: bool = False, exist_ok: bool = False) -> None:
        """Call this URL's underlying Path's mkdir."""
        if self.is_local() is False:
            raise ValueError("Cannot mkdir for remote URL.")

        self.path.mkdir(parents=parents, exist_ok=exist_ok)

    @property
    def suffix(self) -> str:
        """Returns this URL path's suffix.

        >>> URL("s3://s3-bucket/path/to/my/profiles.yml").suffix
        '.yml'
        """
        return self.path.suffix

    @property
    def name(self) -> str:
        """Return this URL path's name.

        >>> URL("/local/path/to/project.zip").name
        'project.zip'
        >>> URL("s3://s3-bucket/path/to/profiles.yml").name
        'profiles.yml'
        """
        return self.path.name

    @property
    def parent(self) -> "URL":
        """Return this URL path's parent.

        >>> URL("/local/path/to/project.zip").parent
        URL("/local/path/to")
        >>> URL("s3://s3-bucket/path/to/profiles.yml").parent
        URL("path/to")
        """
        return URL(self.path.parent)

    @property
    def authentication(self) -> HTTPAuthentication:
        """Return this URL HTTP Authentication (if any).

        >>> URL("/local/path/to/project.zip").authentication
        HTTPAuthentication(username='***', password='***')
        >>> URL("https://user:pw@gitlab.com").authentication  # pragma: allowlist secret
        HTTPAuthentication(username='***', password='***')
        >>> URL("https://username@gitlab.com").authentication
        HTTPAuthentication(username='***', password='***')
        """
        return HTTPAuthentication(
            username=self._parsed.username, password=self._parsed.password
        )

    def __truediv__(self, other) -> "URL":
        """Allows concatenating a path to this URL's path.

        >>> URL("/local/path/to") / "project.zip"
        URL("/local/path/to/project.zip")
        >>> URL("s3://s3-bucket/path/to") / "my/profiles.yml"
        URL("s3://s3-bucket/path/to/my/profiles.yml")
        """
        new_path = self.path / other
        return URL(self._parsed._replace(path=str(new_path)).geturl())

    def __rtruediv__(self, other) -> "URL":
        """Allows concatenating this URL to a pathlib.Path."""
        new_path = other / self.path
        return URL(self._parsed._replace(path=str(new_path)).geturl())

    def __str__(self) -> str:
        """Return full URL as a string.

        >>> str(URL("s3://s3-bucket/path/to/my/profiles.yml"))
        's3://s3-bucket/path/to/my/profiles.yml'
        >>> str(URL("https://hey:secret@gh.com/hey/repo"))  # pragma: allowlist secret
        'https://***:***@gh.com/***/repo'
        """
        url_str = self._parsed.geturl()

        if self._parsed.password:
            url_str = url_str.replace(self._parsed.password, "***")

        if self._parsed.username:
            url_str = url_str.replace(self._parsed.username, "***")

        return url_str

    def __repr__(self) -> str:
        """Return a representation of this URL."""
        return f'URL("{self._parsed.geturl()}")'

    def __getattr__(self, name):
        """Try to find attributes in ParsedResult."""
        return getattr(self._parsed, name)

    def __eq__(self, other) -> bool:
        """Compare against another URL or a Path.

        >>> URL("s3://bucket/dbt_project.yml") == URL("s3://bucket/dbt_project.yml")
        True
        >>> URL("s3://bucket/dbt_project.yml") == Path("dbt_project.yml")
        True
        >>> URL("/dbt_project.yml") == Path("/dbt_project.yml")
        True
        """
        if isinstance(other, Path):
            return self.path == other
        elif isinstance(other, URL):
            return self._parsed == other._parsed
        return NotImplemented

    def __fspath__(self) -> str:
        """Required to implement the os.PathLike interface.

        This allows URLs to be passed to open().
        """
        return os.fspath(self.path)

    def __iter__(self) -> Generator[URL, None, None]:
        """Iterate over all paths under this URL.

        If our path doesn't point to a directory, we yield self.
        """
        if not self.is_dir():
            yield self
        else:
            yield from (URL(p) for p in self.path.glob("**/*"))


def extract_zip_url(url: URL):
    """Extract the contents of a zip URL."""
    with ZipFile(url, "r") as zf:
        zf.extractall(url.parent)


def extract_tar_url(url: URL):
    """Extract the contents of a tar URL."""
    with TarFile(url, "r") as tf:
        tf.extractall(url.parent)


def zip_dir_from_url(url: URL, zip_url: URL) -> None:
    """Add all paths to a zip file in tar_url."""
    with ZipFile(zip_url, "w") as zf:
        for _file in url:
            zf.write(_file, arcname=_file.relative_to(url))


def tar_dir_from_url(url: URL, tar_url: URL) -> None:
    """Add all paths to a tar file in tar_url."""
    try:
        compression: str = url.name.rsplit(".", maxsplit=2)[2]
    except IndexError:
        compression = ""

    with tarfile.open(tar_url, f"w:{compression}") as tf:
        for _file in url:
            tf.add(_file)


if __name__ == "__main__":
    import doctest

    import airflow_dbt_python.utils.url as url

    doctest.testmod(url)
