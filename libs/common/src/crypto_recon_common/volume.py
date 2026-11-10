"""Upload finished landing files to a Unity Catalog Volume.

Each local file under `local_root` is uploaded to the same relative path under
`volume_root` and deleted locally once the upload succeeds. A file that fails stays on
disk and is retried on the next sweep, so a short network or Databricks outage loses
nothing. Only finished files (*.jsonl) are picked up; files being written end in .tmp.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Protocol

logger = logging.getLogger(__name__)

VOLUMES_PREFIX = "/Volumes/"


class FilesClient(Protocol):
    """The part of the Databricks SDK Files API the uploader uses."""

    def upload_from(
        self,
        file_path: str,
        source_path: str,
        *,
        overwrite: bool | None = None,
        use_parallel: bool = True,
    ) -> object: ...


def connect_files_client(profile: str | None = None) -> FilesClient:
    """Authenticate to Databricks and return its Files API.

    Uses the named CLI profile, or the standard environment variables
    (DATABRICKS_HOST, DATABRICKS_TOKEN) when no profile is given. Fails fast on bad auth.
    """
    from databricks.sdk import WorkspaceClient

    client = WorkspaceClient(profile=profile)
    user = client.current_user.me()
    logger.info("authenticated to %s as %s", client.config.host, user.user_name)
    return client.files


@dataclass
class SweepResult:
    uploaded: list[str] = field(default_factory=list)
    failed: list[Path] = field(default_factory=list)


class VolumeUploader:
    """Mirrors finished local files into a Volume path, then deletes them locally."""

    def __init__(
        self,
        files: FilesClient,
        local_root: Path,
        volume_root: str,
        *,
        pattern: str = "*.jsonl",
    ) -> None:
        if not volume_root.startswith(VOLUMES_PREFIX):
            raise ValueError(f"volume_root must start with {VOLUMES_PREFIX!r}: {volume_root!r}")
        self._files = files
        self._local_root = local_root
        self._volume_root = PurePosixPath(volume_root)
        self._pattern = pattern

    def pending(self) -> list[Path]:
        """Finished local files waiting to be uploaded, oldest path first."""
        if not self._local_root.exists():
            return []
        return sorted(p for p in self._local_root.rglob(self._pattern) if p.is_file())

    def remote_path(self, local_path: Path) -> str:
        relative = local_path.relative_to(self._local_root)
        return str(self._volume_root.joinpath(*relative.parts))

    def sweep(self) -> SweepResult:
        """Upload every pending file once. Failures are logged and kept for the next sweep."""
        result = SweepResult()
        for path in self.pending():
            remote = self.remote_path(path)
            try:
                # File names are unique, so overwrite only matters when an earlier upload
                # succeeded but the local delete did not; the content is identical.
                self._files.upload_from(remote, str(path), overwrite=True, use_parallel=False)
            except Exception:
                logger.exception("upload failed, will retry on next sweep: %s", path)
                result.failed.append(path)
                continue
            path.unlink()
            result.uploaded.append(remote)
        if result.uploaded or result.failed:
            logger.info(
                "upload sweep: %d uploaded, %d failed", len(result.uploaded), len(result.failed)
            )
        return result


def upload_continuously(
    uploader: VolumeUploader, stop: threading.Event, interval_seconds: float = 5.0
) -> None:
    """Sweep every `interval_seconds` until `stop` is set, then do one final sweep."""
    while not stop.wait(interval_seconds):
        uploader.sweep()
    uploader.sweep()
