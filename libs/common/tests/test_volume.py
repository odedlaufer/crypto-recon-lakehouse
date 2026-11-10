import threading
from pathlib import Path

import pytest

from crypto_recon_common.volume import VolumeUploader, upload_continuously

VOLUME = "/Volumes/crypto_dev/raw/landing"


class FakeFiles:
    def __init__(self, fail_for: set[str] | None = None) -> None:
        self.fail_for = fail_for or set()
        self.uploads: dict[str, str] = {}

    def upload_from(
        self,
        file_path: str,
        source_path: str,
        *,
        overwrite: bool | None = None,
        use_parallel: bool = True,
    ) -> None:
        if file_path in self.fail_for:
            raise ConnectionError("network down")
        assert overwrite is True
        self.uploads[file_path] = Path(source_path).read_text(encoding="utf-8")


def _make(root: Path, relative: str, text: str = "line\n") -> Path:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def test_uploads_to_mirrored_path_and_deletes_local(tmp_path: Path) -> None:
    local = _make(tmp_path, "ws/matches/dt=2026-11-10/hr=12/a.jsonl", "x\n")
    files = FakeFiles()
    result = VolumeUploader(files, tmp_path, VOLUME).sweep()

    remote = f"{VOLUME}/ws/matches/dt=2026-11-10/hr=12/a.jsonl"
    assert result.uploaded == [remote]
    assert files.uploads == {remote: "x\n"}
    assert not local.exists()


def test_ignores_files_still_being_written(tmp_path: Path) -> None:
    _make(tmp_path, "ws/matches/a.jsonl.tmp")
    uploader = VolumeUploader(FakeFiles(), tmp_path, VOLUME)
    assert uploader.pending() == []
    assert uploader.sweep().uploaded == []


def test_failed_upload_is_kept_and_retried(tmp_path: Path) -> None:
    local = _make(tmp_path, "ws/heartbeat/b.jsonl")
    remote = f"{VOLUME}/ws/heartbeat/b.jsonl"
    files = FakeFiles(fail_for={remote})
    uploader = VolumeUploader(files, tmp_path, VOLUME)

    first = uploader.sweep()
    assert first.failed == [local]
    assert local.exists()

    files.fail_for.clear()
    assert uploader.sweep().uploaded == [remote]
    assert not local.exists()


def test_one_failure_does_not_block_other_files(tmp_path: Path) -> None:
    _make(tmp_path, "ws/a.jsonl")
    _make(tmp_path, "ws/b.jsonl")
    files = FakeFiles(fail_for={f"{VOLUME}/ws/a.jsonl"})
    result = VolumeUploader(files, tmp_path, VOLUME).sweep()
    assert result.uploaded == [f"{VOLUME}/ws/b.jsonl"]
    assert len(result.failed) == 1


def test_missing_local_root_is_empty(tmp_path: Path) -> None:
    assert VolumeUploader(FakeFiles(), tmp_path / "nope", VOLUME).sweep().uploaded == []


def test_rejects_non_volume_path(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        VolumeUploader(FakeFiles(), tmp_path, "/data/landing")


def test_upload_continuously_does_final_sweep_on_stop(tmp_path: Path) -> None:
    _make(tmp_path, "ws/a.jsonl")
    files = FakeFiles()
    stop = threading.Event()
    stop.set()
    upload_continuously(VolumeUploader(files, tmp_path, VOLUME), stop, interval_seconds=60)
    assert list(files.uploads) == [f"{VOLUME}/ws/a.jsonl"]
