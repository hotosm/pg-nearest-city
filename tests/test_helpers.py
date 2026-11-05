"""Tests for pg_nearest_city.scripts.helpers."""

from pathlib import Path

from pg_nearest_city.scripts.helpers import LinkError, make_file_link


class TestMakeFileLink:
    def test_hardlink_success(self, tmp_path):
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_text("hello")
        errors = make_file_link(src, dst, is_hardlink=True)
        assert dst.exists()
        assert dst.read_text() == "hello"
        # No copy errors (hardlink or symlink succeeded)
        assert not any(e.link_type == "copy" for e in errors)

    def test_overwrite_existing(self, tmp_path):
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_text("new")
        dst.write_text("old")
        make_file_link(src, dst, overwrite_existing=True)
        assert dst.read_text() == "new"

    def test_symlink_fallback(self, tmp_path):
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_text("data")
        # Request symlink first
        make_file_link(src, dst, is_hardlink=False)
        assert dst.exists()

    def test_link_error_namedtuple(self):
        err = LinkError(link_type="copy", link_err=OSError("fail"))
        assert err.link_type == "copy"
        assert isinstance(err.link_err, OSError)


def test_cross_device_hardlink_falls_back_to_symlink(tmp_path, monkeypatch):
    src = tmp_path / "src.txt"
    src.write_text("hello")
    dst = tmp_path / "dst.txt"

    def cross_device(self, target):
        raise OSError(18, "Invalid cross-device link")

    monkeypatch.setattr(Path, "hardlink_to", cross_device)
    errors = make_file_link(src, dst, is_hardlink=True)

    assert [e.link_type for e in errors] == ["hardlink"]
    assert dst.read_text() == "hello"
