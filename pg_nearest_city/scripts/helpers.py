"""Helper functions for dataset file management."""

import pathlib
import shutil
from typing import Literal, NamedTuple

try:
    # changes in Python 3.13
    _LinkError = pathlib.UnsupportedOperation  # type: ignore[attr-defined]
except AttributeError:
    _LinkError = NotImplementedError


class LinkError(NamedTuple):
    """Records a failed link/copy attempt and the exception raised."""

    link_type: Literal["copy", "hardlink", "symlink"]
    link_err: BaseException


def make_file_link(
    src: pathlib.Path,
    dst: pathlib.Path,
    is_hardlink: bool = True,
    overwrite_existing: bool = True,
) -> list:
    """Attempts to hardlink or symlink a file, falling back to copying if needed."""
    errors: list[LinkError] = []

    if overwrite_existing:
        dst.unlink(missing_ok=True)
    for use_hardlink in (is_hardlink, not is_hardlink):
        try:
            if use_hardlink:
                dst.hardlink_to(src)
            else:
                dst.symlink_to(src)
            return errors
        except _LinkError as e:
            errors.append(
                LinkError(
                    link_err=e, link_type="hardlink" if use_hardlink else "symlink"
                )
            )

    try:
        shutil.copy2(src=src, dst=dst)
    except OSError as e:
        errors.append(LinkError(link_err=e, link_type="copy"))
    return errors
