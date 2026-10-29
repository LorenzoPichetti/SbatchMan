"""Export campaign SbatchMan projects as a portable tarball."""

import json
import os
import re
import shutil
import tarfile
import tempfile
from pathlib import Path
from typing import Optional


def _is_within(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def export_campaign_project(
    log_file: Path,
    output_path: Optional[Path] = None,
) -> Path:
    """Bundle a campaign journal and all of its application projects.

    The archive layout is ``campaign_<id>/campaign.json`` and
    ``campaign_<id>/applications/<app>/SbatchMan/``.
    """
    log_file = Path(log_file).expanduser().resolve()
    if not log_file.is_file():
        raise FileNotFoundError(f"Campaign journal not found: {log_file}")

    with log_file.open("r", encoding="utf-8") as stream:
        campaign_data = json.load(stream)
    if not isinstance(campaign_data, dict):
        raise ValueError("Campaign journal must contain a JSON object")

    campaign_id = str(campaign_data.get("campaign_id") or log_file.stem)
    safe_campaign_id = re.sub(r"[^A-Za-z0-9._-]+", "_", campaign_id).strip("._") or "export"
    archive_root = f"campaign_{safe_campaign_id}"

    projects = campaign_data.get("application_projects") or {}
    if not isinstance(projects, dict) or not projects:
        raise ValueError("Campaign journal does not contain any application project paths")

    resolved_projects = []
    for app_name, project_value in projects.items():
        if (
            not isinstance(app_name, str)
            or Path(app_name).name != app_name
            or "/" in app_name
            or "\\" in app_name
            or app_name in {".", ".."}
        ):
            raise ValueError(f"Unsafe application name in campaign journal: {app_name!r}")
        if not isinstance(project_value, str) or not project_value:
            raise ValueError(f"Missing SbatchMan project path for application {app_name!r}")
        project_root = Path(project_value).expanduser().resolve()
        if not project_root.is_dir():
            raise FileNotFoundError(
                f"SbatchMan project for application {app_name!r} not found: {project_root}"
            )
        resolved_projects.append((app_name, project_root))

    if output_path is None:
        output_path = log_file.with_name(f"{archive_root}.tar.gz")
    else:
        output_path = Path(output_path).expanduser().resolve()
    if output_path == log_file:
        raise ValueError("Archive output path must differ from the campaign journal")
    if output_path.exists() and output_path.is_dir():
        raise ValueError(f"Archive output path is a directory: {output_path}")
    if any(_is_within(output_path, project_root) for _, project_root in resolved_projects):
        raise ValueError("Archive output path must be outside the application SbatchMan projects")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    temporary_path = None
    try:
        with tempfile.NamedTemporaryFile(
            prefix="sbatchman-campaign-export-", suffix=".tar.gz", delete=False
        ) as temporary_file:
            temporary_path = Path(temporary_file.name)

        with tarfile.open(temporary_path, mode="w:gz") as archive:
            archive.add(log_file, arcname=f"{archive_root}/campaign.json")
            for app_name, project_root in resolved_projects:
                archive.add(
                    project_root,
                    arcname=f"{archive_root}/applications/{app_name}/SbatchMan",
                    recursive=True,
                )

        shutil.move(str(temporary_path), str(output_path))
        temporary_path = None
        return output_path
    finally:
        if temporary_path is not None:
            try:
                os.unlink(temporary_path)
            except FileNotFoundError:
                pass
