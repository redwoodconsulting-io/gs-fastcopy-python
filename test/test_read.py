import gzip
import os
import subprocess
import tempfile
from unittest.mock import ANY, patch

import gs_fastcopy

JSON_STR = b'{"A": 3}'

builtin_run = subprocess.run


# This intercepts the gcloud storage cp invocation,
# and writes a file to the given filename.
def subprocess_run_mock(*args, **kwargs):
    commands = args[0]
    if commands[0:3] == ["gcloud", "storage", "cp"]:
        # ugh I don't like this but I just need to skip the flag if it's there
        # this will need to get smarter if we add more flags
        filename = commands[6] if commands[3] == "--billing-project" else commands[4]
        with open(filename, "wb") as f:
            contents = gzip.compress(JSON_STR) if filename.endswith(".gz") else JSON_STR
            f.write(contents)
        return subprocess.CompletedProcess(args, 0, b"", None)
    else:
        return builtin_run(*args, **kwargs)


@patch.object(gs_fastcopy.subprocess, "run", new_callable=lambda: subprocess_run_mock)
def test_read_no_compression(_mock_run):
    with gs_fastcopy.read("gs://my-bucket/my-file.json") as f:
        result = f.read()

    assert result == JSON_STR


def test_read_local_no_compression():
    with tempfile.NamedTemporaryFile() as tmp_file:
        with open(tmp_file.name, "wb") as f:
            f.write(JSON_STR)

        with gs_fastcopy.read(tmp_file.name) as f:
            result = f.read()
            assert result == JSON_STR

        with gs_fastcopy.read(os.path.relpath(tmp_file.name)) as f:
            result = f.read()
            assert result == JSON_STR


@patch.object(gs_fastcopy.subprocess, "run", new_callable=lambda: subprocess_run_mock)
def test_read_with_compression(_mock_run):
    with gs_fastcopy.read("gs://my-bucket/my-file.json.gz") as f:
        result = f.read()

    assert result == JSON_STR


def test_read_local_with_compression():
    with tempfile.NamedTemporaryFile(suffix=".gz") as tmp_file:
        with gzip.open(tmp_file.name, "wb") as fgz:
            fgz.write(JSON_STR)

        with gs_fastcopy.read(tmp_file.name) as f:
            result = f.read()
            assert result == JSON_STR

        with gs_fastcopy.read(os.path.relpath(tmp_file.name)) as f:
            result = f.read()
            assert result == JSON_STR


@patch.object(gs_fastcopy.subprocess, "run")
def test_read_billing_project(mock_run):
    mock_run.side_effect = subprocess_run_mock

    with gs_fastcopy.read(
        "gs://my-bucket/my-file.json.gz", billing_project="project123"
    ) as f:
        _ = f.read()

    mock_run.assert_any_call(
        [
            "gcloud",
            "storage",
            "cp",
            "--billing-project",
            "project123",
            "gs://my-bucket/my-file.json.gz",
            ANY,
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
