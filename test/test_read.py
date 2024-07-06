import gzip
import io
import subprocess
from unittest.mock import MagicMock, patch

import gs_fastcopy

JSON_STR = b'{"A": 3}'

builtin_run = subprocess.run


# This intercepts the gcloud storage cp invocation,
# and writes a file to the given filename.
def subprocess_run_mock(*args, **kwargs):
    commands = args[0]
    if commands[0:3] == ["gcloud", "storage", "cp"]:
        with open(commands[4], "wb") as f:
            contents = (
                gzip.compress(JSON_STR) if commands[4].endswith(".gz") else JSON_STR
            )
            f.write(contents)
        return ""
    else:
        builtin_run(*args, **kwargs)


@patch.object(gs_fastcopy.subprocess, "run", new_callable=lambda: subprocess_run_mock)
def test_read_no_compression(mock_run):
    with gs_fastcopy.read("gs://my-bucket/my-file.json") as f:
        result = f.read()

    assert result == JSON_STR


@patch.object(gs_fastcopy.subprocess, "run", new_callable=lambda: subprocess_run_mock)
def test_read_with_compression(mock_run):
    with gs_fastcopy.read("gs://my-bucket/my-file.json.gz") as f:
        result = f.read()

    assert result == JSON_STR
