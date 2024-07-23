import gzip
import subprocess
from unittest.mock import ANY, MagicMock, patch

import gs_fastcopy

JSON_STR = b'{"A": 3}'

builtin_run = subprocess.run


# This intercepts the upload API call to gcloud storage,
# and writes what would've been written into a one-array
# list (referring to state in the unit test caller).
def build_upload_chunks_concurrently_mock(result):
    def side_effecter(buffer_file_name, gs_blob, **kwargs):
        nonlocal result

        with open(buffer_file_name, "rb") as f:
            result[0] = f.read()

        if buffer_file_name.endswith(".gz"):
            result[0] = gzip.decompress(result[0])

    return side_effecter


@patch.object(
    gs_fastcopy.transfer_manager,
    "upload_chunks_concurrently",
)
def test_write_no_compression(mock_run):
    result = [None]

    # Set up the mock to intercept the write to gcloud storage.
    mock_run.side_effect = build_upload_chunks_concurrently_mock(result)

    with gs_fastcopy.write("gs://my-bucket/my-file.json") as f:
        f.write(JSON_STR)

    assert result[0] == JSON_STR


@patch.object(
    gs_fastcopy.transfer_manager,
    "upload_chunks_concurrently",
)
def test_write_with_compression(mock_upload):
    result = [None]

    # Set up the mock to intercept the write to gcloud storage.
    mock_upload.side_effect = build_upload_chunks_concurrently_mock(result)

    with gs_fastcopy.write("gs://my-bucket/my-file.json.gz") as f:
        f.write(JSON_STR)

    assert result[0] == JSON_STR


@patch.object(
    gs_fastcopy,
    "_get_available_cpus",
)
@patch.object(
    gs_fastcopy.transfer_manager,
    "upload_chunks_concurrently",
)
def test_write_default_workers(mock_upload, mock_get_cpus):
    mock_get_cpus.return_value = 123

    with gs_fastcopy.write("gs://my-bucket/my-file.json") as f:
        f.write(JSON_STR)

    mock_upload.assert_called_once_with(
        ANY,
        ANY,
        max_workers=123,
    )


@patch.object(
    gs_fastcopy.transfer_manager,
    "upload_chunks_concurrently",
)
def test_write_custom_workers(mock_upload):
    with gs_fastcopy.write("gs://my-bucket/my-file.json", max_workers=16) as f:
        f.write(JSON_STR)

    mock_upload.assert_called_once_with(
        ANY,
        ANY,
        max_workers=16,
    )
