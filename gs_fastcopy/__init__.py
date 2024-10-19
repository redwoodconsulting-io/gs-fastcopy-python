"""
gs_fastcopy: optimized file transfer to and from Google Cloud Storage.

gs_fastcopy provides a stream interface around the XML Multipart Upload
API (which only works with files) for optimized reading and writing. It
also wraps the command-line tools `pigz` and `unpigz` (parallel gzip)
for faster (de)compression, if available.

See also the `read()` and `write()` functions.
"""

import os
import shutil
import subprocess
import tempfile
from contextlib import contextmanager

from google.cloud import storage
from google.cloud.storage import transfer_manager


@contextmanager
def read(gs_uri, billing_project=None):
    """
    Context manager for reading a file from Google Cloud Storage.
    Compresses and decompresses files on the fly, if necessary.
    Also supports local files.

    Usage example, reading a numpy npz file:

    ```
    import gs_fastcopy
    import numpy as np

    with gs_fastcopy.read('gs://my-bucket/my-file.npz') as f:
        npz = np.load(f)
        a = npz['a']
        b = npz['b']
    ```

    This will download the file to a temporary directory, and open it
    for reading. When the 'with' block exits, the handle is closed and
    the temporary directory is deleted.

    If the gs_uri ends with '.gz', the file is decompressed before
    reading. Note that the decompression is performed in an external
    process, not streaming in memory. This means you need enough disk
    space for the compressed file, and the decompressed file, together.

    :param gs_uri: The Google Cloud Storage URI to read from.
    :param billing_project: The billing project for the transfer (default: app default credentials quota project).
    """
    # If true, don't delete the compressed file after decompression.
    keep_archive = False

    with tempfile.TemporaryDirectory() as tmp:
        buffer_file_name = os.path.join(
            tmp, "download.gz" if gs_uri.endswith(".gz") else "download"
        )

        if gs_uri.startswith("gs://"):
            _download_gs_uri(gs_uri, buffer_file_name, billing_project)
        else:
            # Create a symlink to the local file, to avoid copying,
            # while reusing the decompression code. Note that we
            # add --keep to not delete the file after decompression.
            # Note that we need the abspath to support relative uris.
            keep_archive = True
            os.symlink(os.path.abspath(gs_uri), buffer_file_name)

        # If necessary, decompress the file before reading.
        if buffer_file_name.endswith(".gz"):
            # unpigz is a parallel gunzip implementation that's
            # much faster when hardware is available.
            tool = "unpigz" if shutil.which("unpigz") else "gunzip"

            # See notes for keep_archive=True
            command = (
                [tool, "--keep", "--force", buffer_file_name]
                if keep_archive
                else [tool, buffer_file_name]
            )

            result = subprocess.run(
                command,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )

            # TODO: handle errors better than this
            if result.returncode != 0:
                raise Exception(
                    f"Failed to extract file downloaded from {gs_uri}: stderr: {result.stderr}"
                )

            # Remove the '.gz' extension from the filename (like the tools do)
            buffer_file_name = buffer_file_name[:-3]

        with open(buffer_file_name, "rb") as f:
            yield f


@contextmanager
def write(gs_uri, max_workers=None, chunk_size=None, billing_project=None):
    """
    Context manager for writing a file to Google Cloud Storage.
    Compresses and decompresses files on the fly, if necessary.
    Also supports local files.

    Usage example, writing a numpy npz file:

    ```
    import gs_fastcopy
    import numpy as np

    with gs_fastcopy.write('gs://my-bucket/my-file.npz') as f:
        np.savez(f, a=np.zeros(12), b=np.ones(23))
    ```

    This will open a file for writing in a temporary directory. When
    the 'with' block exits, the file is uploaded to the specified
    Google Cloud Storage URI, and the temporary directory is deleted.

    If the gs_uri ends with '.gz', the file is compressed before
    uploading. Note that the compression is performed in an external
    process, not streaming in memory. This means you need enough disk
    space for the uncompressed file, and the compressed file, together.

    :param gs_uri: The Google Cloud Storage URI to write to.
    :param max_workers: The maximum number of workers to use. None for default (available CPUs).
    :param chunk_size: The size of each chunk to upload. None for default.
    :param billing_project: The billing project for the transfer (default: app default credentials quota project).
    """

    if max_workers is None:
        max_workers = _get_available_cpus()

    # Create a temporary scratch directory.
    # Will be deleted when the 'with' closes.
    with tempfile.TemporaryDirectory() as tmp_dir:
        # We need an actual filename within the scratch directory.
        buffer_file_name = os.path.join(tmp_dir, "file_to_upload")

        # Yield the file object for the caller to write.
        with open(buffer_file_name, "wb") as tmp_file:
            yield tmp_file

        # If requested, compress the file before uploading.
        if gs_uri.endswith(".gz"):
            # pigz is a parallel gzip implementation that's
            # much faster when hardware is available.
            tool = "pigz" if shutil.which("pigz") else "gzip"

            # TODO: handle errors
            result = subprocess.run(
                [tool, buffer_file_name],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )

            # TODO: handle errors better than this
            if result.returncode != 0:
                raise Exception(
                    f"Failed to compress file for upload to {gs_uri}: stderr: {result.stderr}"
                )

            # Add the '.gz' extension to the filename (like the tools do)
            buffer_file_name += ".gz"

        if gs_uri.startswith("gs://"):
            _write_gs_uri(
                buffer_file_name, gs_uri, max_workers, chunk_size, billing_project
            )
        else:
            # If the URI is not a gs:// URI, it's a local file path.
            # In this case, we can just move the file to the destination.
            shutil.move(buffer_file_name, gs_uri)


# Helper function to get the number of available CPUs.
# On many Unixen, available CPUs can be restricted
# using schedule affinity; that is a more accurate
# measure of available CPUs if available. If not,
# fall back to os.cpu_count().
def _get_available_cpus():
    try:
        return len(os.sched_getaffinity(0))
    except AttributeError:
        return os.cpu_count()


def _download_gs_uri(gs_uri, buffer_file_name, billing_project=None):
    gcloud_cmd = ["gcloud", "storage", "cp"]
    if billing_project:
        gcloud_cmd += ["--billing-project", billing_project]
    gcloud_cmd += [gs_uri, buffer_file_name]

    result = subprocess.run(
        gcloud_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE
    )

    # TODO: handle errors better than this
    if result.returncode != 0:
        raise Exception(
            f"Failed to download file from {gs_uri}: stderr: {result.stderr}"
        )


def _write_gs_uri(buffer_file_name, gs_uri, max_workers, chunk_size, billing_project):
    args = {"max_workers": max_workers}
    if chunk_size is not None:
        args["chunk_size"] = chunk_size

    # Parse gs_uri into a blob
    client = storage.Client()
    parsed_uri = storage.Blob.from_string(gs_uri, client=client)
    if billing_project:
        bucket = client.bucket(parsed_uri.bucket.name, user_project=billing_project)
    else:
        bucket = client.bucket(parsed_uri.bucket.name)
    gs_blob = storage.Blob(parsed_uri.name, bucket)

    # TODO: handle errors in transfer_manager
    transfer_manager.upload_chunks_concurrently(buffer_file_name, gs_blob, **args)
