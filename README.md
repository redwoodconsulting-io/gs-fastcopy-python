# gs_fastcopy (python)
Optimized file copying &amp; compression for large files on Google Cloud Storage.

**TLDR:**

```python
import gs_fastcopy
import numpy as np

with gs_fastcopy.write('gs://my-bucket/my-file.npz') as f:
    np.savez(f, a=np.zeros(12), b=np.ones(23))

with gs_fastcopy.read('gs://my-bucket/my-file.npz') as f:
    npz = np.load(f)
    a = npz['a']
    b = npz['b']
```



Provides file-like interfaces for:

- Parallel, [XML multipart](https://cloud.google.com/storage/docs/multipart-uploads) uploads to Cloud Storage.
- Parallel, sliced downloads from Cloud Storage using `gcloud storage`.
- Parallel (de)compression using [`pigz` and `unpigz`](https://github.com/madler/pigz) if available (with fallback to standard `gzip` and `gunzip`).

Together, these provided ~70% improvement on uploading a 1.2GB file, and ~40% improvement downloading the same.

> [!Note]
>
> This benchmark is being tested more rigorously, stay tuned.

## Examples

`gs_fastcopy` is easy to use for reading and writing files.

Without compression:

```python
with gs_fastcopy.write('gs://my-bucket/my-file.npz') as f:
    np.savez(f, a=np.zeros(12), b=np.ones(23))
    
with gs_fastcopy.read('gs://my-bucket/my-file.npz') as f:
    npz = np.load(f)
    a = npz['a']
    b = npz['b']
```

With compression: note that we don't use `savez_compressed`:

```python
with gs_fastcopy.write('gs://my-bucket/my-file.npz.gz') as f:
    np.savez(f, a=np.zeros(12), b=np.ones(23))
    
with gs_fastcopy.read('gs://my-bucket/my-file.npz.gz') as f:
    npz = np.load(f)
    a = npz['a']
    b = npz['b']
```

## Caveats & limitations

* **You need a filesystem.**

  Because `gs_fastcloud` uses tools that work with files, it must be able to read/write files, in particular temporary files as set up by `tempfile.TemporaryDirectory()`.

  This is surprisingly versatile, even "very" serverless environments like Cloud Functions present an in-memory file system.

* **You need the `gcloud` SDK on your path.**

  Or, at least the `gcloud storage` component of the SDK.

  `gs_fastcloud` uses `gcloud` to download files. 

  [#2](https://github.com/redwoodconsulting-io/gs-fastcopy-python/issues/2) considers falling back to Python API downloads.

* **You need enough disk space for the compressed & uncompressed files, together.**

  Because `gs_fastcloud` writes the (un)compressed file to disk while (de)compressing it, the file system needs to accommodate both files before the operation completes.

## Why gs_fastcopy

APIs for Google Storage (GS) typically present `File`-like interfaces which read/write data sequentially. For example: open up a stream then write bytes to it until done. Data is streamed between cloud storage and memory. It's easy to use stream-based compression like `gzip` along the way.

Libraries like [`smart_open`](https://github.com/piskvorky/smart_open) add yet more convenience, providing a unified interface for reading/writing local files and several cloud providers, with transparent encryption for `.gz` files. Quite delightful!

Unfortunately, these approaches are single-threaded. We [noticed](https://github.com/dchaley/deepcell-imaging/issues/248) that transfer time for files sized many 100s of MBs was lower than expected. [@lynnlangit](https://github.com/lynnlangit) pointed me toward the composite upload feature in  `gcloud storage cp`. A "few" hours later, `gs_fastcopy` came to be.

## Why both `gcloud` and XML multi-part

I'm glad you asked! I initially implemented this just with `gcloud`'s [composite uploads](https://cloud.google.com/storage/docs/parallel-composite-uploads). But the documentation gave a few warnings about composite uploads.

> [!Warning]
>
> Parallel composite uploads involve deleting temporary objects shortly after upload. Keep in mind the following:
>
> * Because other storage classes are subject to [early deletion fees](https://cloud.google.com/storage/pricing#early-delete), you should always use [Standard storage](https://cloud.google.com/storage/docs/storage-classes#standard) for temporary objects. Once the final object is composed, you can change its storage class.
> * You should not use parallel composite uploads when uploading to a bucket that has a [retention policy](https://cloud.google.com/storage/docs/bucket-lock), because the temporary objects can't be deleted until they meet the retention period.
> * If the bucket you upload to has [default object holds](https://cloud.google.com/storage/docs/object-holds#default-holds) enabled, you must [release the hold](https://cloud.google.com/storage/docs/holding-objects#set-object-hold) from each temporary object before you can delete it.

Basically, composite uploads leverage API pieces, whereas XML multi-part is a dedicated function that understands the chunk files on GCS are special.

On the other hand, the XML multi-part API does require some permissions. (We may need to fall back to `gcloud` in that case!)

On top of being "weird", composite uploads are actually slower. I found this wonderful benchmarking by Christopher Madden: [High throughput file transfers with Google Cloud Storage (GCS)](https://www.beginswithdata.com/2024/02/01/google-cloud-storage-max-throughput/). TLDR, `gcloud` sliced downloads outperform the Python API, but for writes the XML multi-part API is best. (By far, if many cores are available.)

