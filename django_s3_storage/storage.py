from __future__ import unicode_literals
import gzip
import mimetypes
import os
import posixpath
import shutil
from io import TextIOBase
from contextlib import closing
from functools import wraps
from tempfile import SpooledTemporaryFile
from threading import local
import boto3
from botocore.exceptions import ClientError
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.core.files.storage import Storage
from django.core.files.base import File
from django.core.signals import setting_changed
from django.contrib.staticfiles.storage import ManifestFilesMixin
from django.utils.six.moves.urllib.parse import urlsplit, urlunsplit, urljoin
from django.utils.deconstruct import deconstructible
from django.utils.encoding import force_bytes, filepath_to_uri, force_text, force_str
from django.utils.timezone import make_naive, utc


# Some parts of Django expect an IOError, other parts expect an OSError, so this class inherits both!
# In Python 3, the distinction is irrelevant, but in Python 2 they are distinct classes.
if OSError is IOError:
    class S3Error(OSError):
        pass
else:
    class S3Error(OSError, IOError):
        pass


def _wrap_errors(func):
    @wraps(func)
    def _do_wrap_errors(self, name, *args, **kwargs):
        try:
            return func(self, name, *args, **kwargs)
        except ClientError as ex:
            raise S3Error("S3Storage error at {!r}: {}".format(name, force_text(ex)))
    return _do_wrap_errors


def _temporary_file():
    return SpooledTemporaryFile(max_size=1024*1024*10)  # 10 MB.


class S3File(File):

    """
    A file returned from Amazon S3.
    """

    def __init__(self, file, name, storage):
        super(S3File, self).__init__(file, name)
        self._storage = storage

    def open(self, mode="rb"):
        if self.closed:
            self.file = self._storage.open(self.name, mode).file
        return super(S3File, self).open(mode)


class _Local(local):

    """
    Thread-local connection manager.

    Boto3 objects are not thread-safe.
    http://boto3.readthedocs.io/en/latest/guide/resources.html#multithreading-multiprocessing
    """

    def __init__(self):
        # self.session = boto3.session.Session()
        # self.s3_connection = self.session.client(
        self.s3_connection = boto3.client(
          's3',
          aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
          aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
          endpoint_url=settings.AWS_S3_ENDPOINT_URL,
          verify=False
        )


@deconstructible
class S3Storage(Storage):

    """
    An implementation of Django file storage over S3.
    """

    def _setup(self):
        # Create a thread-local connection manager.
        self._connections = _Local()

    @property
    def s3_connection(self):
        return self._connections.s3_connection

    def __init__(self, **kwargs):
        self._setup()
        # All done!
        super(S3Storage, self).__init__()

    # Helpers.

    def _get_key_name(self, name):
        if name.startswith("/"):
            name = name[1:]
        return name

    def _object_params(self, name):
        params = {
            "Bucket": settings.AWS_S3_BUCKET_NAME,
            "Key": self._get_key_name(name),
        }
        return params

    @_wrap_errors
    def _open(self, name, mode="rb"):
        if mode != "rb":
            raise ValueError("S3 files can only be opened in read-only mode")
        # Load the key into a temporary file. It would be nice to stream the
        # content, but S3 doesn't support seeking, which is sometimes needed.
        obj = self.s3_connection.get_object(**self._object_params(name))
        content = _temporary_file()
        shutil.copyfileobj(obj["Body"], content)
        content.seek(0)
        # All done!
        return S3File(content, name, self)

    @_wrap_errors
    def _save(self, name, content):
        put_params = self._object_params(name)
        temp_files = []
        # The Django file storage API always rewinds the file before saving,
        # therefor so should we.
        content.seek(0)
        # Convert content to bytes.
        if isinstance(content.file, TextIOBase):
            temp_file = _temporary_file()
            temp_files.append(temp_file)
            for chunk in content.chunks():
                temp_file.write(force_bytes(chunk))
            temp_file.seek(0)
            content = temp_file
        # Calculate the content type.
        content_type, _ = mimetypes.guess_type(name, strict=False)
        content_type = content_type or "application/octet-stream"
        put_params["ContentType"] = content_type
        # Calculate the content encoding.
        # Save the file.
        self.s3_connection.put_object(Body=content.read(), **put_params)
        # Close all temp files.
        for temp_file in temp_files:
            temp_file.close()
        # All done!
        return name

    # Subsiduary storage methods.

    @_wrap_errors
    def meta(self, name):
        """Returns a dictionary of metadata associated with the key."""
        return self.s3_connection.head_object(**self._object_params(name))

    @_wrap_errors
    def delete(self, name):
        self.s3_connection.delete_object(**self._object_params(name))

    def exists(self, name):
        if name.endswith("/"):
            # This looks like a directory, but on S3 directories are virtual, so we need to see if the key starts
            # with this prefix.
            results = self.s3_connection.list_objects(
                Bucket=settings.AWS_S3_BUCKET_NAME,
            )
            return "Contents" in results
        # This may be a file or a directory. Check if getting the file metadata throws an error.
        try:
            self.meta(name)
        except S3Error:
            # It's not a file, but it might be a directory. Check again that it's not a directory.
            return self.exists(name + "/")
        else:
            return True

    def listdir(self, path):
        path = self._get_key_name(path)
        path = "" if path == "." else path + "/"
        # Look through the paths, parsing out directories and paths.
        files = []
        dirs = []
        paginator = self.s3_connection.get_paginator("list_objects")
        pages = paginator.paginate(
            Bucket=settings.AWS_S3_BUCKET_NAME,
            Delimiter="/",
            Prefix=path,
        )
        for page in pages:
            for entry in page.get("Contents", ()):
                files.append(posixpath.relpath(entry["Key"], path))
            for entry in page.get("CommonPrefixes", ()):
                dirs.append(posixpath.relpath(entry["Prefix"], path))
        # All done!
        return dirs, files

    def size(self, name):
        return self.meta(name)["ContentLength"]

    def url(self, name):
        return urljoin(settings.AWS_S3_PUBLIC_URL, filepath_to_uri(name))

    def modified_time(self, name):
        return make_naive(self.meta(name)["LastModified"], utc)

    created_time = accessed_time = modified_time

    def get_modified_time(self, name):
        timestamp = self.meta(name)["LastModified"]
        return timestamp if settings.USE_TZ else make_naive(timestamp)

    get_created_time = get_accessed_time = get_modified_time
