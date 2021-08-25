import os
import uuid
import requests
import ssl
import httpx
import urllib3.exceptions
import urllib.error
import urllib.request
import http.client

from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

from lib.common import sha256sum
from lib.log import logger

MAX_RETRY_WAIT = 5
MAX_RETRY_STOP = 100

MAX_RETRY_RESUME_WAIT = 5
MAX_RETRY_RESUME_STOP = 1000  # this is clearly bruteforce but we have no choice

MAX_DIRECT_DOWNLOAD_SIZE = 100  # MB
# This is the window blocksize to use for retry and resume download function
MAX_RETRY_RESUME_BLOCK_SIZE = 50  # MB


@retry(
    retry=(
        retry_if_exception_type(urllib3.exceptions.HTTPError) |
        retry_if_exception_type(http.client.HTTPException) |
        retry_if_exception_type(ssl.SSLError) |
        retry_if_exception_type(requests.exceptions.ConnectionError) |
        retry_if_exception_type(requests.exceptions.ReadTimeout)
    ),
    wait=wait_fixed(MAX_RETRY_WAIT),
    stop=stop_after_attempt(MAX_RETRY_STOP),
)
def url_exists(url, timeout=10):
    resp = requests.head(url, timeout=timeout)
    return resp.ok


@retry(
    retry=(
        retry_if_exception_type(urllib.error.URLError) |
        retry_if_exception_type(http.client.HTTPException) |
        retry_if_exception_type(ssl.SSLError) |
        retry_if_exception_type(requests.exceptions.ConnectionError) |
        retry_if_exception_type(requests.exceptions.ReadTimeout)
    ),
    wait=wait_fixed(MAX_RETRY_WAIT),
    stop=stop_after_attempt(MAX_RETRY_STOP),
)
def get_file_size(url):
    try:
        size = int(urllib.request.urlopen(url).info().get("Content-Length", -1))
    except TypeError:
        size = None
    return size


@retry(
    retry=(
        retry_if_exception_type(urllib3.exceptions.HTTPError) |
        retry_if_exception_type(http.client.HTTPException) |
        retry_if_exception_type(ssl.SSLError) |
        retry_if_exception_type(requests.exceptions.ConnectionError) |
        retry_if_exception_type(requests.exceptions.ReadTimeout)
    ),
    wait=wait_fixed(MAX_RETRY_WAIT),
    stop=stop_after_attempt(MAX_RETRY_STOP),
)
def get_response_with_retry(url, timeout=10):
    resp = requests.get(url, timeout=timeout)
    return resp


@retry(
    retry=(
        retry_if_exception_type(OSError) |
        retry_if_exception_type(httpx.HTTPError) |
        retry_if_exception_type(urllib3.exceptions.HTTPError) |
        retry_if_exception_type(http.client.HTTPException) |
        retry_if_exception_type(ssl.SSLError) |
        retry_if_exception_type(requests.exceptions.ConnectionError)
    ),
    wait=wait_fixed(MAX_RETRY_WAIT),
    stop=stop_after_attempt(MAX_RETRY_STOP),
)
def download_with_retry(url, path, sha256=None):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    client = httpx.Client()
    fname = os.path.basename(url)
    try:
        with client.stream("GET", url) as resp:
            resp.raise_for_status()
            tmp_path = f"{path}.{uuid.uuid4()}.part"
            with open(tmp_path, "wb") as out_file:
                for chunk in resp.iter_raw():
                    out_file.write(chunk)
    except Exception as e:
        logger.debug(f"{fname}: retrying ({download_with_retry.retry.statistics['attempt_number']}/{MAX_RETRY_STOP}): {str(e)}")
        raise http.client.HTTPException
    tmp_sha256 = sha256sum(tmp_path)
    if sha256 and tmp_sha256 != sha256:
        # if not no_clean:
        #     os.remove(tmp_path)
        raise Exception(f"{os.path.basename(url)}: wrong SHA256: {tmp_sha256} != {sha256}")
    os.rename(tmp_path, path)
    return sha256


@retry(
    retry=(
        retry_if_exception_type(IOError) |
        retry_if_exception_type(http.client.HTTPException) |
        retry_if_exception_type(ssl.SSLError)
    ),
    wait=wait_fixed(MAX_RETRY_RESUME_WAIT),
    stop=stop_after_attempt(MAX_RETRY_RESUME_STOP),
)
def download_with_retry_and_resume(url, path, timeout=30, sha256=None, no_clean=False, file_size=None):
    # Inspired from https://gist.github.com/mjohnsullivan/9322154
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp_path = path + ".part"
    block_size = MAX_RETRY_RESUME_BLOCK_SIZE * 1000 * 1000  # MB
    first_byte = os.path.getsize(tmp_path) if os.path.exists(tmp_path) else 0
    fname = os.path.basename(url)
    try:
        if file_size is None:
            file_size = int(urllib.request.urlopen(url).info().get("Content-Length", -1))
        logger.debug(f"{fname}: starting download at {first_byte / 1e6:.6f}MB "
                     f"(Total: {file_size / 1e6:.6f}MB)")
        while first_byte < file_size:
            last_byte = first_byte + block_size if first_byte + block_size < file_size else file_size - 1
            r = urllib.request.Request(url)
            r.headers["Range"] = f"bytes={first_byte}-{last_byte}"
            logger.debug(f"{fname}: downloading bytes range {first_byte} - {last_byte}")
            data_chunk = urllib.request.urlopen(r, timeout=timeout).read()
            with open(tmp_path, "ab") as f:
                f.write(data_chunk)
            first_byte = last_byte + 1
    except Exception as e:
        logger.debug(f"{fname}: retrying ({download_with_retry_and_resume.retry.statistics['attempt_number']}/{MAX_RETRY_RESUME_STOP}): {str(e)}")
        raise

    if file_size == os.path.getsize(tmp_path):
        tmp_sha256 = sha256sum(tmp_path)
        if sha256 and tmp_sha256 != sha256:
            if not no_clean:
                os.remove(tmp_path)
            raise Exception(f"{fname}: wrong SHA256: {tmp_sha256} (expected: {sha256})")
        os.rename(tmp_path, path)
        sha256 = tmp_sha256
    elif file_size == -1:
        raise Exception(f"{f}: failed to get 'Content-Length': {url}")

    return sha256


def download_with_retry_and_resume_threshold(url, path, size=None, sha256=None, no_clean=False):
    # For file less than MAX_DIRECT_DOWNLOAD_SIZE we do a direct download
    if size is not None and int(size) <= MAX_DIRECT_DOWNLOAD_SIZE * 1000 * 1000:
        return download_with_retry(url, path, sha256=sha256)
    else:
        return download_with_retry_and_resume(url, path, sha256=sha256, no_clean=no_clean, file_size=size)
