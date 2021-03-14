#!/usr/bin/python3
#
# Copyright (C) 2021 Frédéric Pierret (fepitre) <frederic.pierret@qubes-os.org>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
#

import argparse
import debian.deb822
import datetime
import gzip
import hashlib
import logging
import os
import requests
import sys
import uuid

import ssl
import httpx
import urllib3.exceptions

import urllib.error
import urllib.request
import http.client
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

SNAPSHOT_DEBIAN = "https://snapshot.debian.org"
FTP_DEBIAN = "https://ftp.debian.org"
TS_FORMAT = "%Y%m%dT%H%M%SZ"

MAX_RETRY_WAIT = 15
MAX_RETRY_STOP = 3

MAX_RETRY_RESUME_WAIT = 5
MAX_RETRY_RESUME_STOP = 1000  # this is clearly bruteforce but we have no choice

logger = logging.getLogger("SnapshotMirror")
logging.basicConfig(level=logging.INFO)


def sha256sum(fname):
    sha256 = hashlib.sha256()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def parse_ts(ts):
    return datetime.datetime.strptime(ts, TS_FORMAT)


@retry(
    retry=(
        retry_if_exception_type(httpx.HTTPError) |
        retry_if_exception_type(urllib3.exceptions.HTTPError) |
        retry_if_exception_type(ssl.SSLError)
    ),
    wait=wait_fixed(MAX_RETRY_WAIT),
    stop=stop_after_attempt(MAX_RETRY_STOP),
)
def download_with_retry(url, path, sha256=None, no_clean=False):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    client = httpx.Client()
    try:
        with client.stream("GET", url) as resp:
            resp.raise_for_status()
            tmp_path = f"{path}.{uuid.uuid4()}.part"
            with open(tmp_path, "wb") as out_file:
                for chunk in resp.iter_raw():
                    out_file.write(chunk)
    except Exception as e:
        logger.error(str(e))
        raise
    tmp_sha256 = sha256sum(tmp_path)
    if sha256 and tmp_sha256 != sha256:
        # if not no_clean:
        #     os.remove(tmp_path)
        raise Exception(f"{os.path.basename(url)}: wrong SHA256: {tmp_sha256} != {sha256}")
    os.rename(tmp_path, path)
    return path


@retry(
    retry=(
        retry_if_exception_type(IOError) |
        retry_if_exception_type(http.client.HTTPException) |
        retry_if_exception_type(ssl.SSLError)
    ),
    wait=wait_fixed(MAX_RETRY_RESUME_WAIT),
    stop=stop_after_attempt(MAX_RETRY_RESUME_STOP),
)
def download_with_retry_and_resume(url, path, timeout=30, sha256=None, no_clean=False):
    # Inspired from https://gist.github.com/mjohnsullivan/9322154
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp_path = path + ".part"
    block_size = 1000 * 1000  # 1MB
    first_byte = os.path.getsize(tmp_path) if os.path.exists(tmp_path) else 0
    fname = os.path.basename(url)
    try:
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
            raise Exception(f"{fname}: wrong SHA256: {tmp_sha256} != {sha256}")
        os.rename(tmp_path, path)
    elif file_size == -1:
        raise Exception(f"{f}: failed to get 'Content-Length': {url}")


class SnapshotMirrorException(Exception):
    pass


class Package:
    def __init__(self, name, version, architecture, archive, timestamp, suite, component, size, sha256, url):
        self.name = name
        self.version = version
        self.architecture = architecture
        self.archive = archive
        self.timestamp = timestamp
        self.suite = suite
        self.component = component
        self.size = size
        self.sha256 = sha256
        self.url = url

    def __repr__(self):
        return f"{self.archive}:{self.timestamp}:{self.suite}:{self.component}:{os.path.basename(self.url[0])}"


class SnapshotMirrorCli:
    def __init__(self, localdir, archives, timestamps, suites, components, architectures):
        self.localdir = os.path.abspath(os.path.expanduser(localdir))
        self.archives = archives
        self.timestamps = timestamps
        self.suites = suites
        self.components = components
        self.architectures = architectures

        if not os.path.exists(self.localdir):
            raise SnapshotMirrorException(f"Cannot find: {self.localdir}")

    @staticmethod
    def get_timestamps_from_metasnap(archive):
        """
        Get all snapshot.debian.org timestamps from metasnap.debian.net
        """
        timestamps = []
        url = f"https://metasnap.debian.net/cgi-bin/api?timestamps={archive}"
        try:
            resp = requests.get(url)
        except requests.ConnectionError as e:
            raise SnapshotMirrorException(str(e))

        if resp.ok:
            timestamps = sorted(set(resp.text.rstrip("\n").split("\n")), reverse=True)
        return timestamps

    def get_timestamps(self, archive="debian"):
        """
        Get timestamps to use
        """
        timestamps = []
        if self.timestamps:
            if ':' in self.timestamps[0]:
                all_timestamps = self.get_timestamps_from_metasnap(archive)
                ts_begin, ts_end = self.timestamps[0].split(":", 1)
                if not ts_end:
                    ts_end = all_timestamps[0]
                if not ts_begin:
                    ts_begin = all_timestamps[-1]
                ts_begin = parse_ts(ts_begin)
                ts_end = parse_ts(ts_end)
                for ts in all_timestamps:
                    if ts_begin <= parse_ts(ts) <= ts_end:
                        timestamps.append(ts)
            else:
                timestamps = self.timestamps
        else:
            timestamps = self.get_timestamps_from_metasnap(archive)
        return timestamps

    def get_packages(self, archive, timestamp, suite, component, arch):
        """"
        Get a parsed packages list from Packages.gz repository file
        """
        packages = []
        if arch == "source":
            repodata = f"{self.localdir}/archive/{archive}/{timestamp}/dists/{suite}/{component}/{arch}/Sources.gz"
        else:
            repodata = f"{self.localdir}/archive/{archive}/{timestamp}/dists/{suite}/{component}/binary-{arch}/Packages.gz"
        try:
            with gzip.open(repodata) as fd:
                if arch == "source":
                    for raw_pkg in debian.deb822.Sources.iter_paragraphs(fd):
                        for src_file in raw_pkg["Checksums-Sha256"]:
                            # f"{FTP_DEBIAN}/{archive}/{raw_pkg['Directory']}/{src_file['name']}",
                            pkg = Package(
                                name=raw_pkg["Package"],
                                version=raw_pkg["Version"],
                                architecture="source",
                                archive=archive,
                                timestamp=timestamp,
                                suite=suite,
                                component=component,
                                size=src_file["size"],
                                sha256=src_file["sha256"],
                                url=[
                                    f"{SNAPSHOT_DEBIAN}/archive/{archive}/{timestamp}/{raw_pkg['Directory']}/{src_file['name']}"
                                ]
                            )
                            packages.append(pkg)
                else:
                    for raw_pkg in debian.deb822.Packages.iter_paragraphs(fd):
                        url = f"{SNAPSHOT_DEBIAN}/archive/{archive}/{timestamp}/{raw_pkg['Filename']}"
                        pkg = Package(
                            name=raw_pkg["Package"],
                            version=raw_pkg["Version"],
                            architecture=raw_pkg["Architecture"],
                            archive=archive,
                            timestamp=timestamp,
                            suite=suite,
                            component=component,
                            size=raw_pkg['Size'],
                            sha256=raw_pkg["SHA256"],
                            url=[url],
                        )
                        packages.append(pkg)
        except Exception as e:
            logger.error(str(e))
        return packages

    def download(self, url, sha256=None, size=None, no_clean=False):
        """
        Download function to store file according to its SHA256
        """
        # If SHA256 sum is given (DEB files) we use it else we compute it
        # after download (Packages.gz, i18n etc.).
        fname = f"{self.localdir}/{url.replace(SNAPSHOT_DEBIAN, '').replace(FTP_DEBIAN, '')}"
        if sha256:
            fname_sha256 = f"{self.localdir}/by-hash/SHA256/{sha256}"
            already_downloaded = False
            if os.path.exists(fname_sha256):
                already_downloaded = True
            if not already_downloaded:
                try:
                    # For file less than 10MB we do a direct download
                    if size and int(size) <= 10 * 1000 * 1000:
                        download_with_retry(url, fname_sha256, sha256=sha256, no_clean=no_clean)
                    else:
                        download_with_retry_and_resume(url, fname_sha256, sha256=sha256, no_clean=no_clean)
                except Exception as e:
                    raise SnapshotMirrorException(f"Failed to download package: {str(e)}")
        else:
            if os.path.exists(fname):
                return
            tmp_path = f"{self.localdir}/by-hash/SHA256/{uuid.uuid4()}.tmp"
            download_with_retry(url, tmp_path)
            sha256 = sha256sum(tmp_path)
            if not sha256:
                raise SnapshotMirrorException(f"Failed to get SHA256: {url}")
            fname_sha256 = f"{self.localdir}/by-hash/SHA256/{sha256}"
            if os.path.exists(fname_sha256):
                os.remove(tmp_path)
            else:
                os.rename(tmp_path, fname_sha256)

        # Each file is a symlink to it's SHA256 stored in the parent
        # tree folder
        if not os.path.exists(fname):
            os.makedirs(os.path.dirname(fname), exist_ok=True)
            os.symlink(os.path.relpath(fname_sha256, os.path.dirname(fname)), fname)

        return fname

    def download_repodata(self, archive, timestamp, suite, component, arch):
        """
        Download Packages.gz or Sources.gz
        """
        if arch == "source":
            packages = f"{arch}/Sources.gz"
        else:
            packages = f"binary-{arch}/Packages.gz"
        f = f"/archive/{archive}/{timestamp}/dists/{suite}/{component}/{packages}"
        localfile = self.localdir + f
        remotefile = f"{SNAPSHOT_DEBIAN}{f}"
        logger.debug(remotefile)
        if not os.path.exists(localfile):
            self.download(remotefile)

    def download_release(self, archive, timestamp, suite, component, arch):
        """
        Download repository Release files and translation
        """
        if arch != "source":
            arch = f"binary-{arch}"
        metadata_files = [
            f"/archive/{archive}/{timestamp}/dists/{suite}/Release",
            f"/archive/{archive}/{timestamp}/dists/{suite}/Release.gpg",
            f"/archive/{archive}/{timestamp}/dists/{suite}/InRelease",
            f"/archive/{archive}/{timestamp}/dists/{suite}/{component}/{arch}/Release",
            f"/archive/{archive}/{timestamp}/dists/{suite}/{component}/i18n/Translation-en.bz2"
        ]
        for f in metadata_files:
            localfile = self.localdir + f
            remotefile = f"{SNAPSHOT_DEBIAN}{f}"
            logger.debug(remotefile)
            if os.path.exists(localfile):
                continue
            self.download(remotefile)

    def download_package(self, package, check_only, no_clean):
        logger.info(package)
        if check_only:
            fname_sha256 = f"{self.localdir}/by-hash/SHA256/{package.sha256}"
            if not os.path.exists(fname_sha256):
                logger.info(f"MISSING: {package}")
                return
            if sha256sum(fname_sha256) != package.sha256:
                raise SnapshotMirrorException(
                    f"Wrong SHA256 for: {fname_sha256}")
        else:
            result = None
            for url in package.url:
                try:
                    result = self.download(url, package.sha256, size=package.size, no_clean=no_clean)
                    break
                except Exception as e:
                    logger.debug(f"Try with another URL ({str(e)})")
            if not result:
                raise SnapshotMirrorException("No more URL to try")

    def run(self, check_only=False, no_clean=False):
        """
        Run the snapshot mirroring on all the archives, timestamps, suites,
        components and architectures
        """
        os.makedirs(f"{self.localdir}/by-hash/SHA256", exist_ok=True)
        for archive in self.archives:
            for timestamp in self.get_timestamps(archive):
                for suite in self.suites:
                    for component in self.components:
                        for arch in self.architectures:
                            self.download_repodata(archive, timestamp, suite, component, arch)
                            packages = self.get_packages(archive, timestamp, suite, component, arch)
                            for package in packages:
                                self.download_package(package, check_only=check_only, no_clean=no_clean)
                            # We download Release files at the end to ack
                            # the mirror sync. It is for helping rebuilders
                            # checking available mirrors.
                            self.download_release(archive, timestamp, suite, component, arch)


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "local_directory",
        help="Local directory for snapshot mirror.")
    parser.add_argument(
        "--archive",
        help="Debian archive to mirror. "
             "Default is 'debian' and is the only supported archive right now.",
        action="append",
        default=[],
    )
    parser.add_argument(
        "--suite",
        help="Debian suite to mirror. Can be used multiple times. "
        "Default is 'unstable'",
        action="append",
        default=[],
    )
    parser.add_argument(
        "--component",
        help="Debian component to mirror. Default is 'main'",
        action="append",
        default=[],
    )
    parser.add_argument(
        "--arch",
        help="Debian arch to mirror. Can be used multiple times.",
        action="append",
        default=[],
    )
    parser.add_argument(
        "--timestamp",
        help="Snapshot timestamp to mirror. Can be used multiple times. "
        "Default is all the available timestamps. Timestamps range can be "
        "expressed with ':' separator. Empty boundary is allowed and and this "
        "case, it would use the lower or upper value in all the available "
        "timestamps. For example: '20200101T000000Z:20210315T085036Z', "
        "'20200101T000000Z:' or ':20100101T000000Z'.",
        action="append",
        default=[],
    )
    parser.add_argument(
        "--check-only",
        action="store_true",
        help="Check downloaded packages.",
    )
    parser.add_argument(
        "--no-clean-part-file",
        action="store_true",
        help="No clean partially downloaded packages.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Display logger info messages.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Display logger debug messages.",
    )
    return parser.parse_args()


def main():
    args = get_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)
    elif args.verbose:
        logger.setLevel(logging.INFO)
    else:
        logger.setLevel(logging.ERROR)

    if not args.local_directory:
        logger.error("Please provide local mirror directory")
        return 1

    if not args.archive:
        args.archive = ["debian"]
    if not args.suite:
        args.suite = ["unstable"]
    if not args.component:
        args.component = ["main"]

    try:
        cli = SnapshotMirrorCli(
            localdir=args.local_directory,
            archives=args.archive,
            timestamps=args.timestamp,
            suites=args.suite,
            components=args.component,
            architectures=args.arch,
        )
        cli.run(check_only=args.check_only, no_clean=args.no_clean_part_file)
    except (ValueError, SnapshotMirrorException) as e:
        logger.error(str(e))
        return 1


if __name__ == "__main__":
    sys.exit(main())
