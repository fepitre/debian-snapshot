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
from dateutil.parser import parse as parsedate
from api.db import DBrepodata, DBtimestamp, db_create_session, DBtempfile, DBtempsrcpkg, DBtempbinpkg

SNAPSHOT_DEBIAN = "http://snapshot.debian.org"
FTP_DEBIAN = "https://ftp.debian.org"
TS_FORMAT = "%Y%m%dT%H%M%SZ"

SNAPSHOT_QUBES = "https://deb.qubes-os.org/all-versions"

# Supported Debian archives
DEBIAN_ARCHIVES = {"debian"}

# Supported QubesOS archives
QUBES_ARCHIVES = {"qubes-r4.1-vm"}

MAX_RETRY_WAIT = 5
MAX_RETRY_STOP = 100

MAX_RETRY_RESUME_WAIT = 5
MAX_RETRY_RESUME_STOP = 1000  # this is clearly bruteforce but we have no choice

MAX_DIRECT_DOWNLOAD_SIZE = 100  # MB
# This is the window blocksize to use for retry and resume download function
MAX_RETRY_RESUME_BLOCK_SIZE = 50  # MB

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


def append_to_str(orig, new):
    return ','.join(sorted(set(orig.split(',') + [new])))


@retry(
    retry=(
        retry_if_exception_type(urllib3.exceptions.HTTPError) |
        retry_if_exception_type(http.client.HTTPException) |
        retry_if_exception_type(ssl.SSLError) |
        retry_if_exception_type(requests.exceptions.ConnectionError)
    ),
    wait=wait_fixed(MAX_RETRY_WAIT),
    stop=stop_after_attempt(MAX_RETRY_STOP),
)
def url_exists(url):
    resp = requests.head(url)
    return resp.ok


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
def download_with_retry(url, path, sha256=None, no_clean=False):
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
    block_size = MAX_RETRY_RESUME_BLOCK_SIZE * 1000 * 1000  # MB
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


class SnapshotMirrorRepodataNotFoundException(Exception):
    pass


class File:
    def __init__(self, name, version, architecture, archive, timestamp, suite, component, size, sha256, relative_path, url):
        self.name = name
        self.version = version
        self.architecture = architecture
        self.archive = archive
        self.timestamp = timestamp
        # self.suite = suite
        # self.component = component
        self.size = size
        self.sha256 = sha256
        self.relative_path = relative_path
        self.url = url

    def __repr__(self):
        # return f"{self.archive}:{self.timestamp}:{self.suite}:{self.component}:{os.path.basename(self.relative_path)}"
        return f"{self.archive}:{self.timestamp}:{os.path.basename(self.relative_path)}"


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

        self.map_srcpkg_hash = {}
        self.map_binpkg_hash = {}

    def provision_database(self, archive, timestamp, suites, components, arches, ignore_provisioned=False):
        session = db_create_session()
        to_add = []
        files = {}
        logger.info(f"Provision database for timestamp: {timestamp}")
        # we convert timestamp to a SQL format
        parsed_ts = parsedate(timestamp).strftime("%Y-%m-%dT%H:%M:%SZ")
        db_timestamp = session.query(DBtimestamp).get(parsed_ts)
        if not db_timestamp:
            db_timestamp = DBtimestamp(value=parsed_ts)
            if timestamp != "99990101T000000Z":
                to_add.append(db_timestamp)

        for suite in suites:
            for component in components:
                for arch in arches:
                    if arch == "source":
                        packages = f"{arch}/Sources.gz"
                    else:
                        packages = f"binary-{arch}/Packages.gz"
                    repodata = f"archive/{archive}/{timestamp}/dists/{suite}/{component}/{packages}"
                    repodata_path = f"{self.localdir}/{repodata}"
                    logger.debug(f"Processing {repodata_path}")

                    # Check if we already provisioned DB with
                    repodata_id = hashlib.sha1(repodata.encode()).hexdigest()
                    db_repodata = session.query(DBrepodata).get(repodata_id)
                    if not ignore_provisioned and db_repodata:
                        session.close()
                        continue
                    if not os.path.exists(repodata_path):
                        logger.error(f"Cannot find {repodata_path}.")
                        continue
                    with open(repodata_path) as fd:
                        if arch == "source":
                            for raw_pkg in debian.deb822.Sources.iter_paragraphs(fd):
                                for src_file in raw_pkg["Checksums-Sha256"]:
                                    if not files.get(src_file['sha256'], None):
                                        db_file = DBtempfile(
                                            sha256=src_file['sha256'],
                                            size=int(src_file['size']),
                                            name=src_file['name'],
                                            archive_name=archive,
                                            path="/" + raw_pkg['Directory'],
                                            timestamp_value=parsed_ts
                                        )
                                        files[src_file['sha256']] = db_file
                                    db_srcpkg = DBtempsrcpkg(
                                        name=raw_pkg['Package'],
                                        version=raw_pkg['Version'],
                                        file_sha256=src_file["sha256"])
                                    to_add.append(db_srcpkg)
                        else:
                            for raw_pkg in debian.deb822.Packages.iter_paragraphs(fd):
                                if not files.get(raw_pkg['SHA256'], None):
                                    db_file = DBtempfile(
                                        sha256=raw_pkg['SHA256'],
                                        size=int(raw_pkg['Size']),
                                        name=os.path.basename(raw_pkg['Filename']),
                                        archive_name=archive,
                                        path="/" + os.path.dirname(raw_pkg['Filename']),
                                        timestamp_value=parsed_ts
                                    )
                                    files[raw_pkg['SHA256']] = db_file
                                db_binpkg = DBtempbinpkg(
                                    name=raw_pkg['Package'],
                                    version=raw_pkg['Version'],
                                    file_sha256=raw_pkg["SHA256"],
                                    architecture=raw_pkg['Architecture'])
                                to_add.append(db_binpkg)
                    if not db_repodata:
                        to_add.append(DBrepodata(id=repodata_id))

        to_add = list(files.values()) + to_add
        if to_add:
            logger.debug(f"Commit to DB {len(to_add)} items")
            session.execute("ALTER TABLE tempfiles SET UNLOGGED")
            session.execute("ALTER TABLE tempsrcpkg SET UNLOGGED")
            session.execute("ALTER TABLE tempbinpkg SET UNLOGGED")

            session.add_all(to_add)
            session.commit()

            stmt_insert_new_file = """
            INSERT INTO files (sha256, size, name, archive_name, path, first_seen, last_seen)
            SELECT t.sha256, t.size, t.name, t.archive_name, t.path, t.timestamp_value, t.timestamp_value FROM tempfiles t
            ON CONFLICT DO NOTHING
            """
            session.execute(stmt_insert_new_file)
            session.commit()

            stmt_file_update_first_seen = """
            UPDATE files AS f
            SET first_seen= t.min_time
            FROM (SELECT sha256, min(timestamp_value) as min_time FROM tempfiles GROUP BY sha256) AS t
            WHERE f.sha256 = t.sha256 AND (min_time < f.first_seen OR f.first_seen is NULL)
            """
            session.execute(stmt_file_update_first_seen)
            session.commit()

            stmt_file_update_last_seen = """
            UPDATE files AS f
            SET last_seen = t.max_time
            FROM (SELECT sha256, max(timestamp_value) as max_time FROM tempfiles GROUP BY sha256) AS t
            WHERE f.sha256 = t.sha256 AND (max_time > f.last_seen OR f.last_seen is NULL)
            """
            session.execute(stmt_file_update_last_seen)
            session.commit()

            if "source" in arches:
                stmt_insert_new_srcpkg = """
                INSERT INTO srcpkg (name, version)
                SELECT t.name, t.version FROM tempsrcpkg t
                ON CONFLICT DO NOTHING
                """
                session.execute(stmt_insert_new_srcpkg)
                session.commit()

                stmt_append_new_file_to_srcpkg = """
                INSERT INTO srcpkg_files (srcpkg_name, srcpkg_version, file_sha256)
                SELECT t.name, t.version, t.file_sha256 FROM tempsrcpkg t
                ON CONFLICT DO NOTHING
                """
                session.execute(stmt_append_new_file_to_srcpkg)
                session.commit()

            if len([arch for arch in arches if arch != "source"]) > 0:
                stmt_insert_new_binpkg = """
                INSERT INTO binpkg (name, version)
                SELECT t.name, t.version FROM tempbinpkg t
                ON CONFLICT DO NOTHING
                """
                session.execute(stmt_insert_new_binpkg)
                session.commit()

                stmt_append_new_file_to_binpkg = """
                INSERT INTO binpkg_files (binpkg_name, binpkg_version, file_sha256, architecture)
                SELECT t.name, t.version, t.file_sha256, t.architecture FROM tempbinpkg t
                ON CONFLICT DO NOTHING
                """
                session.execute(stmt_append_new_file_to_binpkg)
                session.commit()

        session.close()
        DBtempfile.__table__.drop()
        DBtempsrcpkg.__table__.drop()
        DBtempbinpkg.__table__.drop()

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

    def get_timestamps_from_file(self, archive):
        """
        Get all snapshot.debian.org timestamps from local filesystem
        """
        localfile = f"{os.path.join(self.localdir, 'by-timestamp', archive + '.txt')}"
        try:
            with open(localfile, "r") as fd:
                timestamps = sorted(set(fd.read().rstrip("\n").split("\n")), reverse=True)
        except FileNotFoundError as e:
            raise SnapshotMirrorException(str(e))
        return timestamps

    def get_timestamps(self, archive="debian"):
        """
        Get timestamps to use
        """
        timestamps = []
        logger.debug("Get timestamps to use")
        if self.timestamps:
            if ':' in self.timestamps[0]:
                all_timestamps = self.get_timestamps_from_file(archive)
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

    def get_files(self, archive, timestamp, suite, component, arch, baseurl=SNAPSHOT_DEBIAN):
        """"
        Get a parsed files list from Packages.gz or Sources.gz repository file
        """
        files = {}
        if arch == "source":
            repodata = f"{self.localdir}/archive/{archive}/{timestamp}/dists/{suite}/{component}/{arch}/Sources.gz"
        else:
            repodata = f"{self.localdir}/archive/{archive}/{timestamp}/dists/{suite}/{component}/binary-{arch}/Packages.gz"
        try:
            with open(repodata) as fd:
                if arch == "source":
                    for raw_pkg in debian.deb822.Sources.iter_paragraphs(fd):
                        for src_file in raw_pkg["Checksums-Sha256"]:
                            pkg = File(
                                name=raw_pkg["Package"],
                                version=raw_pkg["Version"],
                                architecture="source",
                                archive=archive,
                                timestamp=timestamp,
                                suite=suite,
                                component=component,
                                size=src_file["size"],
                                sha256=src_file["sha256"],
                                relative_path=f"archive/{archive}/{timestamp}/{raw_pkg['Directory']}/{src_file['name']}",
                                url=[
                                    f"{baseurl}/archive/{archive}/{timestamp}/{raw_pkg['Directory']}/{src_file['name']}"
                                ]
                            )
                            snapshot_debian_hash = self.map_srcpkg_hash.get(os.path.basename(src_file['name']), None)
                            if snapshot_debian_hash:
                                file_url = f"{SNAPSHOT_DEBIAN}/file/{snapshot_debian_hash}"
                                pkg.url.insert(0, file_url)
                            files[src_file["sha256"]] = pkg
                else:
                    for raw_pkg in debian.deb822.Packages.iter_paragraphs(fd):
                        pkg = File(
                            name=raw_pkg["Package"],
                            version=raw_pkg["Version"],
                            architecture=raw_pkg["Architecture"],
                            archive=archive,
                            timestamp=timestamp,
                            suite=suite,
                            component=component,
                            size=raw_pkg['Size'],
                            sha256=raw_pkg["SHA256"],
                            relative_path=f"archive/{archive}/{timestamp}/{raw_pkg['Filename']}",
                            url=[
                                f"{baseurl}/archive/{archive}/{timestamp}/{raw_pkg['Filename']}"
                            ],
                        )
                        snapshot_debian_hash = self.map_binpkg_hash.get(os.path.basename(raw_pkg['Filename']), None)
                        if snapshot_debian_hash:
                            file_url = f"{SNAPSHOT_DEBIAN}/file/{snapshot_debian_hash}"
                            pkg.url.insert(0, file_url)
                        files[raw_pkg["SHA256"]] = pkg
        except Exception as e:
            logger.error(str(e))
        return files

    def download(self, fname, url, sha256=None, size=None, no_clean=False):
        """
        Download function to store file according to its SHA256
        """
        # If SHA256 sum is given (DEB files) we use it else we compute it
        # after download (Packages.gz, i18n etc.).
        if sha256:
            fname_sha256 = f"{self.localdir}/by-hash/SHA256/{sha256}"
            already_downloaded = False
            if os.path.exists(fname_sha256):
                already_downloaded = True
            if not already_downloaded:
                try:
                    # For file less than MAX_DIRECT_DOWNLOAD_SIZE we do a direct download
                    if size and int(size) <= MAX_DIRECT_DOWNLOAD_SIZE * 1000 * 1000:
                        download_with_retry(url, fname_sha256, sha256=sha256, no_clean=no_clean)
                    else:
                        download_with_retry_and_resume(url, fname_sha256, sha256=sha256, no_clean=no_clean)
                except Exception as e:
                    raise SnapshotMirrorException(f"Failed to download file: {str(e)}")
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

    def download_repodata(self, archive, timestamp, suite, component, arch, baseurl=SNAPSHOT_DEBIAN, force=False):
        """
        Download Packages.gz or Sources.gz
        """
        if arch == "source":
            repodata = f"{arch}/Sources.gz"
        else:
            repodata = f"binary-{arch}/Packages.gz"
        f = f"/archive/{archive}/{timestamp}/dists/{suite}/{component}/{repodata}"
        localfile = self.localdir + f
        remotefile = f"{baseurl}{f}"
        logger.debug(remotefile)
        if not url_exists(remotefile):
            logger.error(f"Cannot find {remotefile}")
            raise SnapshotMirrorRepodataNotFoundException(f)
        if os.path.exists(localfile) and force:
            os.remove(localfile)
        self.download(localfile, remotefile)

    def download_release(self, archive, timestamp, suite, component, arch, baseurl=SNAPSHOT_DEBIAN, force=False):
        """
        Download repository Release files and translation
        """
        if arch != "source":
            arch = f"binary-{arch}"
        metadata_files = [
            f"/archive/{archive}/{timestamp}/dists/{suite}/Release",
            f"/archive/{archive}/{timestamp}/dists/{suite}/Release.gpg",
            f"/archive/{archive}/{timestamp}/dists/{suite}/InRelease",
            f"/archive/{archive}/{timestamp}/dists/{suite}/{component}/{arch}/Release"
        ]
        for f in metadata_files:
            localfile = self.localdir + f
            remotefile = f"{baseurl}{f}"
            logger.debug(remotefile)
            if not url_exists(remotefile):
                logger.error(f"Cannot find {remotefile}")
                continue
            if os.path.exists(localfile) and force:
                os.remove(localfile)
            self.download(localfile, remotefile)

    def download_translation(self, archive, timestamp, suite, component):
        """
        Download repository Translation files
        """
        translation_files = [
            f"/archive/{archive}/{timestamp}/dists/{suite}/{component}/i18n/Translation-en.bz2"
        ]
        for f in translation_files:
            localfile = self.localdir + f
            remotefile = f"{SNAPSHOT_DEBIAN}{f}"
            logger.debug(remotefile)
            if not url_exists(remotefile):
                logger.error(f"Cannot find {remotefile}")
                continue
            if os.path.exists(localfile):
                continue
            self.download(localfile, remotefile)

    def download_file(self, file, check_only, no_clean):
        logger.info(file)
        if check_only:
            fname_sha256 = f"{self.localdir}/by-hash/SHA256/{file.sha256}"
            if not os.path.exists(fname_sha256):
                logger.info(f"MISSING: {file}")
                return
            if sha256sum(fname_sha256) != file.sha256:
                raise SnapshotMirrorException(
                    f"Wrong SHA256 for: {fname_sha256}")
        else:
            result = None
            localfile = f"{self.localdir}/{file.relative_path}"
            for url in file.url:
                try:
                    # logger.debug(f"Try with URL ({url})")
                    result = self.download(localfile, url, file.sha256, size=file.size, no_clean=no_clean)
                    break
                except Exception as e:
                    logger.debug(f"Retry with another URL ({str(e)})")
            if not result:
                raise SnapshotMirrorException("No more URL to try")

    def run(self, check_only=False, no_clean=False, provision_db=False, provision_db_only=False, ignore_provisioned=False):
        """
        Run the snapshot mirroring on all the archives, timestamps, suites,
        components and architectures
        """
        os.makedirs(f"{self.localdir}/by-hash/SHA256", exist_ok=True)
        archives = set(self.archives).intersection(DEBIAN_ARCHIVES)
        for archive in archives:
            timestamps = self.get_timestamps(archive)
            if not provision_db_only:
                for timestamp in timestamps:
                    # Download repository metadata and translation
                    files = {}
                    for suite in self.suites:
                        for component in self.components:
                            self.download_translation(archive, timestamp, suite, component)
                            for arch in self.architectures:
                                try:
                                    self.download_repodata(archive, timestamp, suite, component, arch)
                                except SnapshotMirrorRepodataNotFoundException:
                                    continue
                                files.update(self.get_files(archive, timestamp, suite, component, arch))

                    # Download repository files
                    for file in sorted(files.values(), key=lambda x: x.name):
                        self.download_file(file, check_only=check_only, no_clean=no_clean)

                    # We download Release files at the end to ack
                    # the mirror sync. It is for helping rebuilders
                    # checking available mirrors.
                    for suite in self.suites:
                        for component in self.components:
                            for arch in self.architectures:
                                try:
                                    self.download_release(archive, timestamp, suite, component, arch)
                                except SnapshotMirrorRepodataNotFoundException:
                                    continue
            if provision_db:
                for timestamp in timestamps:
                    self.provision_database(archive, timestamp, self.suites, self.components, self.architectures, ignore_provisioned=ignore_provisioned)

    def run_qubes(self, check_only=False, no_clean=False, provision_db=False, provision_db_only=False):
        """
        Run the mirroring of Qubes all-versions repository
        """
        os.makedirs(f"{self.localdir}/by-hash/SHA256", exist_ok=True)
        archives = set(self.archives).intersection(QUBES_ARCHIVES)
        timestamps = ["99990101T000000Z"]
        suites = ["bullseye"]
        components = ["main"]
        architectures = ["amd64", "source"]

        for archive in archives:
            for timestamp in timestamps:
                if not provision_db_only:
                    files = {}
                    for suite in suites:
                        for component in components:
                            for arch in architectures:
                                self.download_repodata(archive, timestamp, suite, component, arch, baseurl=SNAPSHOT_QUBES, force=True)
                                files.update(self.get_files(archive, timestamp, suite, component, arch, baseurl=SNAPSHOT_QUBES))
                                self.download_release(archive, timestamp, suite, component, arch, baseurl=SNAPSHOT_QUBES, force=True)

                    # Download repository files
                    for file in sorted(files.values(), key=lambda x: x.name):
                        self.download_file(file, check_only=check_only, no_clean=no_clean)
            # Provision database
            if provision_db:
                self.provision_database(archive, timestamps[0], suites, components, architectures, ignore_provisioned=True)

    def init_snapshot_db_hash(self):
        if os.path.exists("/home/user/db/map_srcpkg_hash.csv") and os.path.exists("/home/user/db/map_binpkg_hash.csv"):
            import csv
            with open('/home/user/db/map_srcpkg_hash.csv', newline='') as fd:
                for row in csv.reader(fd, delimiter=',', quotechar='|'):
                    self.map_srcpkg_hash[row[0]] = row[1]
            with open('/home/user/db/map_binpkg_hash.csv', newline='') as fd:
                for row in csv.reader(fd, delimiter=',', quotechar='|'):
                    self.map_binpkg_hash[row[0]] = row[1]


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
        help="Check downloaded files only.",
    )
    parser.add_argument(
        "--provision-db",
        action="store_true",
        help="Provision database.",
    )
    parser.add_argument(
        "--provision-db-only",
        action="store_true",
        help="Provision database only.",
    )
    parser.add_argument(
        "--ignore-provisioned",
        action="store_true",
        help="Ignore already provisioned repodata.",
    )
    parser.add_argument(
        "--no-clean-part-file",
        action="store_true",
        help="No clean partially downloaded files.",
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
        if not args.provision_db_only:
            cli.init_snapshot_db_hash()
        # Debian: snapshot.debian.org
        cli.run(
            check_only=args.check_only,
            no_clean=args.no_clean_part_file,
            provision_db=args.provision_db,
            provision_db_only=args.provision_db_only,
            ignore_provisioned=args.ignore_provisioned
        )
        # QubesOS: deb.qubes-os.org/all-versions
        cli.run_qubes(
            check_only=args.check_only,
            no_clean=args.no_clean_part_file,
            provision_db_only=args.provision_db_only,
            provision_db=args.provision_db
        )
    except (ValueError, SnapshotMirrorException, KeyboardInterrupt) as e:
        logger.error(str(e))
        return 1


if __name__ == "__main__":
    sys.exit(main())
