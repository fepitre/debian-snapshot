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
import re
import requests

import debian.deb822
import hashlib
import logging
import os
import sys
import uuid

from dateutil.parser import parse as parsedate

from lib.log import logger
from lib.common import parse_ts, sha256sum
from lib.exceptions import SnapshotException, SnapshotRepodataNotFoundException
from lib.downloads import url_exists, get_file_size, \
    get_response_with_retry, download_with_retry_and_resume_threshold
from lib.timestamps import get_timestamps_from_file

from db import DBrepodata, DBarchive, DBtimestamp, DBcomponent, DBsuite, \
    DBarchitecture, db_create_session, DBtempfile, DBtempsrcpkg, DBtempbinpkg
#
# Debian
#
SNAPSHOT_DEBIAN = "http://snapshot.debian.org"
FTP_DEBIAN = "https://ftp.debian.org"

# Supported Debian archives
DEBIAN_ARCHIVES = {"debian"}

#
# Qubes
#
SNAPSHOT_QUBES = "https://deb.qubes-os.org/all-versions"

# Supported QubesOS archives
QUBES_ARCHIVES = {"qubes-r4.1-vm"}


class File:
    def __init__(self, name, version, architecture, archive, timestamp, suite, component, size, sha256, relative_path, url):
        self.name = name
        self.version = version
        self.architecture = architecture
        self.archive = archive
        self.timestamp = timestamp
        self.suite = suite
        self.component = component
        self.size = size
        self.sha256 = sha256
        self.relative_path = relative_path
        self.url = url

    def __repr__(self):
        return f"{self.archive}:{self.timestamp}:{os.path.basename(self.relative_path)}"


class SnapshotCli:
    def __init__(self, localdir, archives, timestamps, suites, components, architectures):
        self.localdir = os.path.abspath(os.path.expanduser(localdir))
        self.archives = archives
        self.timestamps = timestamps
        self.suites = suites
        self.components = components
        self.architectures = architectures

        if not os.path.exists(self.localdir):
            raise SnapshotException(f"Cannot find: {self.localdir}")

        self.map_srcpkg_hash = {}
        self.map_binpkg_hash = {}

    def provision_database(self, archive, timestamp, suites, components, arches, ignore_provisioned=False):
        session = db_create_session()
        to_add = []
        to_add_suites = {}
        to_add_components = {}
        to_add_architectures = {}
        to_add_files = {}
        try:
            db_archive = session.query(DBarchive).get(archive)
            if not db_archive:
                db_archive = DBarchive(name=archive)
                to_add.append(db_archive)

            logger.info(f"Provision database for timestamp: {timestamp}")
            # we convert timestamp to a SQL format
            parsed_ts = parsedate(timestamp).strftime("%Y-%m-%dT%H:%M:%SZ")
            db_timestamp = session.query(DBtimestamp).get(parsed_ts)
            if not db_timestamp:
                db_timestamp = DBtimestamp(value=parsed_ts)
                to_add.append(db_timestamp)

            for suite in suites:
                db_suite = session.query(DBsuite).get(suite)
                if not db_suite and not to_add_suites.get(suite, None):
                    db_suite = DBsuite(name=suite)
                    to_add_suites[suite] = db_suite
                for component in components:
                    db_component = session.query(DBcomponent).get(component)
                    if not db_component and not to_add_components.get(component, None):
                        db_component = DBcomponent(name=component)
                        to_add_components[component] = db_component
                    for arch in arches:
                        db_architecture = session.query(DBarchitecture).get(arch)
                        if not db_architecture and not to_add_architectures.get(arch, None):
                            db_architecture = DBarchitecture(name=arch)
                            to_add_architectures[arch] = db_architecture
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
                                        file_ref = src_file['sha256'] + suite + component
                                        if not to_add_files.get(file_ref, None):
                                            db_file = DBtempfile(
                                                sha256=src_file['sha256'],
                                                size=int(src_file['size']),
                                                name=src_file['name'],
                                                path="/" + raw_pkg['Directory'],
                                                archive_name=archive,
                                                timestamp_value=parsed_ts,
                                                suite_name=suite,
                                                component_name=component
                                            )
                                            to_add_files[file_ref] = db_file
                                        db_srcpkg = DBtempsrcpkg(
                                            name=raw_pkg['Package'],
                                            version=raw_pkg['Version'],
                                            file_sha256=src_file["sha256"])
                                        to_add.append(db_srcpkg)
                            else:
                                for raw_pkg in debian.deb822.Packages.iter_paragraphs(fd):
                                    file_ref = raw_pkg['SHA256'] + suite + component
                                    if not to_add_files.get(file_ref, None):
                                        db_file = DBtempfile(
                                            sha256=raw_pkg['SHA256'],
                                            size=int(raw_pkg['Size']),
                                            name=os.path.basename(raw_pkg['Filename']),
                                            path="/" + os.path.dirname(raw_pkg['Filename']),
                                            archive_name=archive,
                                            timestamp_value=parsed_ts,
                                            suite_name=suite,
                                            component_name=component
                                        )
                                        to_add_files[file_ref] = db_file
                                    db_binpkg = DBtempbinpkg(
                                        name=raw_pkg['Package'],
                                        version=raw_pkg['Version'],
                                        file_sha256=raw_pkg["SHA256"],
                                        architecture=raw_pkg['Architecture'])
                                    to_add.append(db_binpkg)
                        if not db_repodata:
                            to_add.append(DBrepodata(id=repodata_id))

            # workaround "all" needed in non-"all" packages
            db_architecture = session.query(DBarchitecture).get("all")
            if not db_architecture and not to_add_architectures.get("all", None):
                db_architecture = DBarchitecture(name="all")
                to_add_architectures["all"] = db_architecture

            to_add_suites = list(to_add_suites.values())
            to_add_components = list(to_add_components.values())
            to_add_architectures = list(to_add_architectures.values())
            to_add_files = list(to_add_files.values())

            if to_add_suites:
                logger.debug(f"Commit to DBsuite: {len(to_add_suites)}")
                session.add_all(to_add_suites)
                session.commit()

            if to_add_components:
                logger.debug(f"Commit to DBcomponent: {len(to_add_components)}")
                session.add_all(to_add_components)
                session.commit()

            if to_add_architectures:
                logger.debug(f"Commit to DBarchitecture: {len(to_add_architectures)}")
                session.add_all(to_add_architectures)
                session.commit()

            if to_add:
                logger.debug(f"Commit to DBarchive, DBtimestamp, DBsrcpkg, DBbinpkg, DBrepodata: {len(to_add)}")
                session.add_all(to_add)
                session.commit()

            stmt_insert_new_timestamp_to_archive = f"""
            INSERT INTO archives_timestamps (archive_name, timestamp_value)
            VALUES ('{archive}', '{parsed_ts}')
            ON CONFLICT DO NOTHING
            """
            session.execute(stmt_insert_new_timestamp_to_archive)
            session.commit()

            # This function is triggered only ON CONFLICT in files_locations
            # table. In consequence, there exists always at least one non-empty
            # array 'ranges' and also at least one previous timestamp with
            # respect to current provisioned one.
            stmt_create_replace_timestamps_ranges = f"""
            CREATE OR REPLACE FUNCTION 
            get_timestamps_ranges (ranges text[])
                RETURNS text[]
            AS $$
                from dateutil.parser import parse as parsedate

                current_timestamp = '{parsed_ts}'
                # Create query for getting previous timestamp with respect
                # to provisioned one 'current_timestamp'
                query = "SELECT value FROM timestamps WHERE value < '"+current_timestamp+"' ORDER BY value DESC LIMIT 1"
                rv = plpy.execute(query)
                previous_timestamp = None
                if rv and rv[0].get("value", None):
                    previous_timestamp = rv[0]["value"]

                # Current timestamp range of the archive
                backward_range = [previous_timestamp, current_timestamp]

                # Check if a file has its latest provisioned timestamp being
                # the previous timestamp in the archive's timestamps. Else,
                # there is a gap (unfortunately, that happens) and the file
                # is missing in previous provisioned timestamps.
                # In this case, we add a singleton range.
                updated_ranges = ranges.copy()
                for i, _ in enumerate(updated_ranges):
                    if parsedate(updated_ranges[i][0]) <= parsedate(current_timestamp) <= parsedate(updated_ranges[i][1]):
                        break
                    elif [updated_ranges[i][1], backward_range[1]] == backward_range:
                        updated_ranges[i][1] = backward_range[1]
                        if i+1 != len(updated_ranges) and updated_ranges[i][1] == updated_ranges[i+1][0]:
                            updated_ranges[i+1][0] = updated_ranges[i][0]
                            updated_ranges.pop(i)
                    elif parsedate(current_timestamp) < parsedate(updated_ranges[i][0]):
                        updated_ranges.insert(i, [current_timestamp, current_timestamp])
                    elif parsedate(updated_ranges[i][1]) < parsedate(current_timestamp) and i+1 == len(updated_ranges):
                        updated_ranges.insert(i+1, [current_timestamp, current_timestamp])
                    else:
                        continue
                    break
                return updated_ranges
            $$ LANGUAGE plpython3u;
            """
            session.execute(stmt_create_replace_timestamps_ranges)
            session.commit()

            if to_add_files:
                logger.debug(f"Commit to DBfile: {len(to_add_files)}")
                session.add_all(to_add_files)
                session.commit()

                session.add_all(to_add_files)
                session.commit()

                stmt_insert_new_file = """
                INSERT INTO files (sha256, size, name, path)
                SELECT t.sha256, t.size, t.name, t.path FROM tempfiles t
                ON CONFLICT DO NOTHING
                """
                session.execute(stmt_insert_new_file)
                session.commit()

                stmt_insert_new_location_to_file = """
                INSERT INTO files_locations (file_sha256, archive_name, suite_name, component_name, timestamp_ranges)
                SELECT t.sha256, t.archive_name, t.suite_name, t.component_name, ARRAY[ARRAY[t.timestamp_value, t.timestamp_value]]
                FROM tempfiles t
                ON CONFLICT (file_sha256, archive_name, suite_name, component_name) DO UPDATE
                SET timestamp_ranges = get_timestamps_ranges(files_locations.timestamp_ranges)
                """
                session.execute(stmt_insert_new_location_to_file)
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
        finally:
            session.close()
            DBtempfile.__table__.drop()
            DBtempsrcpkg.__table__.drop()
            DBtempbinpkg.__table__.drop()

    def get_timestamps(self, archive="debian"):
        """
        Get timestamps to use
        """
        timestamps = []
        logger.debug("Get timestamps to use")
        if self.timestamps:
            if ':' in self.timestamps[0]:
                all_timestamps = get_timestamps_from_file(self.localdir, archive)
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
            timestamps = get_timestamps_from_file(self.localdir, archive)
        return timestamps

    def get_files(self, archive, timestamp, suite, component, arch, baseurl=SNAPSHOT_DEBIAN):
        """"
        Get a parsed files list from Packages.gz or Sources.gz repository file
        """
        files = {}
        if arch == "source":
            repodata_list = [f"{self.localdir}/archive/{archive}/{timestamp}/dists/{suite}/{component}/{arch}/Sources.gz"]
        else:
            repodata_list = [
                f"{self.localdir}/archive/{archive}/{timestamp}/dists/{suite}/{component}/binary-{arch}/Packages.gz",
                f"{self.localdir}/archive/{archive}/{timestamp}/dists/{suite}/{component}/debian-installer/binary-{arch}/Packages.gz"
            ]
        try:
            for repodata in repodata_list:
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

    @staticmethod
    def get_hashes_from_page(url):
        resp = get_response_with_retry(url)
        hashes = {}
        if resp.ok:
            link_regex = r'<a href=\".+\">(.+)</a> -&gt;\n[ \t]*<a href=\"by-hash/SHA256/.+\">by-hash/SHA256/([0-9a-f]+)</a>\n'
            hashes = dict((x, y) for x, y in re.findall(link_regex, resp.text))
        return hashes

    def download(self, fname, url, sha256=None, size=None, no_clean=False, compute_size=False):
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
                    if compute_size:
                        size = get_file_size(url)
                    download_with_retry_and_resume_threshold(url, fname_sha256, size=size, sha256=sha256, no_clean=no_clean)
                except Exception as e:
                    raise SnapshotException(f"Failed to download file: {str(e)}")
        else:
            if os.path.exists(fname):
                return
            tmp_path = f"{self.localdir}/by-hash/SHA256/{uuid.uuid4()}.tmp"
            download_with_retry_and_resume_threshold(url, tmp_path)
            sha256 = sha256sum(tmp_path)
            if not sha256:
                raise SnapshotException(f"Failed to get SHA256: {url}")
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
            raise SnapshotRepodataNotFoundException(f)
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
        base_url = f"archive/{archive}/{timestamp}/dists/{suite}/{component}/i18n"
        translation_files = [
            "Translation-en.bz2"
        ]
        hashes = self.get_hashes_from_page(f"{SNAPSHOT_DEBIAN}/{base_url}")
        for f in translation_files:
            localfile = f"{self.localdir}/{base_url}/{f}"
            remotefile = f"{SNAPSHOT_DEBIAN}/{base_url}/{f}"
            logger.debug(remotefile)
            if not url_exists(remotefile):
                logger.error(f"Cannot find {remotefile}")
                continue
            if os.path.exists(localfile):
                continue
            self.download(localfile, remotefile, sha256=hashes.get(f, None))

    def download_dep11(self, archive, timestamp, suite, component, arches):
        """
        Download dep11 repository files
        """
        base_url = f"archive/{archive}/{timestamp}/dists/{suite}/{component}/dep11"
        files = [
            "icons-48x48.tar.gz",
            "icons-64x64.tar.gz",
            "icons-128x128.tar.gz",
            "icons-48x48@2.tar.gz",
            "icons-64x64@2.tar.gz",
            "icons-128x128@2.tar.gz",
        ]
        for arch in arches:
            if arch not in ("source", "all"):
                files += [
                    f"CID-Index-{arch}.json.gz",
                    f"Components-{arch}.yml.gz"
                ]
        hashes = self.get_hashes_from_page(f"{SNAPSHOT_DEBIAN}/{base_url}")
        for f in files:
            localfile = f"{self.localdir}/{base_url}/{f}"
            remotefile = f"{SNAPSHOT_DEBIAN}/{base_url}/{f}"
            logger.debug(remotefile)
            if not url_exists(remotefile):
                logger.error(f"Cannot find {remotefile}")
                continue
            if os.path.exists(localfile):
                continue
            self.download(localfile, remotefile, sha256=hashes.get(f, None))

    def download_installer(self, archive, timestamp, suite, component, arch):
        """
        Download installer files
        """
        base_url = f"archive/{archive}/{timestamp}/dists/{suite}/{component}"
        files = {}
        if arch != "source":
            repodata_files = ["Packages.gz", "Release"]
            for f in repodata_files:
                localfile = f"{self.localdir}/{base_url}/debian-installer/binary-{arch}/{f}"
                remotefile = f"{SNAPSHOT_DEBIAN}/{base_url}/debian-installer/binary-{arch}/{f}"
                logger.debug(remotefile)
                if not url_exists(remotefile):
                    logger.error(f"Cannot find {remotefile}")
                    continue
                if os.path.exists(localfile):
                    continue
                self.download(localfile, remotefile)
        if arch not in ("source", "all"):
            localfile_sha256sums = f"{self.localdir}/{base_url}/installer-{arch}/current/images/SHA256SUMS"
            remotefile_sha256sums = f"{SNAPSHOT_DEBIAN}/{base_url}/installer-{arch}/current/images/SHA256SUMS"
            if not url_exists(remotefile_sha256sums):
                logger.error(f"Cannot find {remotefile_sha256sums}")
                return
            self.download(localfile_sha256sums, remotefile_sha256sums)
            with open(localfile_sha256sums, 'r') as fd:
                for f in fd.readlines():
                    key, val = f.split()
                    files.setdefault(key, []).append(val[2:])
            for sha256, files in files.items():
                for f in files:
                    # files has the same hash, it is downloading once then creates symlinks
                    localfile = f"{self.localdir}/{base_url}/installer-{arch}/current/images/{f}"
                    urls = [
                        f"https://ftp.debian.org/{archive}/dists/{suite}/{component}/installer-{arch}/current/images/{f}",
                        f"{SNAPSHOT_DEBIAN}/{base_url}/installer-{arch}/current/images/{f}"
                    ]
                    for url in urls:
                        try:
                            logger.debug(url)
                            if os.path.exists(localfile):
                                break
                            if not url_exists(url):
                                logger.error(f"Cannot find {url}")
                                continue
                            self.download(localfile, url, sha256=sha256, compute_size=True)
                            break
                        except Exception as e:
                            logger.debug(f"Retry with another URL ({str(e)})")

    def download_file(self, file, check_only, no_clean):
        logger.info(file)
        if check_only:
            fname_sha256 = f"{self.localdir}/by-hash/SHA256/{file.sha256}"
            if not os.path.exists(fname_sha256):
                logger.info(f"MISSING: {file}")
                return
            if sha256sum(fname_sha256) != file.sha256:
                raise SnapshotException(
                    f"Wrong SHA256 for: {fname_sha256}")
        else:
            result = None
            localfile = f"{self.localdir}/{file.relative_path}"
            size = int(file.size) if file.size is not None else None
            for url in file.url:
                try:
                    # logger.debug(f"Try with URL ({url})")
                    result = self.download(localfile, url, file.sha256, size=size, no_clean=no_clean)
                    break
                except Exception as e:
                    logger.debug(f"Retry with another URL ({str(e)})")
            if not result:
                raise SnapshotException("No more URL to try")

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
                            self.download_dep11(archive, timestamp, suite, component, self.architectures)
                            for arch in self.architectures:
                                try:
                                    self.download_repodata(archive, timestamp, suite, component, arch)
                                    self.download_installer(archive, timestamp, suite, component, arch)
                                except SnapshotRepodataNotFoundException:
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
                                except SnapshotRepodataNotFoundException:
                                    continue
            if provision_db:
                # we provision from past to now for timestamp_ranges array
                for timestamp in reversed(timestamps):
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
        help="Local directory for snapshot.")
    parser.add_argument(
        "--archive",
        help="Debian archive to snapshot. "
             "Default is 'debian' and is the only supported archive right now.",
        action="append",
        default=[],
    )
    parser.add_argument(
        "--suite",
        help="Debian suite to snapshot. Can be used multiple times. "
        "Default is 'unstable'",
        action="append",
        default=[],
    )
    parser.add_argument(
        "--component",
        help="Debian component to snapshot. Default is 'main'",
        action="append",
        default=[],
    )
    parser.add_argument(
        "--arch",
        help="Debian arch to snapshot. Can be used multiple times.",
        action="append",
        default=[],
    )
    parser.add_argument(
        "--timestamp",
        help="Timestamp to use for snapshot. Can be used multiple times. "
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
        logger.error("Please provide local snapshot directory")
        return 1

    if not args.archive:
        args.archive = ["debian"]
    if not args.suite:
        args.suite = ["unstable"]
    if not args.component:
        args.component = ["main"]

    try:
        cli = SnapshotCli(
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
    except (ValueError, SnapshotException, KeyboardInterrupt) as e:
        logger.error(str(e))
        return 1


if __name__ == "__main__":
    sys.exit(main())
