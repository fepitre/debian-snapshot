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

import debian.deb822
import hashlib
import logging
import os
import sys
import uuid

from dateutil.parser import parse as parsedate

from lib.log import logger
from lib.common import parse_ts, sha256sum
from lib.exceptions import SnapshotException
from lib.downloads import url_exists, get_file_size, \
    get_response_with_retry, download_with_retry_and_resume_threshold
from lib.timestamps import get_timestamps_from_file

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import func

from db import DBarchive, DBtimestamp, DBrepodata, DBcomponent, DBsuite, DBarchitecture, \
    DBfile, DBsrcpkg, DBbinpkg, DBhash, HashesLocations, ArchivesTimestamps, SrcpkgFiles, BinpkgFiles, \
    db_create_session
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
    def __init__(self, archive, timestamp, path, localfile, remotefiles,
                 size=None, sha256=None):
        self.archive = archive
        self.timestamp = timestamp
        self.path = path
        self.localfile = localfile
        self.remotefiles = remotefiles
        self.size = size
        self.sha256 = sha256

    def __repr__(self):
        return self.to_str()

    def to_str(self):
        path = self.path
        if self.path.startswith('/'):
            path = self.path[1:]
        return f"{self.archive}:{self.timestamp}:{re.sub('[^A-Za-z0-9-_]+', '-', path)}"


class Package(File):
    def __init__(self, name, version, architecture, archive, timestamp, suite, component, size,
                 sha256, path, localfile, remotefiles):
        super().__init__(archive=archive, timestamp=timestamp, path=path, localfile=localfile,
                         remotefiles=remotefiles, size=size, sha256=sha256)
        self.name = name
        self.version = version
        self.architecture = architecture
        self.suite = suite
        self.component = component


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

    @staticmethod
    def get_hashes_from_page(url):
        resp = get_response_with_retry(url)
        hashes = {}
        if resp.ok:
            link_regex = r'<a href=\".+\">(.+)</a> -&gt;\n[ \t]*<a href=\"by-hash/SHA256/.+\">by-hash/SHA256/([0-9a-f]+)</a>\n'
            hashes = dict((x, y) for x, y in re.findall(link_regex, resp.text))
        return hashes

    # This function is useful only if we want to download the whole data from snapshot.d.o
    def init_snapshot_db_hash(self):
        if os.path.exists("/home/user/db/map_srcpkg_hash.csv") and os.path.exists("/home/user/db/map_binpkg_hash.csv"):
            import csv
            with open('/home/user/db/map_srcpkg_hash.csv', newline='') as fd:
                for row in csv.reader(fd, delimiter=',', quotechar='|'):
                    self.map_srcpkg_hash[row[0]] = row[1]
            with open('/home/user/db/map_binpkg_hash.csv', newline='') as fd:
                for row in csv.reader(fd, delimiter=',', quotechar='|'):
                    self.map_binpkg_hash[row[0]] = row[1]

    # Download function
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
                    sha256 = download_with_retry_and_resume_threshold(url, fname_sha256, size=size, sha256=sha256, no_clean=no_clean)
                except Exception as e:
                    raise SnapshotException(f"Failed to download file: {str(e)}")
        else:
            tmp_path = f"{self.localdir}/by-hash/SHA256/{uuid.uuid4()}.tmp"
            sha256 = download_with_retry_and_resume_threshold(url, tmp_path)
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

        return sha256

    # Download a "File"
    def download_file(self, file, check_only=False, no_clean=False):
        logger.info(file)
        if check_only:
            if not file.sha256:
                logger.info(f"No SHA256 info for {file}")
                return
            fname_sha256 = f"{self.localdir}/by-hash/SHA256/{file.sha256}"
            if not os.path.exists(fname_sha256):
                logger.info(f"MISSING: {file}")
                return
            if sha256sum(fname_sha256) != file.sha256:
                raise SnapshotException(
                    f"Wrong SHA256 for {fname_sha256}")
        else:
            size = int(file.size) if file.size is not None else None
            sha256 = file.sha256
            for url in file.remotefiles:
                try:
                    # logger.debug(f"Try with URL ({url})")
                    sha256 = self.download(file.localfile, url, sha256=file.sha256, size=size, no_clean=no_clean, compute_size=size == -1)
                    break
                except Exception as e:
                    logger.debug(f"Retry with another URL ({str(e)})")
            if not sha256:
                raise SnapshotException(f"No more URL to try for {file}")
            return sha256

    # Download "dists" content in a Debian repository
    def download_distfiles(self, archive, timestamp, suites, components, architectures, baseurl=SNAPSHOT_DEBIAN, force=False, provision_db_only=False, download_installer_files=True):
        """
        Download repodata (index, translations, i18n, dep11, etc.)
        """
        distfiles = {}
        for suite in suites:
            distfiles[suite] = {}
            # not component specific
            distfiles[suite]["all"] = {}

            # Release files
            suite_path = f"archive/{archive}/{timestamp}/dists/{suite}"
            release_files = [
                f"Release",
                f"Release.gpg",
                f"InRelease"
            ]
            hashes_suite_path = self.get_hashes_from_page(f"{baseurl}/{suite_path}")
            for f in release_files:
                release_file = File(
                    archive=archive,
                    timestamp=timestamp,
                    path=f"dists/{suite}/{f}",
                    localfile=f"{self.localdir}/{suite_path}/{f}",
                    remotefiles=[f"{baseurl}/{suite_path}/{f}"],
                    sha256=hashes_suite_path.get(f, None)
                )
                distfiles[suite]["all"][release_file.to_str()] = release_file

            for component in components:
                distfiles[suite][component] = []
                files = {}

                # Release component files
                release_files = []
                for arch in architectures:
                    if arch == "source":
                        release_arch_path = "source"
                    else:
                        release_arch_path = f"binary-{arch}"
                    release_files.append(f"{component}/{release_arch_path}/Release")
                for f in release_files:
                    release_file = File(
                        archive=archive,
                        timestamp=timestamp,
                        path=f"dists/{suite}/{f}",
                        localfile=f"{self.localdir}/{suite_path}/{f}",
                        remotefiles=[f"{baseurl}/{suite_path}/{f}"],
                        sha256=hashes_suite_path.get(f, None),
                    )
                    files[release_file.to_str()] = release_file

                # Repository packages
                for arch in architectures:
                    if arch == "source":
                        basepath = f"dists/{suite}/{component}/{arch}"
                        repodata = "Sources.gz"
                    else:
                        basepath = f"dists/{suite}/{component}/binary-{arch}"
                        repodata = "Packages.gz"
                    dist = f"archive/{archive}/{timestamp}/{basepath}"
                    hashes = self.get_hashes_from_page(f"{baseurl}/{dist}")
                    repodata_file = File(
                        archive=archive,
                        timestamp=timestamp,
                        path=f"/{basepath}/{repodata}",
                        localfile=f"{self.localdir}/{dist}/{repodata}",
                        remotefiles=[f"{baseurl}/{dist}/{repodata}"],
                        sha256=hashes.get(repodata, None),
                    )
                    files[repodata_file.to_str()] = repodata_file

                # Translations
                i18n = f"archive/{archive}/{timestamp}/dists/{suite}/{component}/i18n"
                translation_files = [
                    "Translation-en.bz2"
                ]
                hashes = self.get_hashes_from_page(f"{baseurl}/{i18n}")
                for f in translation_files:
                    translation_file = File(
                        archive=archive,
                        timestamp=timestamp,
                        path=f"dists/{suite}/{component}/i18n/{f}",
                        localfile=f"{self.localdir}/{i18n}/{f}",
                        remotefiles=[f"{baseurl}/{i18n}/{f}"],
                        sha256=hashes.get(f, None),
                    )
                    files[translation_file.to_str()] = translation_file

                # Dep11
                dep11 = f"archive/{archive}/{timestamp}/dists/{suite}/{component}/dep11"
                dep11_files = [
                    "icons-48x48.tar.gz",
                    "icons-64x64.tar.gz",
                    "icons-128x128.tar.gz",
                    "icons-48x48@2.tar.gz",
                    "icons-64x64@2.tar.gz",
                    "icons-128x128@2.tar.gz",
                ]
                for arch in architectures:
                    if arch not in ("source", "all"):
                        dep11_files += [
                            f"CID-Index-{arch}.json.gz",
                            f"Components-{arch}.yml.gz"
                        ]
                hashes = self.get_hashes_from_page(f"{baseurl}/{dep11}")
                for f in dep11_files:
                    dep11_file = File(
                        archive=archive,
                        timestamp=timestamp,
                        path=f"dists/{suite}/{component}/dep11/{f}",
                        localfile=f"{self.localdir}/{dep11}/{f}",
                        remotefiles=[f"{baseurl}/{dep11}/{f}"],
                        sha256=hashes.get(f, None),
                    )
                    files[dep11_file.to_str()] = dep11_file

                # installer related content
                for arch in architectures:
                    if arch != "source" and download_installer_files:
                        debian_installer = f"archive/{archive}/{timestamp}/dists/{suite}/{component}/debian-installer/binary-{arch}"
                        hashes = self.get_hashes_from_page(f"{baseurl}/{debian_installer}")
                        for f in ["Packages.gz", "Release"]:
                            installer_file = File(
                                archive=archive,
                                timestamp=timestamp,
                                path=f"{self.localdir}/{debian_installer}/{f}",
                                localfile=f"{self.localdir}/{debian_installer}/{f}",
                                remotefiles=[f"{baseurl}/{debian_installer}/{f}"],
                                sha256=hashes.get(f, None)
                            )
                            files[installer_file.to_str()] = installer_file

                    if arch not in ("source", "all"):
                        parsed_files = {}
                        installer = f"archive/{archive}/{timestamp}/dists/{suite}/{component}/installer-{arch}"
                        installer_localfile_sha256sums = f"{self.localdir}/{installer}/current/images/SHA256SUMS"
                        installer_remote_sha256sums = f"{SNAPSHOT_DEBIAN}/{installer}/current/images/SHA256SUMS"
                        if not os.path.exists(installer_localfile_sha256sums):
                            if not url_exists(installer_remote_sha256sums):
                                logger.error(f"Cannot find {installer_remote_sha256sums}")
                                continue
                            # We download it before
                            shasums_sha256 = self.download(installer_localfile_sha256sums, installer_remote_sha256sums)
                            shasums_file = File(
                                archive=archive,
                                timestamp=timestamp,
                                path=f"dists/{suite}/{component}/installer-{arch}/current/images/SHA256SUMS",
                                localfile=installer_localfile_sha256sums,
                                remotefiles=[installer_remote_sha256sums],
                                sha256=shasums_sha256,
                                size=os.path.getsize(installer_localfile_sha256sums)
                            )
                            files[shasums_file.to_str()] = shasums_file
                        with open(installer_localfile_sha256sums, 'r') as fd:
                            for f in fd.readlines():
                                key, val = f.split()
                                parsed_files.setdefault(key, []).append(val[2:])
                        for sha256, installer_files in parsed_files.items():
                            for f in installer_files:
                                installer_file = File(
                                    archive=archive,
                                    timestamp=timestamp,
                                    path=f"dists/{suite}/{component}/installer-{arch}/current/images/{f}",
                                    localfile=f"{self.localdir}/{installer}/current/images/{f}",
                                    remotefiles=[
                                        f"{FTP_DEBIAN}/{archive}/dists/{suite}/{component}/installer-{arch}/current/images/{f}",
                                        f"{baseurl}/{installer}/current/images/{f}"
                                    ],
                                    sha256=sha256,
                                    size=-1
                                )
                                files[installer_file.to_str()] = installer_file

                if not provision_db_only:
                    # Download repository files
                    for file in files.values():
                        logger.debug(file.remotefiles[0])
                        if os.path.exists(file.localfile) and force:
                            os.remove(file.localfile)
                        # e.g. a suite may not exist depending of the timestamps
                        if not url_exists(file.remotefiles[0]):
                            logger.error(f"Cannot find {file.remotefiles[0]}")
                            continue
                        # update sha256 from downloaded file
                        # this is for storing value in DB
                        file.sha256 = self.download_file(file)
                        distfiles[suite][component].append(file)
        return distfiles

    # Download "pool" content in a Debian repository
    def download_poolfiles(self, archive, timestamp, suites, components, architectures, baseurl=SNAPSHOT_DEBIAN, provision_db_only=False):
        """"
        Download parsed files from Packages.gz or Sources.gz repository file
        """
        poolfiles = {}
        for suite in suites:
            poolfiles[suite] = {}
            for component in components:
                poolfiles[suite][component] = {}
                for arch in architectures:
                    poolfiles[suite][component][arch] = []
                    if arch == "source":
                        repodata_list = [f"{self.localdir}/archive/{archive}/{timestamp}/dists/{suite}/{component}/{arch}/Sources.gz"]
                    else:
                        # TODO: We currently ignore Packages.gz for debian-installer until we rework
                        # the service from scratch (again) with a better approach to store files in
                        # DB like original snapshot.d.o.
                        repodata_list = [
                            f"{self.localdir}/archive/{archive}/{timestamp}/dists/{suite}/{component}/binary-{arch}/Packages.gz",
                            # f"{self.localdir}/archive/{archive}/{timestamp}/dists/{suite}/{component}/debian-installer/binary-{arch}/Packages.gz"
                        ]
                    try:
                        for repodata in repodata_list:
                            with open(repodata) as fd:
                                if arch == "source":
                                    for raw_pkg in debian.deb822.Sources.iter_paragraphs(fd):
                                        for src_file in raw_pkg["Checksums-Sha256"]:
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
                                                path=f"{raw_pkg['Directory']}/{src_file['name']}",
                                                localfile=f"{self.localdir}/archive/{archive}/{timestamp}/{raw_pkg['Directory']}/{src_file['name']}",
                                                remotefiles=[
                                                    # f"{FTP_DEBIAN}/{archive}/{raw_pkg['Directory']}/{src_file['name']}",
                                                    f"{baseurl}/archive/{archive}/{timestamp}/{raw_pkg['Directory']}/{src_file['name']}"
                                                ]
                                            )
                                            snapshot_debian_hash = self.map_srcpkg_hash.get(os.path.basename(src_file['name']), None)
                                            if snapshot_debian_hash:
                                                file_url = f"{SNAPSHOT_DEBIAN}/file/{snapshot_debian_hash}"
                                                pkg.remotefiles.insert(0, file_url)
                                            # poolfiles[suite][component][arch].setdefault(src_file["sha256"], [])
                                            poolfiles[suite][component][arch].append(pkg)
                                else:
                                    for raw_pkg in debian.deb822.Packages.iter_paragraphs(fd):
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
                                            path=f"{raw_pkg['Filename']}",
                                            localfile=f"{self.localdir}/archive/{archive}/{timestamp}/{raw_pkg['Filename']}",
                                            remotefiles=[
                                                # f"{FTP_DEBIAN}/{archive}/{raw_pkg['Filename']}",
                                                f"{baseurl}/archive/{archive}/{timestamp}/{raw_pkg['Filename']}"
                                            ],
                                        )
                                        snapshot_debian_hash = self.map_binpkg_hash.get(os.path.basename(raw_pkg['Filename']), None)
                                        if snapshot_debian_hash:
                                            file_url = f"{SNAPSHOT_DEBIAN}/file/{snapshot_debian_hash}"
                                            pkg.remotefiles.insert(0, file_url)
                                        poolfiles[suite][component][arch].append(pkg)
                    except Exception as e:
                        logger.error(str(e))

        if not provision_db_only:
            # We separate the download part from parse part
            for suite in suites:
                for component in components:
                    for arch in architectures:
                        for file in poolfiles[suite][component][arch]:
                            self.download_file(file)
        return poolfiles

    def provision_database(self, archive, timestamp, suites, components, architectures,
                           poolfiles, ignore_provisioned=False):
        session = db_create_session()
        to_add = []
        to_add_suites = {}
        to_add_components = {}
        to_add_architectures = {}
        to_add_hashes = {}
        to_add_files = {}
        to_add_srcpkg = {}
        to_add_binpkg = {}

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
                    for arch in architectures:
                        db_architecture = session.query(DBarchitecture).get(arch)
                        if not db_architecture and not to_add_architectures.get(arch, None):
                            db_architecture = DBarchitecture(name=arch)
                            to_add_architectures[arch] = db_architecture

                        repodata = f"archive/{archive}/{timestamp}/dists/{suite}/{component}/{arch}"
                        logger.debug(f"Processing {repodata}")
                        # Check if we already provisioned DB with
                        repodata_id = hashlib.sha1(repodata.encode()).hexdigest()
                        db_repodata = session.query(DBrepodata).get(repodata_id)
                        if not ignore_provisioned and db_repodata:
                            session.close()
                            continue

                        for f in poolfiles[suite][component][arch]:
                            hash_ref = f.sha256 + f.suite + f.component
                            if not to_add_hashes.get(hash_ref, None):
                                to_add_hashes[hash_ref] = {
                                        "sha256": f.sha256,
                                        "archive_name": archive,
                                        "timestamp_value": parsed_ts,
                                        "suite_name": suite,
                                        "component_name": component
                                }
                            file_ref = f.sha256 + f.name + f.path
                            if not to_add_files.get(file_ref, None):
                                to_add_files[file_ref] = {
                                        "sha256": f.sha256,
                                        "name": f.name,
                                        "size": f.size,
                                        "path": f.path,
                                }
                            package_ref = f.sha256 + f.name + f.version + arch
                            if arch == "source":
                                if not to_add_srcpkg.get(package_ref, None):
                                    db_pkg = {
                                        "name": f.name,
                                        "version": f.version,
                                        "sha256": f.sha256
                                    }
                                    to_add_srcpkg[package_ref] = db_pkg
                            else:
                                if not to_add_binpkg.get(package_ref, None):
                                    db_pkg = {
                                        "name": f.name,
                                        "version": f.version,
                                        "sha256": f.sha256,
                                        "architecture": f.architecture
                                    }
                                    to_add_binpkg[package_ref] = db_pkg

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
            to_add_hashes = list(to_add_hashes.values())
            to_add_files = list(to_add_files.values())
            to_add_srcpkg = list(to_add_srcpkg.values())
            to_add_binpkg = list(to_add_binpkg.values())

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

            stmt = insert(ArchivesTimestamps).values(
                [{"archive_name": archive, "timestamp_value": parsed_ts}]
            )
            stmt = stmt.on_conflict_do_nothing()
            session.execute(stmt)
            session.commit()

            if to_add_hashes:
                logger.debug(f"Commit to DBhash: {len(to_add_hashes)}")
                stmt = insert(DBhash).values(
                    [{"sha256": f["sha256"]} for f in to_add_hashes]
                )
                stmt = stmt.on_conflict_do_nothing(index_elements=[DBhash.sha256])
                session.execute(stmt)
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

                    # Create query for getting timestamps
                    query = "SELECT value FROM timestamps ORDER BY value ASC"
                    rv = plpy.execute(query)
                    if rv is not None:
                        all_timestamps = [t["value"] for t in rv]

                    current_timestamp = '{parsed_ts}'
                    previous_timestamp = None
                    try:
                        previous_timestamp = all_timestamps[all_timestamps.index(current_timestamp)-1]
                    except ValueError:
                        pass

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

                    # We review the whole list and ensure that we merge into one range
                    # two successive ranges with the same end/begin.
                    final_ranges = [updated_ranges[0]]
                    for i, current_range in enumerate(updated_ranges[1:]):
                        idx = all_timestamps.index(current_range[1])
                        previous_range = final_ranges[-1]

                        backward_range_expected = all_timestamps[idx-2:idx]
                        backward_range = [previous_range[1], current_range[0]]
                        if backward_range != backward_range_expected:
                            final_ranges.append(current_range)
                        else:
                            final_ranges[-1][1] = current_range[1]

                    return final_ranges
                $$ LANGUAGE plpython3u;
                """
                session.execute(stmt_create_replace_timestamps_ranges)
                session.commit()

                stmt = insert(HashesLocations).values(
                    [
                        {
                            "sha256": f["sha256"],
                            "archive_name": f["archive_name"],
                            "suite_name": f["suite_name"],
                            "component_name": f["component_name"],
                            "timestamp_ranges": [[f["timestamp_value"], f["timestamp_value"]]]
                        } for f in to_add_hashes
                    ]
                )
                stmt = stmt.on_conflict_do_update(
                    index_elements=[
                        HashesLocations.c.sha256,
                        HashesLocations.c.archive_name,
                        HashesLocations.c.suite_name,
                        HashesLocations.c.component_name
                    ],
                    set_=dict(
                        timestamp_ranges=func.get_timestamps_ranges(HashesLocations.c.timestamp_ranges),
                    )
                )
                session.execute(stmt)
                session.commit()

            if to_add_files:
                logger.debug(f"Commit to DBfile: {len(to_add_files)}")
                stmt = insert(DBfile).values(
                    [
                        {
                            "sha256": f["sha256"],
                            "size": f["size"],
                            "name": f["name"],
                            "path": f["path"]
                        } for f in to_add_files
                    ]
                )
                stmt = stmt.on_conflict_do_nothing()
                session.execute(stmt)
                session.commit()

            if to_add_srcpkg:
                logger.debug(f"Commit to DBsrcpkg: {len(to_add_srcpkg)}")
                stmt = insert(DBsrcpkg).values(
                    [
                        {
                            "name": f["name"],
                            "version": f["version"]
                        } for f in to_add_srcpkg
                    ]
                )
                stmt = stmt.on_conflict_do_nothing()
                session.execute(stmt)
                session.commit()

                stmt = insert(SrcpkgFiles).values(
                    [
                        {
                            "srcpkg_name": f["name"],
                            "srcpkg_version": f["version"],
                            "sha256": f["sha256"],
                        } for f in to_add_srcpkg
                    ]
                )
                stmt = stmt.on_conflict_do_nothing()
                session.execute(stmt)
                session.commit()

            if to_add_binpkg:
                logger.debug(f"Commit to DBbinpkg: {len(to_add_binpkg)}")
                stmt = insert(DBbinpkg).values(
                    [
                        {
                            "name": f["name"],
                            "version": f["version"]
                        } for f in to_add_binpkg
                    ]
                )
                stmt = stmt.on_conflict_do_nothing()
                session.execute(stmt)
                session.commit()

                stmt = insert(BinpkgFiles).values(
                    [
                        {
                            "binpkg_name": f["name"],
                            "binpkg_version": f["version"],
                            "sha256": f["sha256"],
                            "architecture": f["architecture"],
                        } for f in to_add_binpkg
                    ]
                )
                stmt = stmt.on_conflict_do_nothing()
                session.execute(stmt)
                session.commit()

        finally:
            session.close()

    def run(self, check_only=False, no_clean=False, provision_db=False, provision_db_only=False, ignore_provisioned=False, download_installer_files=True):
        """
        Run the snapshot mirroring on all the archives, timestamps, suites,
        components and architectures
        """
        os.makedirs(f"{self.localdir}/by-hash/SHA256", exist_ok=True)
        archives = set(self.archives).intersection(DEBIAN_ARCHIVES)
        import time
        for archive in archives:
            timestamps = self.get_timestamps(archive)
            # we provision from past to now for timestamp_ranges array
            for timestamp in reversed(timestamps):
                if not provision_db_only:
                    # Download repository metadata and translation
                    distfiles = self.download_distfiles(archive, timestamp, self.suites, self.components, self.architectures)

                # Download repository packages
                poolfiles = self.download_poolfiles(archive, timestamp, self.suites, self.components, self.architectures, provision_db_only=provision_db_only)

                t0 = time.time()
                if not provision_db:
                    continue
                self.provision_database(archive, timestamp, self.suites, self.components, self.architectures,
                                        poolfiles=poolfiles, ignore_provisioned=ignore_provisioned)
                logger.debug(f"DB provisioned in: {int(time.time()-t0)} seconds")

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
            if not provision_db_only:
                # Download repository metadata and translation
                distfiles = self.download_distfiles(archive, timestamps[0], suites, components, architectures, baseurl=SNAPSHOT_QUBES, force=True)

            # Download repository packages
            poolfiles = self.download_poolfiles(archive, timestamps[0], suites, components, architectures, baseurl=SNAPSHOT_QUBES)

            if provision_db:
                self.provision_database(archive, timestamps[0], suites, components, architectures,
                                        poolfiles=poolfiles, ignore_provisioned=True)


def get_args():
    def formatter(prog):
        return argparse.HelpFormatter(prog, max_help_position=100)
    parser = argparse.ArgumentParser(formatter_class=formatter)
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
        "--skip-installer-files",
        action="store_true",
        help="Skip download of installer files.",
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
            ignore_provisioned=args.ignore_provisioned,
            download_installer_files=not args.skip_installer_files
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
