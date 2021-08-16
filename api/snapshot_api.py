#!flask/bin/python3
# -*- encoding: utf8 -*-
#
# Copyright (C) 2021 Frédéric Pierret <frederic.pierret@qubes-os.org>
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

import json
import logging
import debian.deb822

from operator import itemgetter
from flask import request, Flask, Response
from flask_caching import Cache
from flask_sqlalchemy import SQLAlchemy
from dateutil.parser import parse as parsedate
from db import DBarchive, DBtimestamp, DBfile, DBsrcpkg, DBbinpkg, \
    FilesLocations, BinpkgFiles, DATABASE_URI

# flask app
app = Flask("DebianSnapshotApi")
app.config['MAX_CONTENT_LENGTH'] = 4 * 1024 * 1024

# logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# flask cache
cache = Cache(app, config={'CACHE_TYPE': 'simple'})

# flask sqlalchemy
app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URI
db = SQLAlchemy(app)

API_VERSION = "0.3"


class SnapshotException(Exception):
    pass


class SnapshotEmptyQueryException(SnapshotException):
    pass


def file_desc(file):
    locations = []
    for raw_location in db.session.query(FilesLocations).filter_by(file_sha256=file.sha256):
        timestamp_ranges = []
        for rg in raw_location[4]:
            timestamp_ranges.append(
                (parsedate(rg[0]).strftime("%Y%m%dT%H%M%SZ"),
                 parsedate(rg[-1]).strftime("%Y%m%dT%H%M%SZ"))
            )
        location = {
            "name": file.name,
            "path": file.path,
            "size": file.size,

            "archive_name": raw_location[1],
            "suite_name": raw_location[2],
            "component_name": raw_location[3],
            "timestamp_ranges": timestamp_ranges
        }
        locations.append(location)
    return locations


@app.route("/mr/timestamp/<string:archive_name>", methods=["GET"])
# @cache.cached(timeout=86400)
def timestamps(archive_name):
    api_result = {"_api": API_VERSION, "_comment": "notset"}
    try:
        timestamps = db.session.query(DBarchive).get(archive_name).timestamps
        if not list(timestamps):
            raise SnapshotEmptyQueryException
        status_code = 200
        api_result.update({
            "result": sorted([parsedate(ts.value).strftime("%Y%m%dT%H%M%SZ")
                              for ts in timestamps]),
        })
    except SnapshotEmptyQueryException:
        status_code = 404
    except Exception as e:
        logger.error(str(e))
        status_code = 500
    api_result = json.dumps(api_result, indent=2) + "\n"
    return Response(api_result, status=status_code, mimetype="application/json")


@app.route("/mr/file", methods=["GET"])
# @cache.cached(timeout=86400)
def files():
    api_result = {"_api": API_VERSION, "_comment": "notset"}
    try:
        files = db.session.query(DBfile).order_by(DBfile.name)
        if not list(files):
            raise SnapshotEmptyQueryException
        status_code = 200
        api_result.update({
            "result": [{"file": file.name} for file in files],
        })
    except SnapshotEmptyQueryException:
        status_code = 404
    except Exception as e:
        logger.error(str(e))
        status_code = 500
    api_result = json.dumps(api_result, indent=2) + "\n"
    return Response(api_result, status=status_code, mimetype="application/json")


@app.route("/mr/file/<string:file_hash>/info", methods=["GET"])
# @cache.cached(timeout=86400)
def file_info(file_hash):
    api_result = {"_api": API_VERSION, "_comment": "notset"}
    try:
        # we have only one file because we use sha256 as hash
        # compared to snapshot.d.o
        file = db.session.query(DBfile).get(file_hash)
        if not file:
            raise SnapshotEmptyQueryException
        status_code = 200
        api_result.update({
            "result": file_desc(file),
        })
    except SnapshotEmptyQueryException:
        status_code = 404
    except Exception as e:
        logger.error(str(e))
        status_code = 500
    api_result = json.dumps(api_result, indent=2) + "\n"
    return Response(api_result, status=status_code, mimetype="application/json")


@app.route("/mr/package", methods=["GET"])
# @cache.cached(timeout=86400)
def packages():
    api_result = {"_api": API_VERSION, "_comment": "notset"}
    try:
        packages = db.session.query(DBsrcpkg).order_by(DBsrcpkg.name)
        if not list(packages):
            raise SnapshotEmptyQueryException
        status_code = 200
        filtered_packages = sorted(set([pkg.name for pkg in packages]))
        api_result.update({
            "result": [{"package": pkg} for pkg in filtered_packages],
        })
    except SnapshotEmptyQueryException:
        status_code = 404
    except Exception as e:
        logger.error(str(e))
        status_code = 500
    api_result = json.dumps(api_result, indent=2) + "\n"
    return Response(api_result, status=status_code, mimetype="application/json")


@app.route("/mr/package/<string:srcpkgname>", methods=["GET"])
# @cache.cached(timeout=86400)
def package(srcpkgname):
    api_result = {"_api": API_VERSION, "_comment": "notset"}
    try:
        packages = db.session.query(DBsrcpkg).filter_by(name=srcpkgname)
        if not list(packages):
            raise SnapshotEmptyQueryException
        status_code = 200
        api_result.update({
            "package": srcpkgname,
            "result": [{"version": pkg.version} for pkg in packages],
        })
    except SnapshotEmptyQueryException:
        status_code = 404
    except Exception as e:
        logger.error(str(e))
        status_code = 500
    api_result = json.dumps(api_result, indent=2) + "\n"
    return Response(api_result, status=status_code, mimetype="application/json")


@app.route("/mr/package/<string:srcpkgname>/<string:srcpkgver>/srcfiles", methods=["GET"])
# @cache.cached(timeout=86400)
def srcfiles(srcpkgname, srcpkgver):
    api_result = {"_api": API_VERSION, "_comment": "notset"}
    fileinfo = request.args.get('fileinfo')
    try:
        package = db.session.query(DBsrcpkg).filter_by(name=srcpkgname, version=srcpkgver).first()
        if not package:
            raise SnapshotEmptyQueryException
        status_code = 200
        api_result.update({
            "package": srcpkgname,
            "version": srcpkgver,
            "result": [{"hash": file.sha256} for file in package.files],
        })
        if fileinfo == "1":
            api_result["fileinfo"] = {}
            for file in package.files:
                api_result["fileinfo"][file.sha256] = file_desc(file)
    except SnapshotEmptyQueryException:
        status_code = 404
    except Exception as e:
        logger.error(str(e))
        status_code = 500
    api_result = json.dumps(api_result, indent=2) + "\n"
    return Response(api_result, status=status_code, mimetype="application/json")


@app.route("/mr/binary/<string:pkg_name>", methods=["GET"])
# @cache.cached(timeout=86400)
def binary(pkg_name):
    api_result = {"_api": API_VERSION, "_comment": "notset"}
    try:
        binpackages = db.session.query(DBbinpkg).filter_by(name=pkg_name)
        if not list(binpackages):
            raise SnapshotEmptyQueryException
        status_code = 200
        api_result.update({
            "binary": pkg_name,
            "result": [
                {
                    "name": binpkg.name,
                    "binary_version": binpkg.version
                } for binpkg in binpackages
            ],
        })
    except SnapshotEmptyQueryException:
        status_code = 404
    except Exception as e:
        logger.error(str(e))
        status_code = 500
    api_result = json.dumps(api_result, indent=2) + "\n"
    return Response(api_result, status=status_code, mimetype="application/json")


@app.route("/mr/binary/<string:pkg_name>/<string:pkg_ver>/binfiles", methods=["GET"])
# @cache.cached(timeout=86400)
def binfiles(pkg_name, pkg_ver):
    api_result = {"_api": API_VERSION, "_comment": "notset"}
    fileinfo = request.args.get('fileinfo')
    try:
        binpackage = db.session.query(DBbinpkg).filter_by(name=pkg_name, version=pkg_ver).first()
        if not binpackage:
            raise SnapshotEmptyQueryException
        status_code = 200
        api_result.update({
            "binary_version": pkg_ver,
            "binary": pkg_name,
            "result": [
                {
                    "hash": associated_file.file_sha256,
                    "architecture": associated_file.architecture
                } for associated_file in binpackage.files
            ],
        })
        if fileinfo == "1":
            api_result["fileinfo"] = {}
            for associated_file in binpackage.files:
                file = associated_file.file
                api_result["fileinfo"][file.sha256] = file_desc(file)
    except SnapshotEmptyQueryException:
        status_code = 404
    except Exception as e:
        logger.error(str(e))
        status_code = 500
    api_result = json.dumps(api_result, indent=2) + "\n"
    return Response(api_result, status=status_code, mimetype="application/json")


@app.route('/mr/buildinfo', methods=['POST'])
def upload_buildinfo():
    api_result = {
        "_api": API_VERSION,
        "_comment": "notset: This feature is currently very experimental!"
    }
    status_code = 200
    try:
        assert request.content_type.startswith("multipart/form-data;")
        assert request.form.get("buildinfo")
        buildinfo_file = request.form['buildinfo']
        parsed_info = debian.deb822.BuildInfo(buildinfo_file)

        ranges = {}
        not_found = []
        installed = parsed_info.relations['installed-build-depends']
        for dep in installed:
            name = dep[0]['name']
            _, version = dep[0]['version']
            arch = dep[0]['arch'] or parsed_info['Build-Architecture']
            results = db.session.query(BinpkgFiles.architecture, FilesLocations)\
                .join(BinpkgFiles, BinpkgFiles.file_sha256 == FilesLocations.c.file_sha256)\
                .filter_by(binpkg_name=name, binpkg_version=version).all()
            if len(results) == 0:
                not_found.append((name, version, arch))
                continue
            for r in results:
                architecture, _, archive_name, suite_name, component_name, timestamp_ranges = r
                if architecture not in ("all", arch):
                    not_found.append(f"{name}:{arch}={version}")
                    break
                requested_suite_name = request.args.get('suite_name')
                if requested_suite_name and suite_name != requested_suite_name:
                    continue
                begin, end = r.timestamp_ranges[0]
                location = f"{archive_name}:{suite_name}:{component_name}:{arch}"
                ranges.setdefault(location, []).append(
                    (parsedate(begin).strftime("%Y%m%dT%H%M%SZ"),
                     parsedate(end).strftime("%Y%m%dT%H%M%SZ"))
                )

            if not_found:
                api_result["results"] = not_found
                status_code = 404
            else:
                results = []
                for loc in ranges:
                    # Adapted from https://salsa.debian.org/josch/metasnap/-/blob/master/cgi-bin/api#L390
                    # This algorithm is similar to Interval Scheduling
                    # https://en.wikipedia.org/wiki/Interval_scheduling
                    # But instead of returning the ranges, we return the endtime of ranges
                    # See also:
                    # https://stackoverflow.com/questions/27753830/
                    # https://stackoverflow.com/questions/4962015/
                    # https://cs.stackexchange.com/questions/66376/
                    # https://stackoverflow.com/questions/52137509/
                    # https://www.codechef.com/problems/ONEKING
                    # https://discuss.codechef.com/t/oneking-editorial/9096
                    ranges[loc].sort(key=itemgetter(1))
                    res = []
                    last = "19700101T000000Z"  # impossibly early date
                    for b, e in ranges[loc]:
                        if last >= b:
                            continue
                        last = e
                        res.append(last)
                    archive_name, suite_name, component_name, arch = loc.split(":", 4)
                    results.append({
                        "archive_name": archive_name,
                        "suite_name": suite_name,
                        "component_name": component_name,
                        "architecture": arch,
                        "timestamps": res
                    })
                api_result["results"] = results
    except Exception as e:
        logger.error(str(e))
        status_code = 500
    api_result = json.dumps(api_result, indent=2) + "\n"
    return Response(api_result, status=status_code, mimetype="application/json")


if __name__ == "__main__":
    try:
        app.run(debug=True)
    except Exception as e:
        logger.error(str(e))
