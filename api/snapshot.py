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

from flask import request, Flask, Response
from flask_caching import Cache
from flask_sqlalchemy import SQLAlchemy
from dateutil.parser import parse as parsedate
from db import DBtimestamp, DBfile, DBsrcpkg, DBbinpkg, \
    FirstFilesLocations, LastFilesLocations, DATABASE_URI

# flask app
app = Flask(__name__)

# logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# flask cache
cache = Cache(app, config={'CACHE_TYPE': 'simple'})

# flask sqlalchemy
app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URI
db = SQLAlchemy(app)

API_VERSION = "0.1"


class SnapshotException(Exception):
    pass


class SnapshotEmptyQueryException(SnapshotException):
    pass


def file_desc(file):
    locations = {}
    # first locations
    for raw_location in db.session.query(FirstFilesLocations).filter_by(file_sha256=file.sha256):
        location = {
            "archive_name": raw_location[1],
            "suite_name": raw_location[3],
            "component_name": raw_location[4],
            "first_seen": parsedate(raw_location[2]).strftime("%Y%m%dT%H%M%SZ")
        }
        file_ref = raw_location[0] + raw_location[1] + raw_location[3] + raw_location[4]
        locations[file_ref] = location
    # last locations
    for raw_location in db.session.query(LastFilesLocations).filter_by(file_sha256=file.sha256):
        location = {
            "archive_name": raw_location[1],
            "suite_name": raw_location[3],
            "component_name": raw_location[4],
            "last_seen": parsedate(raw_location[2]).strftime("%Y%m%dT%H%M%SZ"),
        }
        file_ref = raw_location[0] + raw_location[1] + raw_location[3] + raw_location[4]
        if locations.get(file_ref, None):
            locations[file_ref].update(location)
    locations = list(locations.values())
    desc = {
        "name": file.name,
        "path": file.path,
        "size": file.size,
        "locations": locations,

        # TEMP: for retro-compatibility, we keep this field taken from
        # the first location
        "first_seen": locations[0]["first_seen"] if locations else None,
    }
    return desc


@app.route("/mr/timestamp", methods=["GET"])
# @cache.cached(timeout=86400)
def timestamps():
    api_result = {"_api": API_VERSION, "_comment": "notset"}
    try:
        timestamps = db.session.query(DBtimestamp).all()
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
            "result": [file_desc(file)],
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
        api_result.update({
            "result": [{"package": pkg.name} for pkg in packages],
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
                api_result["fileinfo"][file.sha256] = [file_desc(file)]
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
            "result": [{"hash": associated_file.file_sha256, "architecture": associated_file.architecture} for associated_file in binpackage.files],
        })
        if fileinfo == "1":
            api_result["fileinfo"] = {}
            for associated_file in binpackage.files:
                file = associated_file.file
                api_result["fileinfo"][file.sha256] = [file_desc(file)]
    except SnapshotEmptyQueryException:
        status_code = 404
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
