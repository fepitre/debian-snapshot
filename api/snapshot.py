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
# from flask_sqlalchemy import SQLAlchemy
from api.db import DBrepodata, DBtimestamp, DBfile, DBsrcpkg, DBbinpkg, db_create_session

# flask app
app = Flask(__name__)

# logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# flask cache
cache = Cache(app, config={'CACHE_TYPE': 'simple'})

API_VERSION = "0"


@app.route("/mr/file", methods=["GET"])
# @cache.cached(timeout=86400)
def files():
    api_result = {"_api": API_VERSION, "_comment": "notset"}
    try:
        session = db_create_session(readonly=True)
        files = session.query(DBfile).order_by(DBfile.name)
        status_code = 200
        api_result.update({
            "result": [{"file": file.name} for file in files],
        })
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
        session = db_create_session(readonly=True)
        # we have only one file because we use sha256 as hash
        # compared to snapshot.d.o
        file = session.query(DBfile).get(file_hash)
        timestamps = sorted([t.value for t in file.timestamps])
        status_code = 200
        api_result.update({
            "result": [{
                "name": file.name,
                "archive_name": file.archive_name,
                "path": file.path,
                "size": file.size,
                "timestamps": timestamps,
                "first_seen": timestamps[0],
                "last_seen": timestamps[-1]
            }],
        })
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
        session = db_create_session(readonly=True)
        packages = session.query(DBsrcpkg).order_by(DBsrcpkg.name)
        status_code = 200
        api_result.update({
            "result": [{"package": pkg.name} for pkg in packages],
        })
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
        session = db_create_session(readonly=True)
        packages = session.query(DBsrcpkg).filter_by(name=srcpkgname)
        status_code = 200
        api_result.update({
            "package": srcpkgname,
            "result": [{"version": pkg.version} for pkg in packages],
        })
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
        session = db_create_session(readonly=True)
        packages = session.query(DBsrcpkg).filter_by(name=srcpkgname, version=srcpkgver).first()
        status_code = 200
        api_result.update({
            "package": srcpkgname,
            "version": srcpkgver,
            "result": [{"hash": file.sha256} for file in packages.files],
        })
        if fileinfo == "1":
            api_result["fileinfo"] = {}
            for file in packages.files:
                timestamps = sorted([t.value for t in file.timestamps])
                api_result["fileinfo"][file.sha256] = [
                    {
                        "name": file.name,
                        "archive_name": file.archive_name,
                        "path": file.path,
                        "size": file.size,
                        "timestamps": timestamps,
                        "first_seen": timestamps[0],
                        "last_seen": timestamps[-1]
                    }
                ]
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
        session = db_create_session(readonly=True)
        binpackages = session.query(DBbinpkg).filter_by(name=pkg_name)
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
        session = db_create_session(readonly=True)
        binpackages = session.query(DBbinpkg).filter_by(name=pkg_name, version=pkg_ver).first()
        status_code = 200
        api_result.update({
            "binary_version": pkg_ver,
            "binary": pkg_name,
            "result": [{"hash": associated_file.file_sha256, "architecture": associated_file.architecture} for associated_file in binpackages.files],
        })
        if fileinfo == "1":
            api_result["fileinfo"] = {}
            for associated_file in binpackages.files:
                file = associated_file.file
                timestamps = sorted([t.value for t in file.timestamps])
                api_result["fileinfo"][file.sha256] = [
                    {
                        "name": file.name,
                        "archive_name": file.archive_name,
                        "path": file.path,
                        "size": file.size,
                        "timestamps": timestamps,
                        "first_seen": timestamps[0],
                        "last_seen": timestamps[-1]
                    }
                ]
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
