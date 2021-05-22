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

from sqlalchemy.engine import Engine
from sqlalchemy import event

from sqlalchemy import Column, Integer, BigInteger, String, Table, ForeignKey, ForeignKeyConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, relationship

# DATABASE_URI = "postgresql://snapshot:snapshot@debian.notset.fr/snapshot"
DATABASE_URI = "postgresql://snapshot:snapshot@127.0.0.1/snapshot"
# DATABASE_URI = 'sqlite:////snapshot/snapshot.db'

engine = create_engine(DATABASE_URI)
Base = declarative_base()
Base.metadata.bind = engine


def db_ro_flush(*args, **kwargs):
    return


def db_create_session(readonly=False):
    Base.metadata.create_all(engine)
    DBSession = sessionmaker(bind=engine)
    session = DBSession()
    if readonly:
        session.flush = db_ro_flush

    return session

# Association tables


SrcpkgFiles = Table(
    'srcpkg_files', Base.metadata,
    Column('srcpkg_name', String, primary_key=True),
    Column('srcpkg_version', String, primary_key=True),
    Column('file_sha256', String, ForeignKey('files.sha256'), primary_key=True),
    ForeignKeyConstraint(
        ('srcpkg_name', 'srcpkg_version'),
        ('srcpkg.name', 'srcpkg.version')),
)


class BinpkgFiles(Base):
    __tablename__ = 'binpkg_files'
    __table_args__ = (
        ForeignKeyConstraint(('binpkg_name', 'binpkg_version'), ('binpkg.name', 'binpkg.version')),
    )
    binpkg_name = Column(String, primary_key=True)
    binpkg_version = Column(String, primary_key=True)
    file_sha256 = Column(String, ForeignKey('files.sha256'), primary_key=True)
    architecture = Column(String, primary_key=True)
    file = relationship("DBfile")


# Main tables


class DBrepodata(Base):
    __tablename__ = 'repodata'
    id = Column(String(40), primary_key=True)

    def __repr__(self):
        return f"<ID {self.id}>"


class DBtimestamp(Base):
    __tablename__ = 'timestamps'
    value = Column(String, primary_key=True)

    def __repr__(self):
        return f"<Timestamp {self.value}>"


class DBfile(Base):
    __tablename__ = 'files'

    sha256 = Column(String(64), primary_key=True)
    size = Column(BigInteger, nullable=False)
    name = Column(String, nullable=False)
    archive_name = Column(String, nullable=False)
    path = Column(String, nullable=False)
    first_seen = Column(String, nullable=False)
    last_seen = Column(String, nullable=False)

    def __repr__(self):
        return f"<File {self.sha256}>"


class DBsrcpkg(Base):
    __tablename__ = 'srcpkg'

    name = Column(String, primary_key=True)
    version = Column(String, primary_key=True)
    files = relationship("DBfile", secondary=SrcpkgFiles)

    def __repr__(self):
        return f"<Package {self.name}-{self.version}>"


class DBbinpkg(Base):
    __tablename__ = 'binpkg'

    name = Column(String, primary_key=True)
    version = Column(String, primary_key=True)
    files = relationship("BinpkgFiles")

    def __repr__(self):
        return f"<Binary {self.name}-{self.version}>"


# Temporary tables for DB provisioning


class DBtempfile(Base):
    __tablename__ = 'tempfiles'

    sha256 = Column(String(64), primary_key=True)
    size = Column(BigInteger, nullable=False)
    name = Column(String, nullable=False)
    archive_name = Column(String, nullable=False)
    path = Column(String, nullable=False)
    timestamp_value = Column(String, nullable=False)

    def __repr__(self):
        return f"<TempFile {self.sha256}>"


class DBtempsrcpkg(Base):
    __tablename__ = 'tempsrcpkg'

    srcpkg_id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    version = Column(String, nullable=False)
    file_sha256 = Column(String, nullable=False)

    def __repr__(self):
        return f"<TempPackage {self.name}-{self.version}>"


class DBtempbinpkg(Base):
    __tablename__ = 'tempbinpkg'

    binpkg_id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    version = Column(String, nullable=False)
    file_sha256 = Column(String, nullable=False)
    architecture = Column(String, nullable=False)

    def __repr__(self):
        return f"<TempBinary {self.name}-{self.version}>"
