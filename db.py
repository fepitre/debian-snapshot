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

from sqlalchemy import Column, Integer, BigInteger, String, ARRAY, Table, ForeignKey, \
    ForeignKeyConstraint, UniqueConstraint
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


HashesLocations = Table(
    'hashes_locations', Base.metadata,
    Column('sha256', String, ForeignKey('hashes.sha256'), primary_key=True),
    Column('archive_name', String, ForeignKey('archives.name'), primary_key=True),
    Column('suite_name', String, ForeignKey('suites.name'), primary_key=True),
    Column('component_name', String, ForeignKey('components.name'), primary_key=True),
    Column('timestamp_ranges', ARRAY(String), nullable=False)
    # timestamp_ranges is an array of ranges. A range is defined as an array
    # of two representing begin/end interval among of all available timestamps
    # for an archive.
)


ArchivesTimestamps = Table(
    'archives_timestamps', Base.metadata,
    Column('archive_name', String, ForeignKey('archives.name'), primary_key=True),
    Column('timestamp_value', String, ForeignKey('timestamps.value'), primary_key=True),
)


SrcpkgFiles = Table(
    'srcpkg_files', Base.metadata,
    Column('srcpkg_name', String, primary_key=True),
    Column('srcpkg_version', String, primary_key=True),
    Column('sha256', String, ForeignKey('hashes.sha256'), primary_key=True),
    ForeignKeyConstraint(
        ('srcpkg_name', 'srcpkg_version'),
        ('srcpkg.name', 'srcpkg.version')),
)


class BinpkgFiles(Base):
    __tablename__ = 'binpkg_files'
    __table_args__ = (
        ForeignKeyConstraint(
            ('binpkg_name', 'binpkg_version'),
            ('binpkg.name', 'binpkg.version')
        ),
    )
    binpkg_name = Column(String, primary_key=True)
    binpkg_version = Column(String, primary_key=True)
    sha256 = Column(String, ForeignKey('hashes.sha256'), primary_key=True)
    architecture = Column(String, ForeignKey('architectures.name'), primary_key=True)
    hashes = relationship("DBhash")


# Main tables


class DBrepodata(Base):
    __tablename__ = 'repodata'
    id = Column(String(40), primary_key=True)

    def __repr__(self):
        return f"<ID {self.id}>"


class DBarchive(Base):
    __tablename__ = 'archives'
    name = Column(String, primary_key=True)
    timestamps = relationship("DBtimestamp", secondary=ArchivesTimestamps)

    def __repr__(self):
        return f"<Archive {self.name}>"


class DBtimestamp(Base):
    __tablename__ = 'timestamps'
    value = Column(String, primary_key=True)

    def __repr__(self):
        return f"<Timestamp {self.value}>"


class DBsuite(Base):
    __tablename__ = 'suites'
    name = Column(String, primary_key=True)

    def __repr__(self):
        return f"<Suite {self.name}>"


class DBcomponent(Base):
    __tablename__ = 'components'
    name = Column(String, primary_key=True)

    def __repr__(self):
        return f"<Component {self.name}>"


class DBarchitecture(Base):
    __tablename__ = 'architectures'
    name = Column(String, primary_key=True)

    def __repr__(self):
        return f"<Architecture {self.name}>"


class DBhash(Base):
    __tablename__ = 'hashes'
    sha256 = Column(String(64), primary_key=True)

    def __repr__(self):
        return f"<Hash {self.sha256}>"


class DBfile(Base):
    __tablename__ = 'files'

    id = Column(Integer, primary_key=True)
    sha256 = Column(String(64), ForeignKey('hashes.sha256'))
    size = Column(BigInteger, nullable=False)
    name = Column(String, nullable=False)
    path = Column(String, nullable=False)
    __table_args__ = (UniqueConstraint('sha256', 'name', 'size', 'path'),)

    def __repr__(self):
        return f"<File {self.sha256}>"


class DBsrcpkg(Base):
    __tablename__ = 'srcpkg'

    name = Column(String, primary_key=True)
    version = Column(String, primary_key=True)
    hashes = relationship("DBhash", secondary=SrcpkgFiles)

    def __repr__(self):
        return f"<Package {self.name}-{self.version}>"


class DBbinpkg(Base):
    __tablename__ = 'binpkg'

    name = Column(String, primary_key=True)
    version = Column(String, primary_key=True)
    hashes = relationship("BinpkgFiles")

    def __repr__(self):
        return f"<Binary {self.name}-{self.version}>"


# Temporary tables for DB provisioning


class DBtemphash(Base):
    __tablename__ = 'temphashes'
    __table_args__ = (
        UniqueConstraint('sha256', 'archive_name', 'suite_name', 'component_name', 'timestamp_value'),
        {'prefixes': ['UNLOGGED']}
    )
    id = Column(Integer, primary_key=True)
    sha256 = Column(String(64), nullable=False)
    archive_name = Column(String, nullable=False)
    suite_name = Column(String, nullable=False)
    component_name = Column(String, nullable=False)
    timestamp_value = Column(String, nullable=False)


class DBtempfile(Base):
    __tablename__ = 'tempfiles'
    __table_args__ = (
        UniqueConstraint('sha256', 'name', 'size', 'path'), {'prefixes': ['UNLOGGED']}
    )

    id = Column(Integer, primary_key=True)
    sha256 = Column(String(64), nullable=True)
    size = Column(BigInteger, nullable=False)
    name = Column(String, nullable=False)
    path = Column(String, nullable=False)
