Snapshot
===

Create a Debian snapshot service like [snapshot.debian.org](snapshot.debian.org).

It currently uses `snapshot.debian.org` set of timestamps and data for provisioning the service. 

> WIP: In a near future, we plan to manage our own set of timestamps and download data directly from `deb.debian.org`.
> We currently stick to `snapshot.debian.org` set of timestamps only for current development and testing and notably,
> for `metasnap.debian.net` portability.

## Client

```
usage: snapshot.py [-h] [--archive ARCHIVE] [--suite SUITE] [--component COMPONENT] [--arch ARCH] [--timestamp TIMESTAMP]
                   [--check-only] [--provision-db] [--provision-db-only] [--ignore-provisioned] [--no-clean-part-file] [--verbose]
                   [--debug]
                   local_directory

positional arguments:
  local_directory       Local directory for snapshot.

optional arguments:
  -h, --help            show this help message and exit
  --archive ARCHIVE     Debian archive to snapshot. Default is 'debian' and is the only supported archive right now.
  --suite SUITE         Debian suite to snapshot. Can be used multiple times. Default is 'unstable'
  --component COMPONENT
                        Debian component to snapshot. Default is 'main'
  --arch ARCH           Debian arch to snapshot. Can be used multiple times.
  --timestamp TIMESTAMP
                        Timestamp to use for snapshot. Can be used multiple times. Default is all the available timestamps.
                        Timestamps range can be expressed with ':' separator. Empty boundary is allowed and and this case, it would
                        use the lower or upper value in all the available timestamps. For example:
                        '20200101T000000Z:20210315T085036Z', '20200101T000000Z:' or ':20100101T000000Z'.
  --check-only          Check downloaded files only.
  --provision-db        Provision database.
  --provision-db-only   Provision database only.
  --ignore-provisioned  Ignore already provisioned repodata.
  --no-clean-part-file  No clean partially downloaded files.
  --verbose             Display logger info messages.
  --debug               Display logger debug messages.
```

### Examples

1) Partial timestamps set for `debian` archive (default value), `unstable` and `bullseye` suites, `amd64`, `all` and `source`
architectures, `main` component (default value) since `20200101T000000Z` to local directory `/snapshot`:
```
./snapshot.py /snapshot --debug --suite unstable --suite bullseye --arch amd64 --arch all --arch source --timestamp 20200101T000000Z:
```
>Note: Pay attention to the ':'

2) Partial timestamps set for `debian` archive, `bullseye` suite, `all` architecture, `main` component for `20210221T150011Z`
and `20210315T085036Z` timestamp to local directory `/snapshot`:
```
./snapshot.py /snapshot --debug --suite unstable --suite bullseye --arch amd64 --arch all --timestamp 20210221T150011Z --timestamp 20210315T085036Z
```

3) Full timestamps set for Debian `unstable`, `arm64` architecture to local directory `/snapshot`:
```
./snapshot.py /snapshot --debug --suite unstable --arch arm64
```

### Available timestamps

A partial timestamps set (see Example 1) is available at `http(s)://debian.notset.fr/snapshot`. The only thresholds are (extracted `Nginx` conf):

```
limit_conn conn_limit_per_ip 20;
limit_rate 10m;
```

This is for allowing every Debian rebuilder infrastructure to scale their actual builders.

## API

The snapshot process extracts and stores repository metadata (`Sources.gz` and `Packages.gz`) into a database.
From it, we expose a machine-readable output API similar to [snapshot.debian.org](https://salsa.debian.org/snapshot-team/snapshot/-/raw/master/API),
but extended. We store in database the full location information for a file in terms of `archive`, `suite`, `component`
and ranges of `timestamps` that we expose for the API results. Contrary to `snapshot.debian.org`, it allows to know
the exact repository location for a given file and to not being limited to only `archive` name and the first `timestamp`
that is has been recorded.

We currently expose the following similar endpoints:
```
URL: /mr/package
HTTP status codes: 200 404 500
Summary: list source package names

URL: /mr/package/<package>
HTTP status codes: 200 404 500
Summary: list all available source versions for this package

URL: /mr/package/<package>/<version>/srcfiles
Options: fileinfo=1 includes fileinfo section
HTTP status codes: 200 404 500
Summary: list all files associated with a source package

URL: /mr/binary/<package>
HTTP status codes: 200 404 500
Summary: list all available binary versions for this package

URL: /mr/binary/<package>/<version>/binfiles
Options: fileinfo=1 includes fileinfo section
HTTP status codes: 200 404 500
Summary: list all files associated with a binary package

URL: /mr/file
http status codes: 200 404 500
Summary: list all files

URL: /mr/file/<sha256>/info
http status codes: 200 404 500
Summary: information about file

URL: /mr/timestamp/<archive_name>
http status codes: 200 404 500
Summary: list all available timestamps for this archive name

URL: /mr/buildinfo
Options: suite_name=<suite_name> filter results for the given Debian suite
http status codes: 200 404 500
Summary: compute minimal set of timestamps containing all package versions in uploaded buildinfo file
```

>Note: Contrary to `snapshot.debian.org`, we only use `SHA256`.

### API examples

#### Get `debian` archive available timestamps (http://debian.notset.fr/snapshot/mr/timestamp/debian):
```json
{
  "_api": "0.3",
  "_comment": "notset",
  "result": [
    "20170101T032652Z",
    "20170101T092432Z",
    "20170101T153528Z",
(...)
    "20210718T032051Z",
    "20210718T092653Z",
    "20210718T144801Z",
    "20210718T204229Z",
    "20210719T031839Z",
    "20210719T090459Z"
  ]
}

```

#### Get source files info for `python-designateclient` package version `2.3.0-2` (http://debian.notset.fr/snapshot/mr/package/python-designateclient/2.3.0-2/srcfiles?fileinfo=1):
```json
{
  "_api": "0.3",
  "_comment": "notset",
  "package": "python-designateclient",
  "version": "2.3.0-2",
  "result": [
    {
      "hash": "240d86861138fbf8a34c1bf96412bf290dc8eae4a560473b0ecee605b8d1288f"
    },
    {
      "hash": "d65b4d861612c0bed42cdecedbcb0c32d886fc27bdc5642399ed410de042ed85"
    },
    {
      "hash": "ffb63b9b69d579fabd05d81a84c679dc396c29a663fcd244b0e8c600257478f3"
    }
  ],
  "fileinfo": {
    "240d86861138fbf8a34c1bf96412bf290dc8eae4a560473b0ecee605b8d1288f": [
      {
        "name": "python-designateclient_2.3.0-2.dsc",
        "path": "/pool/main/p/python-designateclient",
        "size": 3417,
        "archive_name": "debian",
        "suite_name": "buster",
        "component_name": "main",
        "timestamp_ranges": [
          ["20170618T072316Z", "20170821T035341Z"],
          ["20170822T154312Z", "20170922T035316Z"],
          ["20170924T042402Z", "20171024T092932Z"],
          ["20171025T221056Z", "20171106T213509Z"]
        ]
      },
      {
        "name": "python-designateclient_2.3.0-2.dsc",
        "path": "/pool/main/p/python-designateclient",
        "size": 3417,
        "archive_name": "debian",
        "suite_name": "unstable",
        "component_name": "main",
        "timestamp_ranges": [
          ["20170101T032652Z", "20171101T160520Z"]
        ]
      }
    ],
    "d65b4d861612c0bed42cdecedbcb0c32d886fc27bdc5642399ed410de042ed85": [
      {
        "name": "python-designateclient_2.3.0-2.debian.tar.xz",
        "path": "/pool/main/p/python-designateclient",
        "size": 4208,
        "archive_name": "debian",
        "suite_name": "buster",
        "component_name": "main",
        "timestamp_ranges": [
          ["20170618T072316Z", "20170821T035341Z"],
          ["20170822T154312Z", "20170922T035316Z"],
          ["20170924T042402Z", "20171024T092932Z"],
          ["20171025T221056Z", "20171106T213509Z"]
        ]
      },
      {
        "name": "python-designateclient_2.3.0-2.debian.tar.xz",
        "path": "/pool/main/p/python-designateclient",
        "size": 4208,
        "archive_name": "debian",
        "suite_name": "unstable",
        "component_name": "main",
        "timestamp_ranges": [
          ["20170101T032652Z", "20171101T160520Z"]
        ]
      }
    ],
    "ffb63b9b69d579fabd05d81a84c679dc396c29a663fcd244b0e8c600257478f3": [
      {
        "name": "python-designateclient_2.3.0.orig.tar.xz",
        "path": "/pool/main/p/python-designateclient",
        "size": 57008,
        "archive_name": "debian",
        "suite_name": "buster",
        "component_name": "main",
        "timestamp_ranges": [
          ["20170618T072316Z", "20170821T035341Z"],
          ["20170822T154312Z", "20170922T035316Z"],
          ["20170924T042402Z", "20171024T092932Z"],
          ["20171025T221056Z", "20171106T213509Z"]
        ]
      },
      {
        "name": "python-designateclient_2.3.0.orig.tar.xz",
        "path": "/pool/main/p/python-designateclient",
        "size": 57008,
        "archive_name": "debian",
        "suite_name": "unstable",
        "component_name": "main",
        "timestamp_ranges": [
          ["20170101T032652Z", "20171101T160520Z"]
        ]
      }
    ]
  }
}
```

#### Get binary files info for `python-designateclient` package version `2.3.0-2` (http://debian.notset.fr/snapshot/mr/binary/python-designateclient/2.3.0-2/binfiles?fileinfo=1):
```json
{
  "_api": "0.3",
  "_comment": "notset",
  "binary_version": "2.3.0-2",
  "binary": "python-designateclient",
  "result": [
    {
      "hash": "c50880146a09fa6a6f9cd7dfc11d5c0fc1147c673f938d0a667d348f59caf499",
      "architecture": "all"
    }
  ],
  "fileinfo": {
    "c50880146a09fa6a6f9cd7dfc11d5c0fc1147c673f938d0a667d348f59caf499": [
      {
        "name": "python-designateclient_2.3.0-2_all.deb",
        "path": "/pool/main/p/python-designateclient",
        "size": 43340,
        "archive_name": "debian",
        "suite_name": "buster",
        "component_name": "main",
        "timestamp_ranges": [
          ["20170618T072316Z", "20170821T035341Z"],
          ["20170822T154312Z", "20170922T035316Z"],
          ["20170924T042402Z", "20171024T092932Z"],
          ["20171025T221056Z", "20171106T213509Z"]
        ]
      },
      {
        "name": "python-designateclient_2.3.0-2_all.deb",
        "path": "/pool/main/p/python-designateclient",
        "size": 43340,
        "archive_name": "debian",
        "suite_name": "unstable",
        "component_name": "main",
        "timestamp_ranges": [
          ["20170101T032652Z", "20171101T160520Z"]
        ]
      }
    ]
  }
}
```

For every file, you have the detailed info in terms of archive, suite, component and timestamps it has been seen.
For a given location, the `timestamp_ranges` is a set of all timestamp ranges that a file is present.
A timestamp range is in the format of `[begin_timestamp, end_timestamp]` and contains all the timestamps available
for the archive between `begin_timestamp` and `end_timestamp`.

#### Compute a minimal set of timestamps containing all package versions referenced in a buildinfo file

* Example 1 (`curl -F 'buildinfo=<-' http://debian.notset.fr/snapshot/mr/buildinfo < bash_5.1-2_amd64.buildinfo`):
```json
{
  "_api": "0.3",
  "_comment": "notset: This feature is currently very experimental!",
  "results": [
    {
      "archive_name": "debian",
      "suite_name": "bullseye",
      "component_name": "main",
      "architecture": "amd64",
      "timestamps": [
        "20210101T211102Z",
        "20210110T204103Z",
        "20210116T204022Z",
        "20210208T213147Z"
      ]
    },
    {
      "archive_name": "debian",
      "suite_name": "unstable",
      "component_name": "main",
      "architecture": "amd64",
      "timestamps": [
        "20201230T203527Z",
        "20210106T142920Z"
      ]
    },
    {
      "archive_name": "debian",
      "suite_name": "buster",
      "component_name": "main",
      "architecture": "amd64",
      "timestamps": [
        "20210705T151228Z"
      ]
    }
  ]
}
```

For every known locations in terms of `archive_name`, `suite_name`, `component_name` and available architecture, it gives
the set of timestamps containing all package versions referenced in the provided buildinfo file. For rebuilder softwares,
you would use only one location which can contain more or less timestamps to be added to cover all the packages dependencies.

* Example 2 (`curl -F 'buildinfo=<-' http://debian.notset.fr/snapshot/mr/buildinfo?suite_name=buster < bash_5.1-2_amd64.buildinfo`):
```json
{
  "_api": "0.3",
  "_comment": "notset: This feature is currently very experimental!",
  "results": [
    {
      "archive_name": "debian",
      "suite_name": "buster",
      "component_name": "main",
      "architecture": "amd64",
      "timestamps": [
        "20210705T151228Z"
      ]
    }
  ]
}
```
It supports to filter which Debian suite to use. Additional filtering options will be provided in a near future.

## Archives from other distributions

### QubesOS

We include the support for the [multi-versions](https://deb.qubes-os.org/all-versions) repository of QubesOS.
On this repository, we can find the QubesOS packages for `bullseye` and `buster`. As there is not strictly speaking `snapshots`
but a repository having multiple versions for packages, we reference the unique timestamp as `99990101T000000Z`.
Archives are named as `qubes-rX.Y-vm` where `rX.Y` references the Qubes release and `vm` is the Qubes `package-set`.
