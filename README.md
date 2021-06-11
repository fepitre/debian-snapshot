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

URL: /mr/timestamp
http status codes: 200 404 500
Summary: list all timestamps
```

>Note: Contrary to `snapshot.debian.org`, we only use `SHA256`.

## Archives from other distributions

### QubesOS

We include the support for the [multi-versions](https://deb.qubes-os.org/all-versions) repository of QubesOS.
On this repository, we can find the QubesOS packages for `bullseye` and `buster`. As there is not strictly speaking `snapshots`
but a repository having multiple versions for packages, we reference the unique timestamp as `99990101T000000Z`.
Archives are named as `qubes-rX.Y-vm` where `rX.Y` references the Qubes release and `vm` is the Qubes `package-set`.
