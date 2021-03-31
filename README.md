SnapshotMirror
===

Create a local mirror of [snapshot.debian.org](snapshot.debian.org).

```
usage: snapshot-mirror.py [-h] [--archive ARCHIVE] [--suite SUITE] [--component COMPONENT] [--arch ARCH] [--timestamp TIMESTAMP]
                          [--check-only] [--no-clean-part-file] [--verbose] [--debug]
                          local_directory

positional arguments:
  local_directory       Local directory for snapshot mirror.

optional arguments:
  -h, --help            show this help message and exit
  --archive ARCHIVE     Debian archive to mirror. Default is 'debian' and is the only supported archive right now.
  --suite SUITE         Debian suite to mirror. Can be used multiple times. Default is 'unstable'
  --component COMPONENT
                        Debian component to mirror. Default is 'main'
  --arch ARCH           Debian arch to mirror. Can be used multiple times.
  --timestamp TIMESTAMP
                        Snapshot timestamp to mirror. Can be used multiple times. Default is all the available timestamps. Timestamps range
                        can be expressed with ':' separator. Empty boundary is allowed and and this case, it would use the lower or upper
                        value in all the available timestamps. For example: '20200101T000000Z:20210315T085036Z', '20200101T000000Z:' or
                        ':20100101T000000Z'.
  --check-only          Check downloaded packages.
  --no-clean-part-file  No clean partially downloaded packages.
  --verbose             Display logger info messages.
  --debug               Display logger debug messages
```

### Examples

1) Partial mirror for `debian` archive (default value), `unstable` and `bullseye` suites, `amd64`, `all` and `source`
architectures, `main` component (default value) since `20200101T000000Z` to local directory `/snapshot`:
```
./snapshot-mirror.py /snapshot --debug --suite unstable --suite bullseye --arch amd64 --arch all --arch source --timestamp 20200101T000000Z:
```
>Note: Pay attention to the ':'

2) Partial mirror for `debian` archive, `bullseye` suite, `all` architecture, `main` component for `20210221T150011Z`
and `20210315T085036Z` timestamp to local directory `/snapshot`:
```
./snapshot-mirror.py /snapshot --debug --suite unstable --suite bullseye --arch amd64 --arch all --timestamp 20210221T150011Z --timestamp 20210315T085036Z
```

3) Full mirror for Debian `unstable`, `arm64` architecture to local directory `/snapshot`:
```
./snapshot-mirror.py /snapshot --debug --suite unstable --arch arm64
```

### Available mirror

A partial mirror (see Example 1) is available (in progress) at `http(s)://debian.notset.fr/snapshot`. The only thresholds are (extracted `Nginx` conf):

```
limit_conn conn_limit_per_ip 20;
limit_rate 10m;
```

This is for allowing every Debian rebuilder infrastructure to scale their actual builders.
