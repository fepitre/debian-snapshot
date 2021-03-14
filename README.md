SnapshotMirror
===

Create a local mirror of [snapshot.debian.org](snapshot.debian.org).

```
usage: snapshot-mirror.py [-h] [--archive ARCHIVE] [--suite SUITE] [--component COMPONENT] [--arch ARCH]
                          [--timestamp TIMESTAMP] [--timestamps-range TIMESTAMPS_RANGE] [--verbose] [--debug]
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
                        Snapshot timestamp to mirror. Can be used multiple times. Default is all the available
                        timestamps.
  --timestamps-range TIMESTAMPS_RANGE
                        Snapshot timestamps range to mirror expressed with ':' separator.Empty boundary is allowed
                        and and this case, it would use the lower or upper value in all the available timestamps.
                        For example: '20200101T000000Z:20210315T085036Z', '20200101T000000Z:' or
                        ':20100101T000000Z'.
  --verbose             Display logger info messages.
  --debug               Display logger debug messages.
```

### Examples

* Partial mirror for `debian` archive (default value), `unstable` and `bullseye` suites, `amd64` and `all` 
architectures, `main` component (default value) since `20200101T000000Z` to local directory `/mnt/snapshot`:
```
./snapshot-mirror.py /mnt/snapshot --debug --suite unstable --suite bullseye --arch amd64 --arch all  --timestamp 20200101T000000Z:
```

* Partial mirror for `debian` archive, `bullseye` suite, `all` architecture, `main` component for `20210221T150011Z`
and `20210315T085036Z` timestamps to local directory `/mnt/snapshot`:
```
./snapshot-mirror.py /mnt/snapshot --debug --suite unstable --suite bullseye --arch amd64 --arch all --timestamp 20210221T150011Z --timestamp 20210315T085036Z
```

* Full mirror for Debian `unstable`, `arm64` architecture to local directory `/mnt/snapshot`:
```
./snapshot-mirror.py /mnt/snapshot --debug --suite unstable --arch arm64
```
