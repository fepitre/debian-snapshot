import hashlib
import datetime


TS_FORMAT = "%Y%m%dT%H%M%SZ"

def sha256sum(fname):
    sha256 = hashlib.sha256()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def parse_ts(ts):
    return datetime.datetime.strptime(ts, TS_FORMAT)
