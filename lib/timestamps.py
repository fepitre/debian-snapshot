import os
import requests

from lib.exceptions import SnapshotException


def get_timestamps_from_metasnap(archive):
    """
    Get all snapshot.debian.org timestamps from metasnap.debian.net
    """
    timestamps = []
    url = f"https://metasnap.debian.net/cgi-bin/api?timestamps={archive}"
    try:
        resp = requests.get(url)
    except requests.ConnectionError as e:
        raise SnapshotException(str(e))
    if resp.ok:
        timestamps = sorted(set(resp.text.rstrip("\n").split("\n")), reverse=True)
    return timestamps


def get_timestamps_from_file(snapshot_dir, archive):
    """
    Get all snapshot.debian.org timestamps from local filesystem
    """
    localfile = f"{os.path.join(snapshot_dir, 'by-timestamp', archive + '.txt')}"
    try:
        with open(localfile, "r") as fd:
            timestamps = sorted(set(fd.read().rstrip("\n").split("\n")), reverse=True)
    except FileNotFoundError as e:
        raise SnapshotException(str(e))
    return timestamps
