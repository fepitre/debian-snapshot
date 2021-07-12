#!/usr/bin/env python3

import urllib.request
import urllib.error
import http.client
import re
from pathlib import Path

from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed


@retry(
    retry=(
        retry_if_exception_type(urllib.error.HTTPError) |
        retry_if_exception_type(http.client.HTTPException)
    ),
    wait=wait_fixed(5),
    stop=stop_after_attempt(100),
)
def main():
    # FIXME: debian, debian-archive, debian-backports, debian-debug,
    #  debian-ports, debian-security, debian-volatile
    outdir = Path("/snapshot/by-timestamp")
    outdir.mkdir(exist_ok=True)
    for archive in ["debian", "debian-backports", "debian-ports", "debian-security"]:
        months = []
        with urllib.request.urlopen(
            "http://snapshot.debian.org/archive/%s/" % archive
        ) as f:
            for line in f:
                res = re.fullmatch(
                    r'<a href="\./\?year=(?P<year>\d+)&amp;month=(?P<month>\d+)">\d+</a>\n',
                    line.decode("utf-8"),
                )
                if res is None:
                    continue
                months.append((int(res.group("year")), int(res.group("month"))))
        assert len(months) > 0
        timestamps = []
        outfile = (outdir / archive).with_suffix(".txt")
        lastmonth = None
        if outfile.exists():
            timestamps = outfile.read_text().splitlines()
            lastmonth = (int(timestamps[-1][:4]), int(timestamps[-1][4:6].lstrip("0")))

        for year, month in months:
            ts = []
            # skip all months before the one of the last timestamp we have
            if lastmonth is not None and (year, month) < lastmonth:
                continue
            with urllib.request.urlopen(
                "http://snapshot.debian.org/archive/%s/?year=%d&month=%d"
                % (archive, year, month)
            ) as f:
                for line in f:
                    res = re.fullmatch(
                        r"<a href=\"(\d{8}T\d{6}Z)/\">\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d</a><br />\n",
                        line.decode("utf-8"),
                    )
                    if res is None:
                        continue
                    # if this month is the month with the last timestamp, skip
                    # those timestamps we already have
                    if (
                        lastmonth is not None
                        and (year, month) == lastmonth
                        and res.group(1) in timestamps
                    ):
                        continue
                    ts.append(res.group(1))
            timestamps.extend(ts)
        outfile.write_text("\n".join(timestamps)+"\n")


if __name__ == '__main__':
    main()
