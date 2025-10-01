#!/usr/bin/env python3
from hamutils.adif.adi import ADIReader

import argparse
import datetime
import json
import sys


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("file")
    args = parser.parse_args()

    with open(args.file) as f:
        reader = ADIReader(f)
        qsos = [qso for qso in reader]
        for qso in qsos:
            for (k, v) in qso.items():
                if type(v) in (datetime.date, datetime.time, datetime.datetime):
                    qso[k] = str(v)
        json.dump(qsos, sys.stdout, indent=4)
