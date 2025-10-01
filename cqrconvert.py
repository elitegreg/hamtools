#!/usr/bin/env python3

import argparse
import json
import sys

from contextlib import closing

from hamutils.adif.adi import ADIReader, ADIWriter


qsl_map = {
        "B": {
            "qsl_sent_via": "B",
        },
        "D": {
            "qsl_sent_via": "D",
        },
        "E": {
            "qsl_sent_via": "E",
        },
        "M": {
            "qsl_sent_via": "D",
        },
        "N": {
        },
        "SB": {
            "qsl_sent_via": "B",
        },
        "SD": {
            "qsl_sent_via": "D",
        },
        "SE": {
            "qsl_sent_via": "E",
        },
        "SM": {
            "qsl_sent_via": "D",
        },
        "SMD": {
            "qsl_sent_via": "D",
        },
        "SMB": {
            "qsl_sent_via": "B",
        },
        "SCE": {
            "qsl_sent_via": "E",
        },
        "OQRS": {
            "qsl_sent": "Y",
            "qsl_sent_via": "E",
        }
}


def fix_qso(qso):
    try:
        profileidx = qso["app_cqrlog_profile"].split("|", 1)[0]
    except:
        print(f"Error QSO: {qso}")
        raise

    try:
        my_data = profiles[profileidx]
    except KeyError:
        pass
    else:
        qso.update(my_data)

    try:
        app_cqrlog_qsls = qso["app_cqrlog_qsls"]
        qsl_data = qsl_map[app_cqrlog_qsls]
    except KeyError:
        pass
    else:
        qso.update(qsl_data)

    return qso


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("jsoncfg")
    parser.add_argument("sourcefile")
    parser.add_argument("destfile")
    args = parser.parse_args()

    with open(args.jsoncfg, "r") as f:
        profiles = json.load(f)

    with open(args.sourcefile, "r") as f:
        reader = ADIReader(f)
        qsos = [fix_qso(qso) for qso in reader]

    with open(args.destfile, "wb") as of:
        with closing(ADIWriter(of, 'CQRLogConvert', 0.1)) as writer:
            for qso in qsos:
                try:
                    writer.add_qso(**qso)
                except Exception as e:
                    print(f"Error {e} with qso: {qso}")
                    sys.exit(1)

