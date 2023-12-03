import argparse
import collections
import datetime
import enum
import pandas as pd
import re
import typing


QSO_RE = re.compile("^QSO:\s+(\d+)\s+(CW|PH|DG|RY)\s+(\d\d\d\d-\d\d-\d\d)\s+(\d+)\s+([A-Z0-9]+)\s+(\d+)\s+(\w+)\s+([A-Z0-9]+)\s+(\d+)\s+(\w+)\s*\d?")

Mode = enum.Enum("Mode", "CW PH DG".split())

SC_COUNTIES = set("ABBE AIKE ALLE ANDE BAMB BARN BEAU BERK CHOU CHAR CHES CHFD "
                  "CKEE CLRN COLL DARL DILL DORC EDGE FAIR FLOR GEOR GRWD GVIL "
                  "HAMP HORR JASP KERS LAUR LEE LEXI LNCS MARI MARL MCOR NEWB "
                  "OCON ORNG PICK RICH SALU SPAR SUMT UNIO WILL YORK".split())

CANADIAN_PROVINCES = set("AB BC MB NB NL NS NT NU ON PE QC SK YT".split())
US_STATES = set("AL AK AZ AR CA CO CT DC DE FL GA HI ID IL IN IA KS KY LA ME "
                "MD MA MI MN MS MO MT NE NV NH NJ NM NY NC ND OH OK OR PA RI "
                "SD TN TX UT VT VA WA WV WI WY".split())

VALID_EXCHANGES = SC_COUNTIES.union(US_STATES).union(CANADIAN_PROVINCES).union({"DX"})

BANDS = {
    (1800, 2000): "160m",
    (3500, 4000): "80m",
    (7000, 7300): "40m",
    (14000, 14350): "20m",
    (21000, 21450): "15m",
    (28000, 29700): "10m",
    (50000, 54000): "6m",
}

BONUS_STATIONS = ["W4CAE", "WW4SF"]
START_TIME = datetime.datetime(2023, 2, 25, 15)
END_TIME = datetime.datetime(2023, 2, 26, 2)


class StatsKeeper:
    def __init__(self):
        self._rows = []
        self._dup_count = 0

    def record_dup(self, qso):
        self._dup_count += 1

    def record(self, qso):
        if qso.srx == "DX":
            srx_cat = "dx"
        elif qso.srx in US_STATES:
            srx_cat = "states"
        elif qso.srx in CANADIAN_PROVINCES:
            srx_cat = "provinces"
        elif qso.srx in SC_COUNTIES:
            srx_cat = "sc counties"

        self._rows.append({"band": qso.band, "mode": qso.mode.name, srx_cat: qso.srx})

    def process(self):
        self._all = pd.concat((pd.DataFrame([r], columns=["band", "mode", "dx", "states", "provinces", "sc counties"]) for r in self._rows), ignore_index=True)
        self._dx = self._all.loc[~self._all.dx.isna()].drop(["states", "provinces", "sc counties"], axis=1)
        self._mults = self._all.loc[self._all.dx.isna()].drop(["dx"], axis=1)

        self._qsos_by_band = self._all.drop(["mode", "dx", "states", "provinces", "sc counties"], axis=1).value_counts()
        self._qsos_by_mode = self._all.drop(["band", "dx", "states", "provinces", "sc counties"], axis=1).value_counts()
        self._qsos_by_band_mode = self._all.drop(["dx", "states", "provinces", "sc counties"], axis=1).value_counts()
        self._dx_band_mode = pd.DataFrame(self._dx.groupby(["band", "mode"])["dx"].count())
        self._dx_band_mode = self._dx_band_mode.reindex(pd.MultiIndex.from_tuples(self._dx_band_mode.index, name=("band", "mode"))).unstack("band").fillna(0)
        self._dx_band_mode.dx = self._dx_band_mode.dx.astype(int)
        self._dx_band_mode["Total"] = self._dx_band_mode.sum(axis=1)
        self._dx_band_mode.loc["Total"] = self._dx_band_mode.sum(axis=0)

        self._mults_by_band = self._mults.drop(["mode"], axis=1).drop_duplicates().set_index(["band"]).groupby(["band"]).count().swapaxes(axis1=1, axis2=0)
        self._mults_by_mode = self._mults.drop(["band"], axis=1).drop_duplicates().set_index(["mode"]).groupby(["mode"]).count().swapaxes(axis1=1, axis2=0)
        self._mults_by_band_mode = self._mults.drop_duplicates().set_index(["band", "mode"]).groupby(["band", "mode"]).count().swapaxes(axis1=1, axis2=0)
        self._mults_no_breakdown = pd.DataFrame(self._mults.drop(["band", "mode"], axis=1).drop_duplicates().count(), columns=["Total"]).swapaxes(axis1=1, axis2=0)
        has_sc = 1 if self._mults_no_breakdown.loc["Total", "sc counties"] > 0 else 0
        self._mults_no_breakdown.loc["Total", "states"] += has_sc

        self._missing_states = US_STATES.copy()
        self._missing_provinces = CANADIAN_PROVINCES.copy()
        self._missing_counties = SC_COUNTIES.copy()
        
        self._missing_states.difference_update( self._all["states"].drop_duplicates())
        self._missing_provinces.difference_update( self._all["provinces"].drop_duplicates())
        self._missing_counties.difference_update( self._all["sc counties"].drop_duplicates())

    @property
    def multiplier(self):
        return len(self._mults.drop_duplicates(["mode", "states", "provinces", "sc counties"]))

    @property
    def qso_count(self):
        return len(self._all) + self._dup_count

    @property
    def unique_count(self):
        return len(self._all)

    def display(self):
        print(f"\nQSOs By Band\n============\n{self._qsos_by_band}")
        print(f"\nQSOs By Mode\n============\n{self._qsos_by_mode}")
        print(f"\nQSOs By Band/Mode\n=================\n{self._qsos_by_band_mode}")
        print(f"\nDX Summary\n==========\n{self._dx_band_mode}")
        print(f"\nStates/Provinces/SC Counties\n============================\n{self._mults_no_breakdown}")
        print(f"\nStates/Provinces/SC Counties By Band\n====================================\n{self._mults_by_band}")
        print(f"\nStates/Provinces/SC Counties By Mode\n====================================\n{self._mults_by_mode}")
        print(f"\nStates/Provinces/SC Counties By Band/Mode\n==========================================\n{self._mults_by_band_mode}")
        print(f"\nMissing States: {', '.join(sorted(self._missing_states))}")
        print(f"\nMissing Provinces: {', '.join(sorted(self._missing_provinces))}")
        print(f"\nMissing Counties: {', '.join(sorted(self._missing_counties))}")


class Scorer:
    # Does not support STATION working from multiple counties

    def __init__(self):
        self._call_band_mode_to_exch = collections.defaultdict(set)
        self._qso_points = 0
        self._bonuses = set()
        self._stats = StatsKeeper()

    def record(self, qso):
        oldexch = self._call_band_mode_to_exch[qso.callsign, qso.band, qso.mode]
        if qso.srx in oldexch:
            # dup
            print(f"WARNING: dup {qso.callsign} {qso.band} {qso.mode}")
            self._stats.record_dup(qso)
            return
        elif len(oldexch) > 0:
            print(f"WARNING: QSO with {qso.callsign} has multiple exchanges on the same band: {qso.srx}, {oldexch}")
            
        oldexch.add(qso.srx)
        self._stats.record(qso)

        if len(qso.srx) == 2:
            # non-SC contact
            self._qso_points += 4
        else:
            self._qso_points += 2

        if qso.callsign in BONUS_STATIONS:
            self._bonuses.add((qso.callsign, qso.band, qso.mode, qso.srx))

    def dump(self):
        self._stats.process()
        multiplier = self._stats.multiplier
        bonus_points = 250 * len(self._bonuses)
        score = self._qso_points * multiplier + bonus_points
        print(f"QSOs: {self._stats.qso_count}  Uniques: {self._stats.unique_count}")
        print(f"QSO POINTS: {self._qso_points}  MULTIPLIER: {multiplier}  BONUS POINTS: {bonus_points}")
        print(f"SCORE: {score}")
        self._stats.display()


class QSO(typing.NamedTuple):
    band: str
    mode: Mode
    timestamp: datetime.datetime
    station: str
    rst_s: int
    stx: str
    callsign: str
    rst_r: int
    srx: str


def validate_rst(rst):
    if rst < 100:
        if rst < 11 or rst > 59:
            raise RuntimeError(f"Invalid RST: {rst}")
    else:
        if rst < 111 or rst > 599:
            raise RuntimeError(f"Invalid RST: {rst}")


def validate_exch(exch):
    if exch not in VALID_EXCHANGES:
        raise RuntimeError(f"Invalid exchange: {exch}")


def load_qsos(filename):
    with open(filename) as f:
        for i, line in enumerate(f):
            try:
                line = line.strip()
                m = QSO_RE.match(line)
                if not m:
                    continue
                (freq, mode, dt, tm, station, rst_s, stx, callsign, rst_r, srx) = m.groups()
                freq = int(freq)
                for freqs, band in BANDS.items():
                    if freq >= freqs[0] and freq <= freqs[1]:
                        break
                else:
                    raise RuntimeError(f"Invalid frequency: {freq}")
                timestamp = datetime.datetime.strptime(f"{dt} {tm}", "%Y-%m-%d %H%M")
                if timestamp < START_TIME or timestamp > END_TIME:
                    print("Ignoring QSO: out of timerange ", timestamp)
                    continue

                rst_s = int(rst_s)
                rst_r = int(rst_r)

                validate_rst(rst_s)
                validate_rst(rst_r)
                validate_exch(stx)
                validate_exch(srx)
                yield QSO(band, getattr(Mode, mode), timestamp, station, rst_s, stx, callsign, rst_r, srx)
            except Exception as ex:
                raise RuntimeError(f"Exception on line {i + 1}: {ex}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file", nargs="+")
    args = parser.parse_args()

    scorer = Scorer()

    for f in args.file:
        for qso in load_qsos(f):
            scorer.record(qso)

    scorer.dump()
