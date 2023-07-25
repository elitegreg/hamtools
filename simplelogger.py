import argparse
import datetime

from contextlib import closing

from hamutils.adif.adi import ADIWriter


DEFAULT_DATE = datetime.datetime.utcnow().date()
DEFAULT_BAND = "20m"
DEFAULT_MODE = "CW"


class LogRecord:
    def __init__(self, station_callsign=None, operator=None):
        self._fields = {}
        if station_callsign:
            self._fields["station_callsign"] = station_callsign
        if operator:
            self._fields["operator"] = operator

    @property
    def fields(self):
        return self._fields

    def prompt_all(self):
        def retry(func):
            while True:
                try:
                    func()
                except EOFError:
                    raise
                except Exception as ex:
                    print("exception caught", ex)
                else:
                    break


        while True:
            retry(self.prompt_band)
            retry(self.prompt_mode)
            retry(self.prompt_datetime)
            retry(self.prompt_call)
            retry(self.prompt_rst)
            retry(self.prompt_exchange)
            if self.prompt_confirm() == "y":
                return

    def prompt_band(self):
        global DEFAULT_BAND
        band = input(f"BAND ({DEFAULT_BAND}): ")
        if band == '':
            band = DEFAULT_BAND
        else:
            DEFAULT_BAND = band
        self._fields["band"] = band

    def prompt_mode(self):
        global DEFAULT_MODE
        mode = input(f"MODE ({DEFAULT_MODE}): ").upper()
        if mode == '':
            mode = DEFAULT_MODE
        else:
            DEFAULT_MODE = mode
        self._fields["mode"] = mode

    def prompt_datetime(self):
        global DEFAULT_DATE
        dt = input(f"DATETIME ([{DEFAULT_DATE.strftime('%Y%m%d')} ]HHMM): ")
        if len(dt) <= 4:
            tm = datetime.datetime.strptime(dt, "%H%M").time()
            dt = datetime.datetime.combine(DEFAULT_DATE, tm)
        else:
            dt = datetime.datetime.strptime(dt, "%Y%m%d %H%M")
            DEFAULT_DATE = dt.date()
        self._fields["datetime_on"] = dt

    def prompt_call(self):
        call = ''
        while call == '':
            call = input("CALL: ").upper()
        self._fields["call"] = call

    def prompt_rst(self):
        if DEFAULT_MODE == "CW":
            rst_rcvd = "599"
            rst_sent = "599"
        else:
            rst_rcvd = "59"
            rst_sent = "59"
        rst = input(f"RST ({rst_rcvd}): ")
        if rst == '':
            rst = rst_rcvd
        else:
            int(rst)  # raises ValueError if not integer
        self._fields["rst_rcvd"] = rst
        self._fields["rst_sent"] = rst_sent

    def prompt_exchange(self):
        exch = input("EXCHANGE: ")
        if exch:
            self._fields["srx_string"] = exch

    def prompt_confirm(self):
        inp = ''
        while inp not in ("y", "n"):
            print("***QSO***")
            for k, v in self._fields.items():
                print(f"{k}: {v}")
            print("*********")
            inp = input("Confirm ([y]|n): ").lower()
            if inp == '':
                inp = 'y'
        return inp


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--station-callsign", required=True)
    parser.add_argument("--operator")
    parser.add_argument("-o", "--output", required=True)
    args = parser.parse_args()

    with open(args.output, "wb") as f:
        with closing(ADIWriter(f, "simplelogger.py", 0.1)) as adif:
            try:
                while True:
                    record = LogRecord(args.station_callsign, args.operator)
                    record.prompt_all()
                    adif.add_qso(**record.fields)
            except EOFError:
                pass
