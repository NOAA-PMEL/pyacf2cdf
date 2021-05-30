import json
from datetime import datetime
import pytz
import time
import sys
from netCDF4 import Dataset, date2num


class Data:
    def __init__(self):

        self.EPS_GAP = 1.0e35
        self._data = dict()
        self._metadata = None

        self.path = "./"
        self.base_filename = None

        self._data["global"] = {
            "PROJECT": "",
            "PLATFORM": "",
            "FILE_START_TIME": "",
            "FILE_STOP_TIME": "",
            "DATA_TYPE": "",
            "VERSION": "",
            "SUBMIT_DATE": "",
            "SOURCE": "",
        }

        self._data["remarks"] = []

        self._data["dimesions"] = {
            "time": None,
            # 'dep': 1,
            # 'lat': 1,
            # 'lon': 1,
        }

        self._data["time_parameters"] = []

        self._data["parameters"] = {}
        self.acf_param_fields = [
            "name",
            "units",
            "instrument",
            "legal_min_value",
            "legal_max_value",
            "missing_value_code",
            "epic_key_code",
            "parameter_notes",
        ]

    def peek_line(self, fd):
        pos = fd.tell()
        line = fd.readline().strip()
        fd.seek(pos)
        return line

    def load_acf_file(self, fname, path=None):

        filename = fname
        fname_parts = fname.split(".")
        self.base_filename = fname_parts[0]
        # print(self.base_filename)

        if path:
            # prepend path to fname
            self.path = path
            filename = path + fname
        print(f"reading ACF: {filename}")
        # try:
        #     with open(filename, mode='r') as dfile:
        #         print(f'{dfile}')
        #         for line in list(dfile):
        try:
            with open(filename, mode="r") as dfile:
                # print(f'{dfile}')
                header = True
                param_order = []
                # for line in list(dfile):
                has_seconds = False
                while True:
                    if header:

                        next_line = self.peek_line(dfile)
                        if next_line == "#HEADER#":
                            line = dfile.readline().strip()
                            while True:
                                next_line = self.peek_line(dfile)
                                if next_line.startswith("#"):
                                    break
                                else:
                                    entry = dfile.readline().strip().split("=")
                                    if entry[0] in self._data["global"]:
                                        self._data["global"][entry[0]] = entry[1]

                        elif next_line == "#REMARKS#":
                            line = dfile.readline().strip()
                            while True:
                                next_line = self.peek_line(dfile)
                                if next_line.startswith("#"):
                                    break
                                else:
                                    entry = dfile.readline().strip()
                                    self._data["remarks"].append(entry)

                        elif next_line == "#TIME PARAMETERS#":
                            line = dfile.readline().strip()
                            params = dfile.readline().strip().split(" ")
                            self._data["time_parameters"] = params
                            if "second" in params:
                                has_seconds = True

                        elif next_line == "#PARAMETER#":
                            line = dfile.readline().strip()
                            name = dfile.readline().strip()
                            if name not in self._data["parameters"]:
                                self._data["parameters"][name] = {}
                                self._data["parameters"][name]["parameter_notes"] = []
                                param_order.append(name)

                            while True:
                                next_line = self.peek_line(dfile)
                                if next_line == "#PARAMETER NOTES#":
                                    line = dfile.readline()
                                    next_line = self.peek_line(dfile)
                                    while next_line[0] != "#":
                                        line = dfile.readline().strip()
                                        self._data["parameters"][name][
                                            "parameter_notes"
                                        ].append(line)
                                        next_line = self.peek_line(dfile)
                                elif next_line.startswith("#"):
                                    break
                                else:
                                    # line = dfile.readline().strip()  # units
                                    self._data["parameters"][name][
                                        "units"
                                    ] = dfile.readline().strip()
                                    self._data["parameters"][name][
                                        "instrument_label"
                                    ] = dfile.readline().strip()
                                    self._data["parameters"][name][
                                        "legal_min_value"
                                    ] = dfile.readline().strip()
                                    self._data["parameters"][name][
                                        "legal_max_value"
                                    ] = dfile.readline().strip()
                                    self._data["parameters"][name][
                                        "missing_value_code"
                                    ] = dfile.readline().strip()
                                    self._data["parameters"][name][
                                        "epic_key_code"
                                    ] = dfile.readline().strip()

                        elif next_line.startswith("#DATA"):
                            line = dfile.readline().strip()
                            par = line.split(" ")
                            for i in range(0, int(par[1])):
                                line = dfile.readline().strip()

                            header = False

                    else:

                        row = dfile.readline()
                        if not row:
                            break

                        vals = row.strip().split("\t")

                        # for timep in self._data['time_parameters']:
                        #     yyyy = 2020
                        #     if timep == 'year':
                        #         yyyy =

                        yyyy = vals[0]
                        MM = vals[1]
                        DD = vals[2]
                        hh = vals[3]
                        mm = vals[4]
                        if has_seconds:
                            ss = vals[5]
                        else:
                            ss = 0

                        if "time" not in self._data:
                            self._data["time"] = []

                        self._data["time"].append(
                            pytz.utc.localize(
                                datetime(
                                    int(yyyy),
                                    int(MM),
                                    int(DD),
                                    int(hh),
                                    int(mm),
                                    int(ss),
                                )
                            )
                        )

                        for i in range(0, len(param_order)):
                            time_cols = 5
                            if has_seconds:
                                time_cols = 6
                            col = i + time_cols

                            if param_order[i] not in self._data:
                                self._data[param_order[i]] = []
                            self._data[param_order[i]].append(float(vals[col]))

        # try:
        #     dfile = open(filename, mode='r')
        except FileNotFoundError as e:
            print(f"File not found: {e}")
            return None

        print("file loaded")

    def load_epic_file(self):

        # TODO: make this settable but user but for now hardcoded
        try:
            json_file = open("epic.json", mode="r")
            self.epic_key = json.load(json_file)
        except Exception as e:
            print(f"write error epic: {e}")
            return None

        json_file.close()

    def write_nc_file(self):

        self.load_epic_file()

        nc_outfile = self.base_filename + ".nc"
        cdf_outfile = self.base_filename + ".cdf"

        print(f"writing netCDF files:\n \tuser: {nc_outfile}\n\tplot: {cdf_outfile}")
        try:
            nc = Dataset(nc_outfile, "w", format="NETCDF4")
            cdf = Dataset(cdf_outfile, "w", format="NETCDF4")
        except Exception as e:
            print(f"write error AS: {e}")
            return None

        # create dimensions
        time_dim_cdf = cdf.createDimension("time", len(self._data["time"]))
        time_dim_nc = nc.createDimension("time", len(self._data["time"]))

        # create variables
        time_var_cdf = cdf.createVariable("time", "f8", ("time"))
        time_var_cdf.units = "seconds since 1970-01-01T00:00:00Z"
        time_var_cdf[:] = date2num(self._data["time"], units=time_var_cdf.units)

        time_var_nc = nc.createVariable("time", "f8", ("time"))
        time_var_nc.units = "seconds since 1970-01-01T00:00:00Z"
        time_var_nc[:] = date2num(self._data["time"], units=time_var_nc.units)

        for name, meta in self._data["parameters"].items():

            # generate plot version: .cdf
            if meta["epic_key_code"] in self.epic_key:
                epic = self.epic_key[meta["epic_key_code"]]

                par_name = f"{epic['name']}_{meta['epic_key_code']}"
                par = cdf.createVariable(
                    par_name,
                    # epic["name"],
                    "f8",
                    ("time"),
                )
                epic_units = epic["units"]
                # par['name'] = epic['name']
                par.setncattr("name", epic["name"])
                par.long_name = epic["long_name"]
                par.generic_name = epic["generic_name"]
                par.FORTRAN_format = epic["FORTRAN_format"]
                par.units = epic["units"]
                par.epic_code = meta["epic_key_code"]
                par.plot_lab = ""
                par.instr_id = meta["instrument_label"]
                par.min_value = meta["legal_min_value"]
                par.max_value = meta["legal_max_value"]
                par.gap_value = meta["missing_value_code"]
                for i in range(0, len(meta["parameter_notes"])):
                    note_name = f"note_{(i+1):02}"
                    par.setncattr(note_name, meta["parameter_notes"][i])

                vals = [
                    # x if x != float(par.gap_value) else self.EPS_GAP
                    x if x != float(par.gap_value) else None
                    for x in self._data[name]
                ]
                # vals2 = []
                # for x in self._data[name]:
                #     if x == float(par.gap_value):
                #         # vals2.append(1.e35)
                #         vals2.append(None)
                #     else:
                #         vals2.append(x)

                par[:] = vals
                # par[:] = self._data[name]

            # generate user version: .nc
            par_nc = nc.createVariable(
                name,
                "f8",
                ("time"),
            )
            # par_nc.units = meta["units"]
            par_nc.units = epic_units
            par_nc.epic_code = meta["epic_key_code"]
            par_nc.instr_id = meta["instrument_label"]
            par_nc.min_value = meta["legal_min_value"]
            par_nc.max_value = meta["legal_max_value"]
            par_nc.gap_value = meta["missing_value_code"]
            # for i in range(0, len(meta["parameter_notes"])):
            #     note_name = f"note_{(i+1):02}"
            #     par_nc.setncattr(note_name, meta["parameter_notes"][i])

            for note in meta["parameter_notes"]:
                parts = note.split("=")
                note_name = parts[0].lower()
                if note_name in meta:
                    note_name = f"note_{note_name}"
                note_text = ""
                if len(parts) > 2:
                    for text in parts[1:]:
                        note_text += f"{text}="
                    note_text = note_text.rstrip("=")
                else:
                    note_text = parts[1]
                par_nc.setncattr(note_name, note_text)

            vals = [
                x if x != float(par_nc.gap_value) else None for x in self._data[name]
            ]
            par_nc[:] = vals

        # set global attributes
        for name, val in self._data["global"].items():
            cdf.setncattr(name, val)
            nc.setncattr(name, val)

        for i in range(0, len(self._data["remarks"])):
            note_name = f"REMARK_{(i+1):02}"
            cdf.setncattr(note_name, self._data["remarks"][i])
            nc.setncattr(note_name, self._data["remarks"][i])

        # isofmt = "%Y-%m-%dT%H:%M:%SZ"
        dt = datetime.utcnow()
        cdf.setncattr(
            # "CREATION_DATE", pytz.utc.localize(datetime.utcnow()).strftime(isofmt)
            "CREATION_DATE",
            dt_to_string(dt),
        )
        nc.setncattr(
            # "CREATION_DATE", pytz.utc.localize(datetime.utcnow()).strftime(isofmt)
            "CREATION_DATE",
            dt_to_string(dt),
        )

        cdf.close()
        nc.close()

        print("done")


isofmt = "%Y-%m-%dT%H:%M:%SZ"


def dt_to_string(dt):
    utc = pytz.utc.localize(dt)
    return utc.strftime(isofmt)


# def string_to_dt(dt_string):
#     dt = datetime.strptime(dt_string, isofmt)
#     # print(type(dt))
#     # print(type(pytz.utc.localize(dt)))
#     return pytz.utc.localize(dt)

if __name__ == "__main__":

    # print(f'args : {sys.argv}')

    fname = sys.argv[1]
    # fname = "/home/derek/Downloads/NEAQS2002_met.acf"
    # fname = "/home/derek/Downloads/ACE1_sw.acf"
    # kw = {}
    # # if len(sys.argv) > 2:
    #     # kw = {}
    # for arg in sys.argv[2:]:
    #     print(arg)
    #     parts = arg.split('=')
    #     kw[parts[0]] = parts[1]

    # print(f'fname={fname}, kw={kw}')

    d = Data()
    # d.load_acf_file('ATOMIC_SW_v0.acf')
    d.load_acf_file(fname)
    d.write_nc_file()
