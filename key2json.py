import json
from datetime import datetime
import pytz
import time
import sys

class Data():

    def __init__(self):
        
        self._data = dict()
        self._metadata = None
        
        self.path = './'
        self.base_filename = None

    def load_key_file(self, fname, path=None):

        filename = fname
        fname_parts = fname.split('.')
        self.base_filename = fname_parts[0]
        # print(self.base_filename)

        if path:
            # prepend path to fname
            self.path = path
            filename = path + fname
        print(f'filename = {filename}, {self.path}')
        
        try:
            with open(filename, mode='r') as dfile:
                print(f'{dfile}')
                for line in list(dfile):
                    # line = dfile.readline()
                    # print(f'line = {line}')
                    params = line.split(':')
                    key =  params[0].strip()
                    if key == '':
                        key = '-1'
                    if key not in self._data:
                        self._data[key] = {}

                    self._data[key]['name'] = params[1].strip()
                    self._data[key]['long_name'] = params[2].strip()
                    self._data[key]['generic_name'] = params[3].strip()
                    self._data[key]['units'] = params[4].strip()
                    self._data[key]['FORTRAN_format'] = params[5].strip()

        except FileNotFoundError as e:
            print(f'File not found: {e}')
            return None
        print(f'{self._data}')
        print('file loaded')

    def write_json_file(self):

        # with open('/home/horton/derek/Software/python/envDataSystem_analysis/utilities/AS_header.txt', mode='r') as h:
        #     header = h.read()
        #     print(header)

        # outfile = self.output_path + self.base_filename + '.json'
        outfile = self.base_filename + '.json'

        print(f'output file: {outfile}')
        try:
            json_file = open(outfile, mode='w')
        except Exception as e:
            print(f'write error AS: {e}')
            return None

        json.dump(self._data, json_file)

        json_file.close()



if __name__ == "__main__":

    print(f'args : {sys.argv}')

    fname = sys.argv[1]
    kw = {}
    # if len(sys.argv) > 2:
        # kw = {}
    for arg in sys.argv[2:]:
        print(arg)
        parts = arg.split('=')
        kw[parts[0]] = parts[1]

    print(f'fname={fname}, kw={kw}')

    d = Data()
    d.load_key_file(fname)
    d.write_json_file()
