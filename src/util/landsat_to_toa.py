'''
File: landsat_toa.py
Author: Min Feng
Version: 0.1
Create: 2017-12-28 15:20:02
Description: convert Landsat DN to TOA
'''

def main(opts):
    from landsat_util import lib_landsat_toa as t
    t.convert(opts.input, opts.output)

def usage():
    _p = environ_mag.usage(False)

    _p.add_argument('-i', '--input', dest='input', required=True)
    _p.add_argument('-o', '--output', dest='output', required=True)

    return _p

if __name__ == '__main__':
    from gio import environ_mag
    environ_mag.init_path()
    environ_mag.run(main, [environ_mag.config(usage())])

