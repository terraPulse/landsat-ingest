'''
File: landsat_to_toa.py
Author: Min Feng
Version: 0.1
Create: 2017-12-27 17:58:43
Description:
'''

def find_file(d, ext):
    import os

    for _f in os.listdir(d):
        if _f.endswith(ext):
            return os.path.join(d, _f)

    return None

def metadata(f):
    _ms = {}
    with open(f) as _fi:
        for _l in _fi:
            _rs = [x.strip() for x in _l.strip().split('=')]
            if len(_rs) == 2:
                _ms[_rs[0]] = _rs[1]

    if 'SUN_AZIMUTH' in _ms:
        _ms['SolarAzimuth'] = _ms['SUN_AZIMUTH']
    if 'SUN_ELEVATION' in _ms:
        _ms['SolarZenith'] = 1 - float(_ms['SUN_ELEVATION'])

    return _ms

def get_rad_params(meta, b):
    if 60 < b < 70:
        _k = '6_VCID_%s' % (b % 10)
    else:
        _k = str(b)

    _b1 = float(meta['RADIANCE_MULT_BAND_%s' % _k])
    _b2 = float(meta['RADIANCE_ADD_BAND_%s' % _k])

    _b3 = float(meta.get('K1_CONSTANT_BAND_%s' % _k, 666.09))
    _b4 = float(meta.get('K2_CONSTANT_BAND_%s' % _k, 1282.71))

    return _b1, _b2, _b3, _b4

def _check_tif_name(d, f):
    import re
    import os

    _f = f

    _m = re.match('^(.+)_BAND(\d).TIF$', _f.upper())
    if _m:
        _f_out = '%s_B%s.TIF' % (_m.group(1), _m.group(2))
        return _f_out
        
    _m = re.match('^(.+)_BAND(1\d).TIF$', _f.upper())
    if _m:
        _f_out = '%s_B%s.TIF' % (_m.group(1), _m.group(2))
        return _f_out

    _m = re.match('^B(\d)0.TIF$', _f.upper())
    if _m:
        _id = os.path.basename(d)
        _f_out = '%s_B%s.TIF' % (_id, _m.group(1))
        return _f_out

    _m = re.match('^(.+)_b(\d).TIF$', _f.upper())
    if _m:
        _f_out = '%s_B%s.TIF' % (_m.group(1), _m.group(2))
        return _f_out

    _m = re.match('^(.+)_BAND6(\d).TIF$', _f.upper())
    if _m:
        _f_out = '%s_B6_VCID_%s.TIF' % (_m.group(1), _m.group(2))
        return _f_out

    if _f.endswith('.tif'):
        if _f.endswith('_b61.tif'):
            _f_out = _f.replace('_b61.tif', '_B6_VCID_1.tif').upper()
        elif _f.endswith('_b62.tif'):
            _f_out = _f.replace('_b62.tif', '_B6_VCID_2.tif').upper()
        else:
            _f_out = _f.upper()

        return _f_out
    
    return f

def fix_tif_name(d):
    import os
    import shutil

    for _f in os.listdir(d):
        _f_out = _check_tif_name(d, _f)

        if _f_out and _f != _f_out:
            shutil.move(os.path.join(d, _f), os.path.join(d, _f_out.upper()))

def fix_meta_file(d):
    import os
    import logging

    for _f in os.listdir(d):
        if not _f.endswith('_MTL.txt'):
            continue

        _f = os.path.join(d, _f)
        logging.info('found MTL file %s' % _f)

        _txt = None
        with open(_f) as _fi:
            _txt = _fi.read().strip()
        
        if _txt and _txt.startswith('b\'') and _txt.endswith('\''):
            logging.warning('fix meta file')

            _txt = _txt[2:-1].decode('string_escape')
            with open(_f, 'w') as _fo:
                _fo.write(_txt)

def tb_calibrate(bnd, met, b):
    import numpy as np

    # tir1_k2/ log((tir1_k1/rad_tir1)+1)
    bnd.nodata = int(bnd.data[0, 0])

    _p1, _p2, _p3, _p4 = get_rad_params(met, b)

    _d = ((bnd.data * _p1) + _p2)
    _d[_d == 0] = 1.0
    _d = (_p3 / _d)
    _v = _d + 1.0
    _v[_v <= 0.0000001] = 0.000001
    _d = (_p4 / np.log(_v)) - 273.15
    _d = _d * 100.0
    _d[bnd.data == bnd.nodata] = -9999

    return bnd.from_grid(_d.astype(np.int16), nodata=-9999)

def extract_bands(d, sid, bs, d_out):
    import os
    import logging
    from gio import geo_raster as ge
    from gio import file_unzip

    _f_toa = os.path.join(d, 'toa.img')
    _f_bth = os.path.join(d, 'thermal.img')
    _f_cld = os.path.join(d, 'cloud.img')

    with file_unzip.file_unzip() as _zip:
        _d_tmp = _zip.generate_file()
        os.makedirs(_d_tmp)

        _f_out = os.path.join(_d_tmp, '%s_toa_band%%d.tif' % (str(sid)))
        _bnd = ge.open(_f_toa)
        for _i in range(len(bs[0])):
            _b = bs[0][_i]
            logging.info('process spectral band %s: %s' % (_i, _b))

            _bbb = _bnd.get_band(_i + 1).cache()
            _bbb.data[_bbb.data == _bbb.nodata] = -9999
            _bbb.nodata = -9999
            _bbb.pixel_type = ge.pixel_type('short')
            _bbb.save(_f_out % _b)

        _f_out = os.path.join(_d_tmp, '%s_bt_band%%d.tif' % (str(sid)))
        _bnd = ge.open(_f_bth)

        _f_met = find_file(d, 'MTL.txt')
        _met = metadata(_f_met)

        for _i in range(len(bs[1])):
            _b = bs[1][_i]
            logging.info('process thermal band %s: %s' % (_i, _b))

            _bbb = _bnd.get_band(_i + 1).cache()
            _bbb = tb_calibrate(_bbb, _met, _b)

            _bbb.save(_f_out % _b)

        _f_fmk = os.path.join(_d_tmp, '%s_cfmask.tif' % (str(sid)))
        ge.open(_f_cld).get_band().cache().save(_f_fmk)

        if _f_met:
            logging.info('copy metadata %s' % _f_met)

            import shutil
            shutil.copy(_f_met, os.path.join(_d_tmp, '%s_MTL.txt' % str(sid)))

        file_unzip.compress_folder(_d_tmp, d_out, [])

def bands(sid):
    if sid.sensor == 'LC':
        return [list(range(1, 8)) + [9], [10, 11]]
    elif sid.sensor == 'LE':
        return [list(range(1, 6)) + [7], [61, 62]]
    elif sid.sensor == 'LT':
        return [list(range(1, 6)) + [7], [6]]

    raise Exception('failed to identify the sensor')

def to_toa(sid, d_inp):
    import os
    if os.path.exists(os.path.join(d_inp, 'cloud.img')):
        return

    from gio import run_commands
    import logging

    logging.info('process folder: %s' % d_inp)

    _cmd = 'rm -f L*_SENSOR*'
    run_commands.run(_cmd, cwd=d_inp)

    _cmd = 'rm -f L*_SOLAR*'
    run_commands.run(_cmd, cwd=d_inp)

    if sid.sensor == 'LC':
        _cmd = 'gdal_merge.py -separate -of HFA -co COMPRESSED=YES -o ref.img LC*_B[1-7,9].TIF'
        run_commands.run(_cmd, cwd=d_inp)

        _cmd = 'gdal_merge.py -separate -of HFA -co COMPRESSED=YES -o thermal.img LC*_B1[0,1].TIF'
        run_commands.run(_cmd, cwd=d_inp)

    elif sid.sensor == 'LE':
        _cmd = 'gdal_merge.py -separate -of HFA -co COMPRESSED=YES -o ref.img L*_B[1,2,3,4,5,7].TIF'
        run_commands.run(_cmd, cwd=d_inp)

        _cmd = 'gdal_merge.py -separate -of HFA -co COMPRESSED=YES -o thermal.img L*_B6_VCID_?.TIF'
        run_commands.run(_cmd, cwd=d_inp)

    elif sid.sensor == 'LT':
        _cmd = 'gdal_merge.py -separate -of HFA -co COMPRESSED=YES -o ref.img L*_B[1,2,3,4,5,7].TIF'
        run_commands.run(_cmd, cwd=d_inp)

        _cmd = 'gdal_merge.py -separate -of HFA -co COMPRESSED=YES -o thermal.img L*_B6.TIF'
        run_commands.run(_cmd, cwd=d_inp)

    else:
        raise Exception('unknown sensor %s' % sid)

    _cmd = 'fmask_usgsLandsatMakeAnglesImage.py -m *_MTL.txt -t ref.img -o angles.img'
    run_commands.run(_cmd, cwd=d_inp)

    _cmd = 'fmask_usgsLandsatSaturationMask.py -i ref.img -m *_MTL.txt -o saturationmask.img'
    run_commands.run(_cmd, cwd=d_inp)

    _cmd = 'fmask_usgsLandsatTOA.py -i ref.img -m *_MTL.txt -z angles.img -o toa.img'
    run_commands.run(_cmd, cwd=d_inp)

    _cmd = 'fmask_usgsLandsatStacked.py -t thermal.img -a toa.img -m *_MTL.txt -z angles.img -s saturationmask.img -o cloud.img'
    run_commands.run(_cmd, cwd=d_inp)

def identify_sid(d):
    from gio import landsat
    import os

    _id = landsat.parse(d)

    if _id:
        return _id

    if os.path.isdir(d):
        for _root, _dirs, _files in os.walk(d):
            for _file in _files:
                _id = landsat.parse(_file)
                if _id:
                    return _id

    return None

def convert(f_inp, d_out):
    from landsat_util import lib_landsat_toa as t
    from gio import file_unzip
    from gio import run_commands
    import os

    if not (f_inp.endswith('.tar.gz') or os.path.isdir(f_inp)):
        raise Exception('unsupported input file')

    with file_unzip.file_unzip() as _zip:
        _d = f_inp

        if f_inp.endswith('.tar.gz'):
            _d = _zip.generate_file()
            os.makedirs(_d)

            run_commands.run('tar xzf %s' % os.path.abspath(f_inp), cwd=_d)
        elif os.path.isdir(f_inp):
            import shutil

            _d = os.path.join(_zip.generate_file(), os.path.basename(f_inp))
            shutil.copytree(f_inp, _d)
        else:
            raise Exception('unsupported input type %s' % f_inp)

        _sid = identify_sid(f_inp) or identify_sid(_d)
        if _sid is None:
            raise Exception('failed to parse Landsat ID')

        t.fix_tif_name(_d)
        t.fix_meta_file(_d)
        t.to_toa(_sid, _d)

        t.extract_bands(_d, _sid, t.bands(_sid), d_out)

def main(opts):
    from gio import landsat
    import os

    _f = '/mnt/data1/mfeng/test/test2/tmp/LE07_L1TP_153028_20001009_20170209_01_T1_MTL.txt'
    _d_out = '/mnt/data1/mfeng/test/test2/tmp/test1'

    _sid = landsat.parse(_f)
    if _sid is None:
        raise Exception('failed to parse the Landsat scene')

    _inp = os.path.dirname(_f)

    fix_tif_name(_inp)
    to_toa(_sid, _inp)
    extract_bands(_inp, _sid, bands(_sid), _d_out)

def usage():
    _p = environ_mag.usage(False)

    return _p

if __name__ == '__main__':
    from gio import environ_mag
    environ_mag.init_path()
    environ_mag.run(main, [environ_mag.config(usage())])

