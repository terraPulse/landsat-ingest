'''
File: get_landsat8_aws.py
Author: Min Feng
Version: 0.1
Create: 2017-12-29 15:53:59
Description: download Landsat 8 images from AWS
'''

import logging

def _download(url, f_out):
    logging.info('downloading URL %s' % url)

    # return _get_s3_from_url(url, f_out)
    from gio import run_commands
    import os

    _cmd = 'wget %s' % url
    _d_out = os.path.dirname(f_out)
    run_commands.run(_cmd, cwd=_d_out)
    _f_out = os.path.join(_d_out, url.split('/')[-1])
    if _f_out != f_out:
        import shutil
        shutil.move(_f_out, f_out)

    return

    import requests
    import shutil

    _num = 0
    try:
        with open(f_out, 'wb') as _fo:
            _r = requests.get(url, stream=True)
            print(_r.headers['content-length'])
            shutil.copyfileobj(_r.raw, _fo)

    except Exception as _err:
        _num += 1
        if _num > 3:
            raise _err

    # import urllib2
    # with open(f_out, 'wb') as _fo:
    #     _num = 0
    #     try:
    #         _fo.write(urllib2.urlopen(url).read())
    #     except Exception, _err:
    #         _num += 1
    #         if _num > 3:
    #             raise _err

def _get_s3_from_url(url, f_out):
    import urllib.parse
    _p = urllib.parse.urlparse(url).path
    if len(_p) > 1:
        if _p.startswith('/'):
            _p = _p[1:]

    return _get_s3(_p, f_out)

def _get_s3(key, f_out):
    import boto

    _s3 = boto.connect_s3()
    _bk = _s3.get_bucket('landsat-pds')

    _kk = _bk.get_key(key)
    if _kk == None:
        raise Exception('failed found key %s' % key)

    _t = f_out + '.bak'
    with open(_t, 'wb') as _fo:
        _kk.get_contents_to_file(_fo)

    import shutil
    shutil.move(_t, f_out)

def get_b8(sid, d_out=None):
    import os
    from gio import landsat

    _fs = {}

    _d_tmp = d_out
    os.path.exists(_d_tmp) or os.makedirs(_d_tmp)

    _id = landsat.parse(sid)
    _c1 = 'c1/' if 'LC08_' in str(sid) else ''

    for _b in ([10] + list(range(1, 8)) + [9, 11]):
        _fid = '%s_B%s.TIF' % (sid, _b)
        _fot = os.path.join(_d_tmp, _fid)
        _fs[_b] = _fot

        if os.path.exists(_fot):
            continue

        _url = 'http://landsat-pds.s3.amazonaws.com/%sL8/%03d/%03d/%s/%s' % \
                (_c1, _id.path, _id.row, sid, _fid)
        _download(_url, _fot)

    for _b in ['MTL.txt']:
        _fid = '%s_%s' % (sid, _b)
        _fot = os.path.join(_d_tmp, _fid)
        _fs[_b] = _fot

        if os.path.exists(_fot):
            continue

        _url = 'http://landsat-pds.s3.amazonaws.com/%sL8/%03d/%03d/%s/%s' % \
                (_c1, _id.path, _id.row, sid, _fid)
        _download(_url, _fot)

    return _fs

def download_scene_id(sid, d_out):
    import os
    from gio import file_unzip

    with file_unzip.file_unzip() as _zip:
        _d_tmp = _zip.generate_file()
        os.path.exists(_d_tmp) or os.makedirs(_d_tmp)

        if get_b8(sid, _d_tmp):
            file_unzip.compress_folder(_d_tmp, d_out, [])

def main(opts):
    download_scene_id(opts.scene_id, opts.output)

def usage():
    _p = environ_mag.usage(True)

    _p.add_argument('-i', '--scene-id', dest='scene_id', required=True)
    _p.add_argument('-o', '--output', dest='output', required=True)

    return _p

if __name__ == '__main__':
    from gio import environ_mag
    environ_mag.init_path()
    environ_mag.run(main, [environ_mag.config(usage())])

