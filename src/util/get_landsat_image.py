
class landsat_file:
    def __init__(self):
        pass

    @property
    def tm_bands(self):
        b = {
             'LANDSAT_1': ['B4.TIF', 'B5.TIF', 'B6.TIF', 'B7.TIF', 'MTL.txt'],
             'LANDSAT_2': ['B4.TIF', 'B5.TIF', 'B6.TIF', 'B7.TIF', 'BQA.TIF', 'GCP.txt', 'MTL.txt', 'VER.jpg', 'VER.txt'],
            #  'LANDSAT_3': ['B1.TIF', 'B2.TIF', 'B3.TIF', 'B4.TIF', 'B5.TIF', 'B6.TIF', 'B7.TIF', 'MTL.txt'],
             'LANDSAT_4': ['B1.TIF', 'B2.TIF', 'B3.TIF', 'B4.TIF', 'B5.TIF', 'B6.TIF', 'B7.TIF', 'MTL.txt'],
             'LANDSAT_5': ['B1.TIF', 'B2.TIF', 'B3.TIF', 'B4.TIF', 'B5.TIF', 'B6.TIF', 'B7.TIF', 'BQA.TIF', 'MTL.txt'],
             'LANDSAT_7': ['B1.TIF', 'B2.TIF', 'B3.TIF', 'B4.TIF', 'B5.TIF',
                           'B6_VCID_1.TIF', 'B6_VCID_2.TIF', 'B7.TIF', 'B8.TIF', 'BQA.TIF', 'MTL.txt'],
             'LANDSAT_8': ['B1.TIF', 'B2.TIF', 'B3.TIF', 'B4.TIF', 'B5.TIF', 'B6.TIF',
                           'B7.TIF', 'B8.TIF', 'B9.TIF', 'B10.TIF', 'B11.TIF', 'BQA.TIF', 'MTL.txt']}
        return b
        
    def url(self, p):
        from gio import landsat
        import os
        
        _h = 'http://storage.googleapis.com'
        _p = p.replace('gs://', '')
        
        _d = landsat.parse(_p)
        if _d is None:
            return None
            
        _fs = []
        for _b in self.tm_bands['LANDSAT_%s' % _d.mission]:
            _fs.append('%s/%s/%s_%s' % (_h, _p, os.path.basename(_p), _b))
    
        return _fs
        
    def download(self, p, d_out):
        import requests
        import os
        import logging
        from gio import config
        
        _d_out = d_out
        for _u in self.url(p):
            _f = os.path.join(_d_out, os.path.basename(_u))

            if os.path.exists(_f) and config.getboolean('conf', 'over_write', False) == False:
                logging.info('skip existed %s' % _f)
                return

            _r = requests.get(_u)

            logging.info('download %s (%s)' % (_u, _f))
            os.path.exists(_d_out) or os.makedirs(_d_out)
            
            if os.path.exists(_f):
                _s = os.path.getsize(_f)
                _u = int(_r.headers['Content-Length'])

                logging.warning('skip existed %s' % _f)
                if _s >= _u:
                    return

            _fb = _f + '.tmp'

            if os.path.exists(_fb):
                os.remove(_fb)

            with open(_fb, 'wb') as _fo:
                for _chunk in _r.iter_content(chunk_size=128):
                    _fo.write(_chunk)

            if os.path.getsize(_fb) < int(_r.headers['Content-Length']):
                logging.warning('failed to get the full file %s' % p)
                return True

            import shutil
            shutil.move(_fb, _f)
                    
        return _d_out

def _task(f, d_out, to_sr):
    import os
    import logging
    from gio import file_unzip
    from gio import config
    from gio import file_mag
    
    if to_sr:
        from gio import landsat
        _f_met = os.path.join(d_out, '%s_MTL.txt' % str(landsat.parse(f)))
        if file_mag.get(_f_met).exists() and not config.getboolean('conf', 'over_write', False):
            logging.info('skip existed scene %s' % d_out)
            return
    
    with file_unzip.zip() as _zip:
        _d_out = _zip.generate_file() if to_sr else d_out
        _nu = landsat_file().download(f, _d_out)
        
        if to_sr:
            logging.info('process to SR')
            from landsat_util import lib_landsat_toa as t
            t.convert(_d_out, d_out)

def _out(f, d, opts):
    from gio import landsat
    import os

    if not opts.scene_in_path:
        return d

    _p = landsat.parse(os.path.basename(f))
    _d = _p.tile if _p else '_'

    return os.path.join(d, _d, os.path.basename(f))
    
def main(opts):
    import logging
    from gio import file_mag
    # _f = 'gs://gcp-public-data-landsat/LT05/01/017/031/LT05_L1TP_017031_20100326_20160903_01_T1'
    
    if opts.output.startswith('s3://') and not opts.to_sr:
        logging.error('only SR supports S3 output (use --to-sr param)')
        return
    
    _d_out = opts.output

    _fs = []
    for _f in opts.input:
        if _f.endswith('.txt'):
            with open(file_mag.get(_f).get()) as _fi:
                _fs.extend(_fi.read().strip().splitlines())
        else:
            _fs.append(_f)
    
    if not opts.scene_in_path:
        if len(_fs) > 1:
            raise Exception('--scene-in-path needs to be enabled for image list')
        
    import os
    _ps = []
    _ls = []

    for _f in _fs:
        _o = _out(_f, _d_out, opts)
        if not _o.endswith('/'):
            _o = _o + '/'

        _ls.append(_o)
        _ps.append((_f, _o, opts.to_sr))

    if opts.download:
        logging.info('start downloading')
        from gio import multi_task
        multi_task.run(_task, multi_task.load(_ps, opts), opts)
        print()

    if opts.output_list:
        logging.info('save list to %s' % opts.output_list)
        from gio import file_unzip
        with file_unzip.zip() as _zip:
            _zip.save('\n'.join(_ls), opts.output_list)

def usage():
    _p = environ_mag.usage(True)
    
    _p.add_argument('-i', '--input', dest='input', required=True, nargs='+')
    _p.add_argument('-s', '--scene-in-path', dest='scene_in_path', type='bool', default=True, \
                    help='include scene ID in the output path')
    _p.add_argument('-w', '--over-write', dest='over_write', type='bool', default=False)
    _p.add_argument('-d', '--download', dest='download', type='bool', default=True)
    _p.add_argument('--to-sr', dest='to_sr', type='bool', help='convert to TOA SR')
    _p.add_argument('-o', '--output', dest='output', required=True)
    _p.add_argument('-l', '--output-list', dest='output_list')
    
    return _p

if __name__ == '__main__':
    from gio import environ_mag
    environ_mag.init_path()
    environ_mag.run(main, [environ_mag.config(usage())]) 
    