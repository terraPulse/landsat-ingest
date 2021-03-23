'''
File: lib_get_landsat.py
Author: Min Feng
Description: download the process Landsat DN to SR
'''

def _proc_sr(f, fzip, process_sr=True):
    from gio import config
    import os
    import logging
    
    if not process_sr:
        return f

    _debug = config.getboolean('conf', 'debug')
    _d_toa = os.path.join(fzip.generate_file(), os.path.basename(f))
    
    try:
        from . import lib_landsat_toa as t
        t.convert(f, _d_toa)
        return _d_toa
    except KeyboardInterrupt as err:
        raise err
    except Exception as err:
        import traceback
        logging.error(traceback.format_exc())
        logging.error(str(err))
        
        logging.warning('failed to convert to TOA %s' % f)
        print('\n\n* Error:', err)
        
    return None
    
def _get_aws(f_inp, fzip):
    from gio import run_commands
    from gio import config
    import logging
    import os
    
    _process_sr = config.getboolean('conf', 'process_sr', True)
    _debug = config.getboolean('conf', 'debug')
    
    _d_img = fzip.generate_file(f_inp)
    os.makedirs(_d_img)
    
    _cmd = 'get_landsat8_aws.py -i %s -o %s --temp %s' % (f_inp, _d_img, fzip.generate_file())
    if _debug:
        _cmd += ' --debug'
    
    _rs = run_commands.run(_cmd, raise_exception=False, debug=_debug)
    if _rs[0] != 0:
        if config.getboolean('conf', 'skip_bad_l8', True):
            logging.warning('skip Landsat 8 image %s' % f_inp)
            return
    
    return _proc_sr(_d_img, fzip, _process_sr)
    
def get(f_inp, fzip):
    from gio import config
    from gio import landsat
    import logging
    import os

    _process_sr = config.getboolean('conf', 'process_sr', True)
    _debug = config.getboolean('conf', 'debug')
    
    _zip = fzip
    _f_inp = f_inp
    
    if '/' not in _f_inp:
        _f_inp = _get_aws(_f_inp, _zip)
        
    elif _f_inp.startswith('gs://'):
        _info = landsat.parse(_f_inp)
        if _info and _info.sensor == 'LC' and config.getboolean('conf', 'use_aws_for_landsat8', False):
            logging.info('download Landsat 8 from AWS')
            _f_inp = _get_aws(os.path.basename(_f_inp), _zip)
        else:
            _f_inp = _get_gcs(_f_inp, _zip)
            
    elif _f_inp.endswith('.tar.gz'):
        if _process_sr:
            _f_inp = _proc_sr(_f_inp, _zip, _process_sr)
            
    elif os.path.isdir(_f_inp):
        if _process_sr:
            _f_inp = _proc_sr(_f_inp, _zip, _process_sr)

    elif _f_inp.startswith('s3://'):
        if _process_sr:
            _f_inp = _proc_sr(_f_inp, _zip, _process_sr)
            
    else:
    	raise Exception('failed to recognize the file type')
    	
    return _f_inp
    