import os
import logging
import requests
import time
import pickle

stats_url = 'http://localhost/stats.php'

logging.basicConfig()
logger = logging.getLogger('stats')
#logger.setLevel(logging.DEBUG)

# load config from global and local files
for conf in ['/etc/default/stats-collectors', os.path.join(os.environ['HOME'], '.stats-collectors')]:
    try:
        with open(conf, 'r') as f:
            for line in f:
                parts = line.split('=')
                if parts[0] == 'URL':
                    stats_url = parts[1].strip()
    except:
        pass

def write_points(table, samples):
    for point in samples:
        r = requests.post(stats_url + '?table=' + table, json=point)
        if r.status_code != 200:
            logger.error('error writing stats: %s', r.text)

def cache(name, timeout, keepCacheOnNone=False):
    cache_file = os.path.join(os.environ.get('TMP', '/tmp'), '%s.cache' % name)
    def cache_wrapper(func):
        def wrapper(*args, **kwargs):
            try:
                mtime = os.path.getmtime(cache_file)
            except OSError as e:
                mtime = 0
            # update cache
            if mtime < time.time() - timeout:
                ret = func(*args, **kwargs)
                if ret is not None:
                    with open(cache_file, 'wb') as f:
                        pickle.dump(ret, f)
                    return ret
                elif not keepCacheOnNone:
                    return ret
            # return from cache
            with open(cache_file, 'rb') as f:
                return pickle.load(f)
        return wrapper
    return cache_wrapper
