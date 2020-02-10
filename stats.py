import logging
import requests

stats_url = 'http://localhost/stats.php'

logging.basicConfig()
logger = logging.getLogger('stats')
#logger.setLevel(logging.DEBUG)

# load config
try:
    with open('/etc/default/stats-collectors', 'r') as f:
        for line in f:
            parts = line.split('=')
            if parts[0] == 'URL':
                stats_url = parts[1].strip()
except:
    pass

def write_points(table, samples):
    for point in samples:
        r = requests.post(stats_url + '?' + table, json=point)
        if r.status_code != 200:
            logger.error('error writing stats')
