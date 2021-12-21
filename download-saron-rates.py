#!/usr/bin/env python3
# -*- coding: utf8 -*-
import sys
import logging
import requests
import csv
from datetime import datetime

import stats

url = 'https://www.six-group.com/exchanges/downloads/indexdata/h_sar%dmc_delayed.csv' # % [1, 3, 6]

table = 'saron'

logging.basicConfig()
logger = logging.getLogger('saron')
#logger.setLevel(logging.DEBUG)

session = requests.session()

samples = []
for p in [1, 3, 6]:
    try:
        with session.get(url % p) as r:
            for row in csv.DictReader([l.decode() for l in r.iter_lines()], delimiter=';'):
                ts = datetime.strptime(row['date'], '%d.%m.%Y').timestamp()
                sample = {
                    'time': int(ts),
                    'saron%dm' % p: float(row['value']),
                }
                samples.append(sample)
                break
    except Exception as e:
        logger.error('error fetching saron data', e)
        sys.exit(1)

logger.debug(samples)
stats.write_points(table, samples)
