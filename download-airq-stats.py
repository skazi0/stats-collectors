#!/usr/bin/env python3
# api docs: http://powietrze.gios.gov.pl/pjp/content/api
# -*- coding: utf8 -*-
import os
import sys
import logging
import re
import requests
import json
from datetime import datetime

import stats

url = 'http://api.gios.gov.pl/pjp-api/rest/'

table = 'air_quality'

index_values = {
    u'Bardzo dobry': 6.0,
    u'Dobry': 5.0,
    u'Umiarkowany': 4.0,
    u'Dostateczny': 3.0,
    u'Zły': 2.0,
    u'Bardzo zły': 1.0,
    u'Brak indeksu': 0.0,
}

logging.basicConfig()
logger = logging.getLogger('airq')
#logger.setLevel(logging.DEBUG)

# load norms from file (these are based on old API response)
norms = {}
with open(os.path.join(os.path.dirname(__file__), 'norms.json')) as f:
    norms = json.load(f)

session = requests.session()

try:
    r = session.get(url + 'station/findAll')
    json = r.json()
except:
    logger.error('error fetching stations list')
    sys.exit(1)

#if json.get('error', False):
#    logger.error('error returned from server: %s' % json['msg'])
#    sys.exit(2)

def clean_text(text):
    text = text.replace('&micro;', 'u')
    text = text.replace('<sub>', '')
    text = text.replace('</sub>', '')
    text = text.replace('<sup>', '')
    text = text.replace('</sup>', '')
    return text

def find_index(value, norm):
    for level in norm:
        if value < level['level']:
            return level['label']
    return ''

samples = []
for station in json:
    if not station['stationName'].startswith(u'Wrocław'):
        continue
    stationID = station['id']
    try:
        r = session.get(url + 'station/sensors/%d' % stationID)
        sensors = r.json()
    except:
        logger.error('error fetching sensors for station ' + station['stationName'])
        continue

    for sensor in sensors:
        key = sensor['param']['paramCode'].lower()
#        logger.debug(key)
#        logger.debug(norms[key])
#        logger.debug(find_index(value['value'], norms[key]))

        try:
            r = session.get(url + 'data/getData/%d' % sensor['id'])
            data = r.json()
        except:
            logger.error('error fetching data for sensor %d', sensor['id'])
            continue

        # take max 2 last values
        for v in data['values'][:2]:
            if v['value'] is None:
                continue
            index_label = find_index(v['value'], norms[key])
            ts = datetime.strptime(v['date'], '%Y-%m-%d %H:%M:%S').timestamp()
            sample = {
                'time': int(ts),
                'value': max(-1, v['value']),
                'qindexnum': index_values[index_label],
                'qindex': index_label,
                'label': sensor['param']['paramName'],
#                'unit': clean_text(cps[value['cp']]['unit']),
                'tags': {
#                    'plId': meta['plId'],
#                    'euId': meta['euId'],
                    'station': station['stationName'],
                    'type': sensor['param']['paramCode'],
                }
            }
            samples.append(sample)
            logger.debug(sample)

stats.write_points(table, samples)
