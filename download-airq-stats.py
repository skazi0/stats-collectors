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

url = 'http://api.gios.gov.pl/pjp-api/v1/rest/'

table = 'air_quality'

index_values = {
    u'Bardzo dobry': 6.0,
    u'Dobry': 5.0,
    u'Umiarkowany': 4.0,
    u'Dostateczny': 3.0,
    u'Zły': 2.0,
    u'Bardzo zły': 1.0,
    u'Brak indeksu': 0.0,
    u'Brak pomiaru': 0.0,
}

logging.basicConfig()
logger = logging.getLogger('airq')
#logger.setLevel(logging.DEBUG)

# load norms from file (these are based on legend from http://powietrze.gios.gov.pl/pjp/current)
norms = {}
with open(os.path.join(os.path.dirname(__file__), 'norms.json')) as f:
    norms = json.load(f)

session = requests.session()

try:
    r = session.get(url + 'station/findAll', params={'size': 400})
    r.raise_for_status()
    json = r.json()
    json = json['Lista stacji pomiarowych']
except Exception as e:
    logger.error('error fetching stations list %s' % e)
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
        if value <= level['level']:
            return level['label']
    return ''

samples = []
for station in json:
    if not station['Nazwa miasta'] == 'Wrocław':
        continue
    stationID = station['Identyfikator stacji']
    try:
        r = session.get(url + 'station/sensors/%d' % stationID)
        r.raise_for_status()
        sensors = r.json()
    except Exception as e:
        logger.error('error fetching sensors for station ' + station['Nazwa stacji'] + ' ' + str(e))
        continue

    sensors = sensors['Lista stanowisk pomiarowych dla podanej stacji']
    for sensor in sensors:
        key = sensor['Wskaźnik - kod'].lower()
        if key not in norms:
            logger.debug('norms for key "%s" not found' % key)
            continue
#        logger.debug(key)
#        logger.debug(norms[key])
#        logger.debug(find_index(value['value'], norms[key]))

        try:
            r = session.get(url + 'data/getData/%d' % sensor['Identyfikator stanowiska'])
            data = r.json()
            r.raise_for_status()
        except Exception as e:
            # skip manual sensors
            if 'manual' not in data['error_reason']:
                logger.error('error fetching data for sensor %d %s %s', sensor['Identyfikator stanowiska'], e, data)
            continue

        data = data['Lista danych pomiarowych']
        # take max 2 last values
        for v in data[:2]:
            if v['Wartość'] is None:
                continue
            index_label = find_index(v['Wartość'], norms[key])
            ts = datetime.strptime(v['Data'], '%Y-%m-%d %H:%M:%S').timestamp()
            sample = {
                'time': int(ts),
                'value': max(-1, v['Wartość']),
                'qindexnum': index_values[index_label],
                'qindex': index_label,
                'label': sensor['Wskaźnik'],
#                'unit': clean_text(cps[value['cp']]['unit']),
                'tags': {
#                    'plId': meta['plId'],
#                    'euId': meta['euId'],
                    'station': station['Nazwa stacji'],
                    'type': sensor['Wskaźnik - kod'],
                }
            }
            samples.append(sample)
            logger.debug(sample)

stats.write_points(table, samples)
