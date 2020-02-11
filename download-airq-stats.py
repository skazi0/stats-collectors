#!/usr/bin/env python3
# -*- coding: utf8 -*-
import os
import sys
import logging
import re
import requests
import json
from datetime import datetime

import stats

#url = 'https://demo.dacsystem.pl/pobierz-dane-mapy'
url = 'http://air.wroclaw.pios.gov.pl/pobierz-dane-mapy'

table = 'air_quality'

index_values = {
    u'Bardzo dobry': 6.0,
    u'Dobry': 5.0,
    u'Umiarkowany': 4.0,
    u'Dostateczny': 3.0,
    u'Zły': 2.0,
    u'Bardzo zły': 1.0,
    u'Brak pomiaru': 0.0,
}

logging.basicConfig()
logger = logging.getLogger('airq')
#logger.setLevel(logging.DEBUG)

session = requests.session()

try:
    r = session.get(url)
    json = r.json()
except:
    logger.error('error fetching air quality data')
    sys.exit(1)

if json.get('error', False):
    logger.error('error returned from server: %s' % json['msg'])
    sys.exit(2)

#json = json.loads(open('pobierz-dane-mapy').read())

cps = json['compounds']
norms = json['norms']

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
for station in json['stations']:
    meta = station['params']
    if not meta['name'].startswith(u'Wrocław'):
        continue
#    logger.debug(meta)
    if not station['values']:
        logger.warning('no values for station %s' % meta['name'])
        continue
    for key, value in station['values'].items():
#        logger.debug(key)
#        logger.debug(norms[key])
#        logger.debug(find_index(value['value'], norms[key]))

        index_label = find_index(value['value'], norms[key])

        sample = {
            'time': value['ts'],
            'value': max(-1.0, 1.0 * value['value']),
            'qindexnum': index_values[index_label],
            'qindex': index_label,
            'label': clean_text(cps[value['cp']]['long']),
            'unit': clean_text(cps[value['cp']]['unit']),
            'tags': {
#                'plId': meta['plId'],
#                'euId': meta['euId'],
                'station': meta['name'],
                'type': clean_text(cps[value['cp']]['code'])
            }
        }
        samples.append(sample)
        logger.debug(sample)

stats.write_points(table, samples)
