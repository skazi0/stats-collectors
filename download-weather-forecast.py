#!/usr/bin/env python3
# -*- coding: utf8 -*-
import os
import sys
import logging
import re
import requests
import json
import pytz
import tzlocal
from datetime import datetime, timedelta

import stats

city_id = 2517
city_name = u'Wrocław'
url_template_daily = 'http://data.twojapogoda.pl/forecasts/city/daily/%d/%d' # city_id / page_num[1-3]
url_template_hourly = 'http://data.twojapogoda.pl/forecasts/city/hourly/%d/%d' # city_id / page_num[1-5]

table='weather_forecast'

logging.basicConfig()
logger = logging.getLogger('forecast')
#logger.setLevel(logging.DEBUG)

session = requests.session()

def fetch_forecast(url):
  try:
      r = session.get(url)
      json_data = r.json()
  except:
      logger.error('error fetching forecast data')
      sys.exit(1)

#  json_data = json.loads(open('forecast_h.json').read())

  if json_data['city']['name'] != city_name:
      logger.error('unexpected city in data: %s', json_data['city']['name'])
      sys.exit(1)

  forecasts = json_data['forecasts']

  samples = []
  for point in forecasts:
    # name = 01:00 -> hourly data
    if ':' in point['name']:
        date = datetime.strptime('%s %s' % (point['date'].split(', ')[-1], point['name']), '%d.%m.%Y %H:%M')
        sample = point_to_sample(point, date)
        # hourly rain is in mm/h not mm/12h
        sample['precip'] *= 12
        samples.append(sample)
        logger.debug(sample)
    # name = Wtorek -> daily data
    else:
        # "day" sample
        date = datetime.strptime('%s 12:00' % point['date'], '%d.%m.%Y %H:%M')
        if date > datetime.now() + timedelta(days=2):
            sample = point_to_sample(point, date)
            samples.append(sample)
            logger.debug(sample)

        # "night" sample
        # estimate feel temp by copying feel-to-temp delta from day sample
        feel_delta = point['temp_feel'] - point['temp']
        point['temp_feel'] = point['temp_night'] + feel_delta
        # replace temp with night temp
        point['temp'] = point['temp_night']
        date = datetime.strptime('%s 00:00' % point['date'], '%d.%m.%Y %H:%M')
        if date > datetime.now() + timedelta(days=2):
            sample = point_to_sample(point, date)
            samples.append(sample)
            logger.debug(sample)

    stats.write_points(table, samples)

def avg(numbers):
    return sum(numbers) / len(numbers)

def point_to_sample(point, ts):
    local_tz = tzlocal.get_localzone()
    return {
        'time': int((local_tz.localize(ts) - datetime.utcfromtimestamp(0).replace(tzinfo=pytz.utc)).total_seconds()),
        'wind_speed': point['wind_speed'],
        'wind_gusts': point['wind_gusts'],
        'wind_sign': point['wind_sign'],
        'temp': point['temp'],
        'temp_feel': point['temp_feel'],
        'relhum': point['relhum'],
        'pressmsl': point['pressmsl'],
        'precip': float(point['precip'].replace(',','.')),
        'biomet': point['biomet'],
        'thermal': point['thermal'],
        'sign': point['sign'],
        'sign_desc': point['sign_desc'],
        # sign_size is '90%' or '20-30%'
        'cloud_cover': avg(list(map(int, point['sign_size'].replace('%', '').split('-')))),
        'city': city_name,
        'city_id': city_id,
    }

# daily forecasts
for page in range(1, 4):
    fetch_forecast(url_template_daily % (city_id, page))

# hourly forecasts
for page in range(1, 6):
    fetch_forecast(url_template_hourly % (city_id, page))
