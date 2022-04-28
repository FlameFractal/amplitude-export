# Script to export ampltiude data (minutes spent per user per current_day)

# internal libs
import os
import json
import gzip
import shutil
from csv import reader, writer
from io import BytesIO
from zipfile import ZipFile
from datetime import date, datetime, timedelta

# external libs
import requests
from tqdm import tqdm

# configuration
start_date = date(2021, 8, 12)
end_date = date(2021, 8, 14)

active_users_cohort_filename = 'active-users-cohort.csv'
output_file_name = 'bindr-amplitude-export.csv'
temp_data_folder = 'data'
data_download_batch_days = 1

api_key = 'd86f1087f9f46acb4bbefe8b2d1f240d'
api_secret = '2a2fe89505f195014318d18109ed964f'
base_url = 'https://amplitude.com/api/2/export'

# helper functions

def write_to_csv(user_durations):
  print('INFO: writing to csv')

  sorted_user_durations = sorted(user_durations, key=lambda d: d['date'])

  with open(output_file_name, 'w', newline='') as csv_file:
    writer_object = writer(csv_file)
    for user in sorted_user_durations:
      writer_object.writerow([user['date'].strftime("%Y-%m-%d"), user['amplitude_id'], user['user_id'], user['duration']])

def process_user_durations(user_session_durations):
  user_durations = [] # [{date, amplitude_id, user_id, duration}]

  for amplitude_id in user_session_durations:
    sorted_session_durations = sorted(user_session_durations[amplitude_id]['session_durations'], key=lambda d: d['start'])

    previous_end = sorted_session_durations[0]['end']
    current = sorted_session_durations[0]['start']
    total_day_duration = 0 # current current_day's total duration
    leftover_duration = 0 # excess after midnight

    for session in sorted_session_durations:
      start = session['start']
      end = session['end']

      # day has changed, append entry
      if start.strftime("%Y%m%d") != current.strftime("%Y%m%d"):
        # discard zero duration entries
        if round(total_day_duration, 2) > 0:
          user_durations.append({
            'date': current,
            'amplitude_id': amplitude_id,
            'user_id': user_session_durations[amplitude_id]['user_id'],
            'duration': round(total_day_duration, 2)
          })
          current = start
          total_day_duration = leftover_duration
          leftover_duration = 0

      # session ends on same day
      if start.strftime("%Y%m%d") == end.strftime("%Y%m%d"):
        duration = (end - start).total_seconds() / 60
      # if session ends on immediate next day, handle overnight sessions
      elif (datetime(end.year, end.month, end.day) - datetime(start.year, start.month, start.day)).total_seconds() == 60*60*24:
        midnight = datetime(start.year, start.month, start.day)
        duration = (midnight - start).total_seconds() / 60
        leftover_duration = (end - midnight).total_seconds() / 60
      # discard session ends more than 24h later
      else:
        duration = 0

      # overlap calculation
      additional_duration = 0
      if start > previous_end:
        additional_duration = duration
      else:
        additional_duration = (end - previous_end).total_seconds() / 60

      additional_duration = max(additional_duration, 0)
      previous_end = max(previous_end, end)
      total_day_duration += duration + additional_duration

  return user_durations

def process_event_record(line, user_session_durations, active_users_amplitude_ids):
  event = json.loads(line)

  amplitude_id = str(event['amplitude_id'])
  session_id = event['session_id']

  if (
    # only process for active users
    (amplitude_id not in active_users_amplitude_ids) or
    # ignore server side "leave call" events
    (session_id == -1)
  ):
    return

  user_id = event.get('user_id')
  event_time = datetime.fromisoformat(event['event_time'])

  if amplitude_id not in user_session_durations:
    user_session_durations[amplitude_id] = {
      'user_id': user_id,
      'session_durations': [] # [{session_id, start, end}]
    }

  session_durations = user_session_durations[amplitude_id]['session_durations']
  session = [session for session in session_durations if session.get('session_id') == session_id]

  # first entry of a session
  if len(session) == 0:
    session_durations.append({
      'session_id': session_id,
      'start': event_time,
      'end': event_time,
    })
  else:
      session = session[0]
      start = session['start']
      end = session['end']

      # get start of session
      if event_time < start:
        session['start'] = event_time
      # get end of session
      elif event_time > end:
        session['end'] = event_time

def process_session_durations(active_users_amplitude_ids):
  user_session_durations = {}

  for (root, _, files) in os.walk(temp_data_folder, topdown=True):
    for file in files:
      if file.endswith('.gz'):
        with gzip.open(os.path.join(root, file), 'r') as fin:
          for line in fin:
            process_event_record(line, user_session_durations, active_users_amplitude_ids)

  return user_session_durations

def download_event_data(event_date):
  print('INFO: downloading data for ' + event_date)

  start = event_date + 'T00'
  end = event_date + 'T23'

  url = base_url + '?start=' + start + '&end=' + end

  response = requests.get(url, auth=(api_key, api_secret), stream=True)
  content = ZipFile(BytesIO(response.content))
  content.extractall(os.path.join(temp_data_folder, event_date))

def daterange(start_date, end_date, step=1):
  for n in range(0, int((end_date - start_date).days), step):
      yield start_date + timedelta(n)

def get_active_users():
  active_users_amplitude_ids = set()

  with open(active_users_cohort_filename, 'r') as csv_file:
    csv_reader = reader(csv_file, delimiter=',')
    for row in csv_reader:
      active_users_amplitude_ids.add(row[0])

  return active_users_amplitude_ids

# entrypoint

def main():
  active_users_amplitude_ids = get_active_users()

  shutil.rmtree(temp_data_folder, ignore_errors=True) # delete downloaded data

  for single_date in tqdm(daterange(start_date, end_date, data_download_batch_days)):
    download_event_data(single_date.strftime("%Y%m%d"))

  user_session_durations = process_session_durations(active_users_amplitude_ids)
  user_durations = process_user_durations(user_session_durations)
  write_to_csv(user_durations)

if __name__=="__main__":
  main()
