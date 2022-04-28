# Script to export ampltiude data (minutes spent per user per day)

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

# configuration
start_date = date(2021, 4, 1)
end_date = date(2022, 3, 31)

active_users_cohort_filename = 'active-users-cohort.csv'
output_file_name = 'bindr-amplitude-export.csv'
temp_data_folder = 'data'

api_key = 'd86f1087f9f46acb4bbefe8b2d1f240d'
api_secret = '2a2fe89505f195014318d18109ed964f'
base_url = 'https://amplitude.com/api/2/export'

# helper functions

def write_to_csv(single_date, user_durations):
  print('INFO: writing to csv for ' + single_date.strftime("%Y%m%d"))

  with open(output_file_name, 'a') as csv_file:
    writer_object = writer(csv_file)
    for user_id in user_durations:
      writer_object.writerow([single_date.strftime("%Y-%m-%d"), user_id, user_durations[user_id]])
    csv_file.close()

def process_user_durations(session_durations):
  user_durations = {}
  
  for session_id in session_durations:
    user_id = session_durations[session_id]['user_id']
    start = session_durations[session_id]['start']
    end = session_durations[session_id]['end']

    duration = (end - start).total_seconds() / 60

    if user_id not in user_durations:
      total_duration = round(duration, 2)
    else:
      total_duration = round(user_durations[user_id] + duration, 2)

    if total_duration <= 0:
      continue

    user_durations[user_id] = total_duration

  return user_durations

def process_event_record(line, session_durations, active_user_ids):
  event = json.loads(line)

  if 'user_id' not in event:
    return

  session_id = event['session_id']
  user_id = event['user_id']
  event_time = datetime.fromisoformat(event['client_event_time'])

  # only process for active users
  if user_id not in active_user_ids:
    return

  # ignore server side "leave call" events
  if session_id == -1:
    return

  # adjust event time (ref: https://amplitude.com/blog/dont-trust-client-data)
  event_time += (datetime.fromisoformat(event['server_upload_time']) - datetime.fromisoformat(event['client_upload_time']))

  # # outlier entries with huge gap between upload and event times
  if(abs((datetime.fromisoformat(event['client_upload_time']) - event_time).total_seconds()) > 0.9*60*60):
    event_time = datetime.fromisoformat(event['server_upload_time'])

  # first entry of a session
  if session_id not in session_durations:
    session_durations[session_id] = {
      'start': event_time,
      'end': event_time,
      'user_id': user_id
    }
  else:
    start = session_durations[session_id]['start']
    end = session_durations[session_id]['end']

    # get start of session
    if event_time < start:
      session_durations[session_id]['start'] = event_time
    # get end of session
    elif event_time > end:
      session_durations[session_id]['end'] = event_time

def process_session_durations(active_user_ids):
  session_durations = {}

  for (root, _, files) in os.walk(temp_data_folder, topdown=True):
    for file in files:
      if file.endswith('.gz'):
        with gzip.open(os.path.join(root, file), 'r') as fin:
          for line in fin:
            process_event_record(line, session_durations, active_user_ids)

  return session_durations

def download_event_data(event_date):
  print('INFO: downloading data for ' + event_date)

  start = event_date + 'T00'
  end = event_date + 'T23'

  url = base_url + '?start=' + start + '&end=' + end

  response = requests.get(url, auth=(api_key, api_secret), stream=True)
  content = ZipFile(BytesIO(response.content))
  content.extractall(temp_data_folder)

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

def get_active_users():
  active_user_ids = set()

  with open(active_users_cohort_filename, 'r') as csv_file:
    csv_reader = reader(csv_file, delimiter=',')
    for row in csv_reader:
      active_user_ids.add(row[1])

  return active_user_ids

# entrypoint

def main():
  active_user_ids = get_active_users()

  for single_date in daterange(start_date, end_date):
    event_date = single_date.strftime("%Y%m%d")

    download_event_data(event_date)
    session_durations = process_session_durations(active_user_ids)
    user_durations = process_user_durations(session_durations)
    write_to_csv(single_date, user_durations)

    shutil.rmtree(temp_data_folder) # delete downloaded data

if __name__=="__main__":
  main()
