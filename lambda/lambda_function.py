import base64
import os
from datetime import timedelta, datetime

from googleapiclient.errors import HttpError

import requests
from googleapiclient.discovery import build, Resource
from googleapiclient.http import MediaFileUpload
from oauth2client.service_account import ServiceAccountCredentials


# Local : tmp
# AWS Lambda : /tmp
TMP_DIR = "tmp"


# Logger
def logger(message, level="INFO"):
    print(f"[{level}] {message}")


# ZOOM API
ZOOM_BASEURL = "https://zoom.us"


def get_zoom_token():
    client_id = os.environ["ZOOM_CLIENT_ID"]
    client_secret = os.environ["ZOOM_CLIENT_SECRET"]
    client = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()

    params = {
        "grant_type": "account_credentials",
        "account_id": os.environ['ZOOM_ACCOUNT_ID']
    }
    headers = {
        "Authorization": f"Basic {client}"
    }

    response = requests.post(f"{ZOOM_BASEURL}/oauth/token", params=params, headers=headers)
    response.raise_for_status()

    token = response.json().get("access_token")
    if token is None:
        raise Exception("Cloud not get access token")
    logger("Got zoom access token")
    return token


def get_date(difference=0):
    return (datetime.today() + timedelta(days=difference)).strftime("%Y-%m-%d")


def get_headers(token):
    return {
        "Authorization": f"Bearer {token}"
    }


def get_meet_records(token):
    response = requests.get(f"{ZOOM_BASEURL}/v2/users/me/recordings", headers=get_headers(token))
    response.raise_for_status()

    return response.json()


# GOOGLE API SETTINGS
SCOPES = ['https://www.googleapis.com/auth/drive.file']
PARENT_DIR_ID = os.environ["PARENT_DIR_ID"]
KEY_FILE = "service-account-key.json"


def get_google_drive_service() -> Resource:
    credentials = ServiceAccountCredentials.from_json_keyfile_name(KEY_FILE, scopes=SCOPES)
    return build("drive", "v3", credentials=credentials, cache_discovery=False)


def download_file(url, file_path, token):
    response = requests.get(url, stream=True, headers=get_headers(token))
    response.raise_for_status()

    logger(f"Starting download file [{os.path.basename(url)}]", level="INFO")
    with open(file_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=1024):
            f.write(chunk)
    logger(f"Finished download file [{os.path.basename(url)}]", level="INFO")


def remove_file(file_name):
    if os.path.exists(file_name):
        os.remove(file_name)


def make_google_drive_dir(name, parent_id, service: Resource):
    folder_metadata = {
        "name": name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_id]
    }
    folder = service.files().create(body=folder_metadata, fields="id").execute()
    return folder.get("id")


def upload_file(file_path: str, folder_name: str, today_dir_id: str, service: Resource):
    try:
        logger(f"Starting upload file {os.path.basename(file_path)}", level="INFO")

        meet_dir_id = make_google_drive_dir(folder_name, today_dir_id, service)
        file_metadata = {'name': os.path.basename(file_path), "parents": [meet_dir_id]}
        media = MediaFileUpload(file_path, mimetype='application/octet-stream', resumable=True)
        service.files().create(body=file_metadata, media_body=media, fields='id').execute()

        logger(f"File upload to Google Drive completed [{os.path.basename(file_path)}]")
    except HttpError as e:
        logger(f"Failed upload to Google Drive [{os.path.basename(file_path)}]: {e}", "ERROR")


def download_and_upload(url: str, file_name: str, folder_name: str, today_dir_id: str, token, service: Resource):
    try:
        download_file(url, file_name, token)
        upload_file(file_name, folder_name, today_dir_id, service)
    except HttpError as e:
        logger(f"Exception [{os.path.basename(file_name)}]: {e}", "ERROR")
    finally:
        remove_file(file_name)


def upload_today_record_to_google_drive():
    token = get_zoom_token()
    meets = get_meet_records(token)
    meet_count = meets.get('total_records')
    if meet_count == 0:
        logger("Not found any meets.")
        return

    logger(f"Fount meets total {meet_count}.")

    service = get_google_drive_service()
    logger("Made a today directory in Google Drive.")
    today_dir_id = make_google_drive_dir(get_date(), PARENT_DIR_ID, service)
    for i, meet in enumerate(meets.get("meetings")):
        logger(f"Now process is {meet.get('uuid')} [{i + 1}/{meet_count}]", "INFO")

        for record in meet.get("recording_files"):
            if record.get("file_extension") != "MP4":
                continue
            record_id = record.get('id')
            logger(f"Find video file [{record_id}]")
            download_and_upload(record.get("download_url"), f"{record_id}.mp4", meet.get('topic'), today_dir_id, token, service)
    logger("Finished", "INFO")


def lambda_handler(event, context):
    upload_today_record_to_google_drive()

    return {
        "statusCode": 204
    }
