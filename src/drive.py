from __future__ import print_function
from googleapiclient.discovery import build
from httplib2 import Http
from googleapiclient.http import MediaIoBaseDownload
import io
import os

# tokens, credentials, etc

TMP_TOKEN = 'token.json'
GOOG_OAUTH_TOKEN = 'client_secrets.json'

# If modifying these scopes, delete the file token.json.
SCOPES = 'https://www.googleapis.com/auth/drive'

# Enclosing folder name:

PARENT_FOLDER_NAME = 'glaciers'

# ---------------------------------------------------------------------
#
# ---------------------------------------------------------------------


def start_service():
    '''
    start an authenticated google service; requires credentials.json file
    from registering google drive to allows for python access. This function 
    must be run as a script.
    '''
    from oauth2client import file, client, tools

    store = file.Storage('token.json')
    creds = store.get()
    if not creds or creds.invalid:
        flow = client.flow_from_clientsecrets('client_secrets.json', SCOPES)
        creds = tools.run_flow(flow, store)
    service = build('drive', 'v3', http=creds.authorize(Http()))
    return service


def query_from_drive(service, query, page_token):
    '''
    pass a query to the google drive api via given service.
    '''
    resp = (
        service
        .files()
        .list(
            q="mimeType ='application/vnd.google-apps.folder' and %s" % query,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            spaces='drive',
            fields='nextPageToken, files(id, name, size, parents)',
            pageToken=page_token
        ).execute()
    )

    return resp


def get_parent_folder_id(service, name=PARENT_FOLDER_NAME):
    '''
    Obtain the parent folder ID from the name.
    '''
    page_token = None
    resp = query_from_drive(service, "name='%s'" % name, page_token)
    files = resp.get('files', [])
    matches = {f.get('name'): f.get('id') for f in files}
    return matches[name]


def create_folder(service, folder_name, parentID=None):
    '''
    Create a folder in drive with `folder_name`. If parentID is given,
    create the folder in the folder with id=ParentID. Returns the id
    of the newly created folder.
    '''
    # Create a folder on Drive, returns the newely created folders ID
    body = {
        'name': folder_name,
        'mimeType': "application/vnd.google-apps.folder"
    }
    if parentID:
        body['parents'] = [parentID]
    root_folder = service.files().create(body=body).execute()
    return root_folder['id']


def get_folder_ids(service, parent_id, glims_ids=None):
    '''
    Returns a dictionary of glacier id folder names, and their
    respective IDs on google drive.
    '''
    page_token = None
    folder_ids = {}

    while True:

        resp = query_from_drive(service, "'%s' in parents" % parent_id, page_token)

        matches = {f.get('name'): f.get('id') for f in resp.get('files', [])}
        folder_ids.update(matches)

        page_token = resp.get('nextPageToken', None)
        if page_token is None:
            break

    if glims_ids:
        folder_ids = {k: v for (k, v) in folder_ids.items() if k in glims_ids}

    return folder_ids


def download_file(service, file_name, file_id):
    request = service.files().get_media(fileId=file_id)
    fh = io.FileIO(file_name, 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print("Download %d%%." % int(status.progress() * 100))

    return


def main():

    # start google service
    service = start_service()
    print('service started')

    create_folder(service, 'Test')

    return


if __name__ == '__main__':
    main()