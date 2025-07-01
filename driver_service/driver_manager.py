import os, io
import pandas as pd
from googleapiclient.http import MediaFileUpload, MediaIoBaseUpload, MediaIoBaseDownload

class DriveManager:
    def __init__(self, drive_service):
        self.drive_service = drive_service

    def get_or_create_folder(self, folder_name, parent_folder_id=None):
        query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        if parent_folder_id:
            query += f" and '{parent_folder_id}' in parents"
        response = self.drive_service.files().list(q=query, spaces='drive', fields='files(id, name)', pageSize=1).execute()
        folders = response.get('files', [])
        if folders:
            return folders[0]['id']
        else:
            metadata = {'name': folder_name, 'mimeType': 'application/vnd.google-apps.folder'}
            if parent_folder_id:
                metadata['parents'] = [parent_folder_id]
            folder = self.drive_service.files().create(body=metadata, fields='id').execute()
            return folder['id']

    def upload_file(self, file_path, folder_id):
        metadata = {'name': os.path.basename(file_path), 'parents': [folder_id]}
        media = MediaFileUpload(file_path, resumable=True)
        file = self.drive_service.files().create(body=metadata, media_body=media, fields='id').execute()
        return file['id']

    def upload_dataframe_as_csv(self, df, filename, folder_id):
        df.to_csv(filename, index=False)
        file_id = self.upload_file(filename, folder_id)
        os.remove(filename)
        return file_id

    def upload_dataframe_in_memory(self, df, filename, folder_id):
        buffer = io.BytesIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)
        metadata = {'name': filename, 'parents': [folder_id]}
        media = MediaIoBaseUpload(buffer, mimetype='text/csv')
        file = self.drive_service.files().create(body=metadata, media_body=media, fields='id').execute()
        return file['id']

    def download_file(self, file_id, destination_path):
        request = self.drive_service.files().get_media(fileId=file_id)
        with io.FileIO(destination_path, 'wb') as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()

    def get_file_id_by_name(self, file_name, parent_folder_id=None):
        query = f"name = '{file_name}' and mimeType = 'text/csv' and trashed = false"
        if parent_folder_id:
            query += f" and '{parent_folder_id}' in parents"
        response = self.drive_service.files().list(q=query, spaces='drive', fields='files(id, name)', pageSize=1).execute()
        files = response.get('files', [])
        return files[0]['id'] if files else None

    def fetch_csv_by_name_as_dataframe(self, file_name, parent_folder_id=None):
        file_id = self.get_file_id_by_name(file_name, parent_folder_id)
        if file_id is None:
            return None
        request = self.drive_service.files().get_media(fileId=file_id)
        buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(buffer, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        buffer.seek(0)
        return pd.read_csv(buffer)


    def list_files_in_folder(self, folder_id):
        query = f"'{folder_id}' in parents and trashed = false"
        results = self.drive_service.files().list(
            q=query,
            spaces='drive',
            fields='files(id, name, mimeType, modifiedTime, size)',
            pageSize=1000
        ).execute()

        files = results.get('files', [])
        return files
