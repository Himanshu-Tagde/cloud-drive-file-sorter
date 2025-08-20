

"""pip install --upgrade google-api-python-client google-auth google-auth-httplib2 google-auth-oauthlib
   google-api-python-client and related auth libraries installed
   Google Cloud project with Drive API enabled
   OAuth client credentials (credentials.json)
"""


import os
import sys
import hashlib
import mimetypes
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Optional, Tuple

import pickle
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaUpload, MediaFileUpload
from googleapiclient.errors import HttpError

CONFIG = {
    "CREDENTIALS_FILE": "credentials.json",
    "USE_SHARED_DRIVE": False,
    "DRIVE_ID": "",
    "ROOT_FOLDER": "DataLake",
    "PROJECT": "creative",
    "TYPE": "media",
    "INPUT_PATH": r"D:\data",
    "YEAR": None,
    "MONTH": None,
    "ALLOWED_EXT": "",
    "OVERWRITE": False,
    "CHECKSUM": True,
    "DEDUPE_BY_CHECKSUM": True,
    "CHUNK_SIZE_MB": 16,
    "VALIDATE_BASIC": False,
}

MEDIA_TYPE_MAP = {
    'images': ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.svg', '.webp'],
    'videos': ['.mp4', '.avi', '.mov', '.mkv', '.flv', '.wmv', '.webm'],
    'audios': ['.mp3', '.wav', '.aac', '.flac', '.ogg', '.m4a'],
    'docs': ['.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', '.txt', '.csv', '.rtf'],
}

def normalize_path(path: str) -> str:
    return path.replace('\\', '/')

def sha256sum(filepath: str, chunk_size: int=1024*1024) -> str:
    h = hashlib.sha256()
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(chunk_size), b''):
            h.update(chunk)
    return h.hexdigest()

def guess_mime(filepath: str) -> str:
    mt, _ = mimetypes.guess_type(filepath)
    return mt or 'application/octet-stream'

def walk_files(path: str) -> List[str]:
    p = normalize_path(path)
    pth = Path(p)
    if pth.is_file():
        return [str(pth)]
    out = []
    for root, _, files in os.walk(p):
        for f in files:
            out.append(str(Path(root) / f))
    return out

def normalize_exts(exts: str) -> Optional[set]:
    if not exts:
        return None
    return {e if e.startswith('.') else '.' + e for e in (e.strip() for e in exts.split(',')) if e}

def filter_files(files: List[str], allowed_exts: Optional[set]) -> List[str]:
    if not allowed_exts:
        return files
    return [f for f in files if Path(f).suffix.lower() in allowed_exts]

def esc_q(s: str) -> str:
    return s.replace("'", "\\'")

def get_media_folder_name(ext: str) -> str:
    ext = ext.lower()
    for category, exts in MEDIA_TYPE_MAP.items():
        if ext in exts:
            return category
    return 'others'

def build_drive_service():
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token_f:
            creds = pickle.load(token_f)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CONFIG['CREDENTIALS_FILE'], ['https://www.googleapis.com/auth/drive'])
            creds = flow.run_local_server(port=0)
        with open('token.pickle', 'wb') as token_f:
            pickle.dump(creds, token_f)
    return build('drive', 'v3', credentials=creds, cache_discovery=False)

def find_or_create_folder(service, name: str, parent_id: Optional[str], drive_id: Optional[str]) -> str:
    query_parts = [
        f"name = '{esc_q(name)}'",
        "mimeType = 'application/vnd.google-apps.folder'",
        "trashed = false"
    ]
    if parent_id:
        query_parts.append(f"'{parent_id}' in parents")
    query = ' and '.join(query_parts)
    
    params = {
        'q': query,
        'fields': 'files(id, name)',
        'pageSize': 10,
        'supportsAllDrives': True,
        'includeItemsFromAllDrives': True,
    }
    
    if drive_id:
        params['driveId'] = drive_id
        params['corpora'] = 'drive'
    else:
        params['corpora'] = 'user'
    
    res = service.files().list(**params).execute()
    folders = res.get('files', [])
    if folders:
        return folders[0]['id']
    
    body = {
        'name': name,
        'mimeType': 'application/vnd.google-apps.folder',
    }
    if parent_id:
        body['parents'] = [parent_id]

    folder = service.files().create(body=body, supportsAllDrives=True, fields='id').execute()
    return folder['id']

def find_existing_file(service, parent_id: str, filename: str) -> Optional[str]:
    query = f"name = '{esc_q(filename)}' and '{parent_id}' in parents and trashed = false"
    res = service.files().list(q=query, fields='files(id)', supportsAllDrives=True, includeItemsFromAllDrives=True, pageSize=10).execute()
    files = res.get('files', [])
    if files:
        return files[0]['id']
    return None

def main():
    cfg = CONFIG
    cfg['INPUT_PATH'] = normalize_path(cfg['INPUT_PATH'])
    allowed_exts = normalize_exts(cfg['ALLOWED_EXT'])
    
    service = build_drive_service()
    drive_id = cfg['DRIVE_ID'] if cfg['USE_SHARED_DRIVE'] else None
    
    now = datetime.now(timezone.utc)
    year = cfg['YEAR'] or now.year
    month = cfg['MONTH'] or now.month
    
    folders = {}
    folders['root'] = find_or_create_folder(service, cfg['ROOT_FOLDER'], None, drive_id)
    folders['year'] = find_or_create_folder(service, f"{year:04d}", folders['root'], drive_id)
    folders['month'] = find_or_create_folder(service, f"{month:02d}", folders['year'], drive_id)
    folders['project'] = find_or_create_folder(service, cfg['PROJECT'], folders['month'], drive_id)
    folders['type'] = find_or_create_folder(service, cfg['TYPE'], folders['project'], drive_id)
    
    files = walk_files(cfg['INPUT_PATH'])
    files = filter_files(files, allowed_exts)
    
    skipped = []
    failed = []
    
    for file_path in files:
        filename = Path(file_path).name
        ext = Path(file_path).suffix
        media_folder_name = get_media_folder_name(ext)
        
        media_folder_id = find_or_create_folder(service, media_folder_name, folders['type'], drive_id)
        
        checksum = sha256sum(file_path) if cfg['CHECKSUM'] else None
        
        if cfg['DEDUPE_BY_CHECKSUM'] and checksum:
            query = f"appProperties has {{ key='sha256' and value='{checksum}' }}"
            res = service.files().list(q=f"'{media_folder_id}' in parents and {query} and trashed = false",
                                       fields='files(id)', supportsAllDrives=True).execute()
            if res.get('files'):
                print(f"Skipping {filename} due to duplicate checksum")
                skipped.append((file_path, "duplicate checksum"))
                continue
        
        existing_file_id = None
        if not cfg['OVERWRITE']:
            existing_file_id = find_existing_file(service, media_folder_id, filename)
        
        metadata = {'name': filename, 'parents': [media_folder_id]}
        if checksum:
            metadata['appProperties'] = {'sha256': checksum}
        
        media = MediaFileUpload(file_path, mimetype=guess_mime(file_path),
                                chunksize=cfg['CHUNK_SIZE_MB']*1024*1024, resumable=True)
        
        try:
            if existing_file_id:
                print(f"Updating {filename}")
                service.files().update(fileId=existing_file_id, body=metadata, media_body=media).execute()
            else:
                print(f"Uploading {filename}")
                service.files().create(body=metadata, media_body=media, supportsAllDrives=True).execute()
        except HttpError as e:
            print(f"Failed to upload {filename}: {e}", file=sys.stderr)
            failed.append((file_path, str(e)))
    
    print(f"Skipped {len(skipped)} files:")
    for path, reason in skipped:
        print(f"  {path}: {reason}")
    if failed:
        print(f"Failed {len(failed)} files:")
        for path, error in failed:
            print(f"  {path}: {error}")

if __name__ == "__main__":
    main()
