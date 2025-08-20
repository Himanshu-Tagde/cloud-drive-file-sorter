# cloud-drive-file-sorter
A Python script to upload files to Google Drive and automatically organize them into Audio, Video, Images, Documents, and Others folders.

- Supports both **My Drive** and **Shared Drives**.
- OAuth2 authentication with cached tokens (no need to log in every time).
- Simple configuration via constants at the top of the script.

---

## 🛠 Requirements

### Software
- **Python** 3.7 or higher

### Python Packages  
Install dependencies with:

```bash
pip install --upgrade google-api-python-client google-auth google-auth-httplib2 google-auth-oauthlib

