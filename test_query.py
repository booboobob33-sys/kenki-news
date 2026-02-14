
from notion_client import Client
import sys

import os

NOTION_TOKEN = os.getenv("NOTION_TOKEN", "")
DATABASE_ID = os.getenv("DATABASE_ID", "")

notion = Client(auth=NOTION_TOKEN)

paths = [
    f"databases/{DATABASE_ID}/query",
    f"/databases/{DATABASE_ID}/query",
    f"v1/databases/{DATABASE_ID}/query",
    f"/v1/databases/{DATABASE_ID}/query"
]

for path in paths:
    print(f"Testing path: {path}")
    try:
        res = notion.request(path=path, method="POST", body={"page_size": 1})
        print(f"SUCCESS: {path}")
        break
    except Exception as e:
        print(f"FAILED: {path} - {e}")
