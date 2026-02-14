
from notion_client import Client
from pprint import pprint

import os

NOTION_TOKEN = os.getenv("NOTION_TOKEN", "")
DATABASE_ID = os.getenv("DATABASE_ID", "")

def check_schema():
    notion = Client(auth=NOTION_TOKEN)
    # Search for a page to get properties
    results = notion.search(
        query="",
        filter={"property": "object", "value": "page"},
        page_size=1
    )
    if results["results"]:
        page = results["results"][0]
        # Check if it belongs to our DB (handle direct database parent or data source parent)
        parent = page["parent"]
        db_id = parent.get("database_id")
        if db_id and db_id.replace("-", "") == DATABASE_ID:
            print("Found page linked to target database.")
            props = page["properties"]
            for key, val in props.items():
                print(f"Property Name: '{key}', Type: {val['type']}")
        else:
            print(f"Page found in different parent: {parent}")

if __name__ == "__main__":
    check_schema()
