
from notion_client import Client
from pprint import pprint

import os

NOTION_TOKEN = os.getenv("NOTION_TOKEN", "")
DATABASE_ID = os.getenv("DATABASE_ID", "")


def check_latest():
    notion = Client(auth=NOTION_TOKEN)

def check_latest():
    notion = Client(auth=NOTION_TOKEN)
    # Use search which is safer
    response = notion.search(
        filter={"property": "object", "value": "page"},
        sort={"direction": "descending", "timestamp": "last_edited_time"},
        page_size=10
    )
    
    print(f"Found {len(response['results'])} pages.")
    
    for page in response["results"]:
        # Filter by database ID if possible/needed
        if page["parent"]["type"] == "database_id":
             # Normalize IDs for comparison (remove dashes if any)
             parent_id = page["parent"]["database_id"].replace("-", "")
             target_id = DATABASE_ID.replace("-", "")
             if parent_id != target_id:
                 continue
                 
        props = page["properties"]
        # Handle title property (it's a list)
        title_prop = props.get("Title", {}).get("title", [])
        title = title_prop[0]["text"]["content"] if title_prop else "No Title"
             
        # Handle select property
        source_prop = props.get("Source Name", {}).get("select", {})
        source = source_prop.get("name") if source_prop else "Unknown"
        
        print(f"Title: {title}")
        print(f"Source: {source}")
        print("-" * 20)

if __name__ == "__main__":
    check_latest()
