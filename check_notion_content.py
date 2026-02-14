
from notion_client import Client
from pprint import pprint
import sys

# Windows console encoding fix
try:
    sys.stdout.reconfigure(encoding='utf-8')
except:
    pass

import os

# Environment variables for security
NOTION_TOKEN = os.getenv("NOTION_TOKEN", "")
DATABASE_ID = os.getenv("DATABASE_ID", "")

def check_content():
    notion = Client(auth=NOTION_TOKEN)
    
    # 1. Get latest page
    response = notion.search(
        filter={"property": "object", "value": "page"},
        sort={"direction": "descending", "timestamp": "last_edited_time"},
        page_size=1
    )
    
    if not response["results"]:
        print("No pages found.")
        return

    page = response["results"][0]
    page_id = page["id"]
    props = page["properties"]
    
    title_prop = props.get("Title", {}).get("title", [])
    title = title_prop[0]["text"]["content"] if title_prop else "No Title"
    print(f"Latest Page: {title} ({page_id})")
    print("-" * 20)
    
    # 2. Get children blocks
    children = notion.blocks.children.list(block_id=page_id)
    
    for block in children["results"]:
        btype = block["type"]
        content = ""
        if btype == "heading_2":
            content = block["heading_2"]["rich_text"][0]["text"]["content"]
            print(f"\n[HEADING] {content}")
        elif btype == "paragraph":
            if block["paragraph"]["rich_text"]:
                content = block["paragraph"]["rich_text"][0]["text"]["content"]
                print(f"[TEXT] {content[:50]}...")
        elif btype == "callout":
            if block["callout"]["rich_text"]:
                content = block["callout"]["rich_text"][0]["text"]["content"]
                print(f"[CALLOUT] {content}")
        else:
            print(f"[{btype}]")

if __name__ == "__main__":
    check_content()
