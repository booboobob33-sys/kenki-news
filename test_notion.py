import os
from notion_client import Client
from pprint import pprint

# Constants from user request
NOTION_TOKEN = os.getenv("NOTION_TOKEN", "")
DATABASE_ID = os.getenv("DATABASE_ID", "")

def test_connection():
    try:
        # Initialize the client
        notion = Client(auth=NOTION_TOKEN)

        # Create a new page in the database
        print("Attempting to create page...")
        new_page = notion.pages.create(
            parent={"database_id": DATABASE_ID},
            properties={
                "Title": {  # Property name is 'Title', not 'Name'
                    "title": [
                        {
                            "text": {
                                "content": "接続テスト成功！"
                            }
                        }
                    ]
                }
            }
        )
        
        print("Success! Page created.")
        print(f"Page ID: {new_page['id']}")
        print(f"Page URL: {new_page['url']}")
        
    except Exception as e:
        print("An error occurred:")
        print(e)
        # If it fails, print available properties helps debugging, but we think we found it.

if __name__ == "__main__":
    test_connection()
