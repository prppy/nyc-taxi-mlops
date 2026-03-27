import json
import os

WATERMARK_FILE = "docs/watermark.json"

def get_last_processed():
    if not os.path.exists(WATERMARK_FILE):
        return None

    with open(WATERMARK_FILE, "r") as f:
        return json.load(f)

def update_watermark(value):
    with open(WATERMARK_FILE, "w") as f:
        json.dump({"last_processed": value}, f)