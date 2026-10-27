
from pyrogram import Client

api_id = int(input("Masukkan API_ID: "))
api_hash = input("Masukkan API_HASH: ")

with Client(
    "music_session",
    api_id=api_id,
    api_hash=api_hash,
    in_memory=True
) as app:
    print("\nSESSION_STRING:")
    print(app.export_session_string())
