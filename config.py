import os
from os import getenv

from dotenv import load_dotenv
from pyrogram import Client, filters
from pytgcalls import PyTgCalls

# For Local Deploy
if os.path.exists(".env"):
    load_dotenv(".env")

# Necessary Vars
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
SESSION = os.getenv("SESSION")
HNDLR = os.getenv("HNDLR", "+")
SUDO_USERS = list(map(int, os.getenv("SUDO_USERS").split()))
API_URL = getenv("API_URL", 'https://api.thequickearn.xyz') #youtube song url
API_KEY = getenv("API_KEY", None)



contact_filter = filters.create(
    lambda _, __, message: (message.from_user and message.from_user.is_contact)
    or message.outgoing
)

bot = Client(SESSION, API_ID, API_HASH, plugins=dict(root="MusicUserbot"))
call_py = PyTgCalls(bot)
