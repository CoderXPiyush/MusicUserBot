import asyncio
import random
import os
import aiohttp  # Added for download_song

from pyrogram import Client, filters
from pyrogram.types import Message
from pytgcalls import StreamType
from pytgcalls.types.input_stream import AudioPiped, AudioVideoPiped
from pytgcalls.types.input_stream.quality import (
    HighQualityAudio,
    HighQualityVideo,
    LowQualityVideo,
    MediumQualityVideo,
)
from youtubesearchpython import VideosSearch

from config import HNDLR, bot, call_py
from MusicUserbot.helpers.queues import QUEUE, add_to_queue, get_queue

from config import API_KEY, API_URL

ROCKSPHOTO = [
    "https://telegra.ph/file/613f681a511feb6d1b186.jpg",
    # ... other URLs
]
IMAGE_THUMBNAIL = random.choice(ROCKSPHOTO)

def convert_seconds(seconds):
    hours = seconds // 3600
    seconds %= 3600
    minutes = seconds // 60
    seconds %= 60
    if hours:
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    return f"{minutes:02d}:{seconds:02d}"

# Ported download_song from youtube.py
async def download_song(link: str):
    video_id = link.split("v=")[-1].split("&")[0]
    download_folder = "downloads"
    os.makedirs(download_folder, exist_ok=True)
    
    # Check if file already exists
    for ext in ["mp3", "m4a", "webm"]:
        file_path = f"{download_folder}/{video_id}.{ext}"
        if os.path.exists(file_path):
            return file_path

    song_url = f"{API_URL}/song/{video_id}?api={API_KEY}"
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get(song_url) as response:
                    if response.status != 200:
                        raise Exception(f"API request failed with status code {response.status}")
                    data = await response.json()
                    status = data.get("status", "").lower()
                    if status == "downloading":
                        await asyncio.sleep(2)
                        continue
                    elif status == "error":
                        error_msg = data.get("error") or data.get("message") or "Unknown error"
                        raise Exception(f"API error: {error_msg}")
                    elif status == "done":
                        download_url = data.get("link")
                        if not download_url:
                            raise Exception("API response did not provide a download URL.")
                        break
                    else:
                        raise Exception(f"Unexpected status '{status}' from API.")
            except Exception as e:
                print(f"Error while checking API status: {e}")
                return None

        try:
            file_format = data.get("format", "mp3")
            file_extension = file_format.lower()
            file_name = f"{video_id}.{file_extension}"
            file_path = os.path.join(download_folder, file_name)

            async with session.get(download_url) as file_response:
                with open(file_path, "wb") as f:
                    while True:
                        chunk = await file_response.content.read(8192)
                        if not chunk:
                            break
                        f.write(chunk)
                return file_path
        except Exception as e:
            print(f"Error occurred while downloading song: {e}")
            return None
    return None

def ytsearch(query):
    try:
        search = VideosSearch(query, limit=1)
        for r in search.result()["result"]:
            ytid = r["id"]
            songname = r["title"][:35] + "..." if len(r["title"]) > 34 else r["title"]
            url = f"https://www.youtube.com/watch?v={ytid}"
            duration = r["duration"]
        return [songname, url, duration]
    except Exception as e:
        print(e)
        return 0

# Modified ytdl to use download_song for audio
async def ytdl(link, stream_type="audio"):
    if stream_type == "audio":
        file_path = await download_song(link)
        if file_path:
            return 1, file_path
        return 0, "Failed to download audio via API"
    else:
        # Keep original yt-dlp for video
        proc = await asyncio.create_subprocess_exec(
            "yt-dlp",
            "-g",
            "-f",
            "best[height<=?720][width<=?1280]",
            f"{link}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        if stdout:
            return 1, stdout.decode().split("\n")[0]
        return 0, stderr.decode()

@Client.on_message(filters.command(["play"], prefixes=f"{HNDLR}"))
async def play(client, m: Message):
    replied = m.reply_to_message
    chat_id = m.chat.id
    if replied:
        if replied.audio or replied.voice:
            await m.delete()
            huehue = await replied.reply("**✧ Processing Request...**")
            dl = await replied.download()
            link = replied.link
            songname = (
                replied.audio.title[:35] + "..." if replied.audio.title
                else replied.audio.file_name[:35] + "..." if replied.audio
                else "Voice Note"
            )
            duration = convert_seconds(replied.audio.duration if replied.audio else replied.voice.duration)

            if chat_id in QUEUE:
                pos = add_to_queue(chat_id, songname, dl, link, "Audio", 0)
                await huehue.delete()
                await m.reply_photo(
                    photo=IMAGE_THUMBNAIL,
                    caption=f"""
**▶ Queued at** `{pos}`
🏷 **Title:** [{songname}]({link})
⏱ **Duration:** `{duration}`
💡 **Status:** `Playing`
🎧 **Requested by:** {m.from_user.mention}
""",
                )
            else:
                await call_py.join_group_call(
                    chat_id,
                    AudioPiped(dl),
                    stream_type=StreamType().pulse_stream,
                )
                add_to_queue(chat_id, songname, dl, link, "Audio", 0)
                await huehue.delete()
                await m.reply_photo(
                    photo=IMAGE_THUMBNAIL,
                    caption=f"""
**▶ Started Playing**
🏷 **Title:** [{songname}]({link})
⏱ **Duration:** `{duration}`
💡 **Status:** `Playing`
🎧 **Requested by:** {m.from_user.mention}
""",
                )
    else:
        if len(m.command) < 2:
            await m.reply("**Reply to an audio file or provide a search query**")
        else:
            await m.delete()
            huehue = await m.reply("**✧ Searching for a song...**")
            query = m.text.split(None, 1)[1]
            search = ytsearch(query)
            if search == 0:
                await huehue.edit("**No results found for the query**")
            else:
                songname, url, duration = search
                hm, ytlink = await ytdl(url, stream_type="audio")
                if hm == 0:
                    await huehue.edit(f"**Download Error ⚠️**\n`{ytlink}`")
                else:
                    if chat_id in QUEUE:
                        pos = add_to_queue(chat_id, songname, ytlink, url, "Audio", 0)
                        await huehue.delete()
                        await m.reply_photo(
                            photo=IMAGE_THUMBNAIL,
                            caption=f"""
**▶ Queued at** `{pos}`
🏷 **Title:** [{songname}]({url})
⏱ **Duration:** `{duration}`
💡 **Status:** `Playing`
🎧 **Requested by:** {m.from_user.mention}
""",
                        )
                    else:
                        try:
                            await call_py.join_group_call(
                                chat_id,
                                AudioPiped(ytlink),
                                stream_type=StreamType().pulse_stream,
                            )
                            add_to_queue(chat_id, songname, ytlink, url, "Audio", 0)
                            await huehue.delete()
                            await m.reply_photo(
                                photo=IMAGE_THUMBNAIL,
                                caption=f"""
**▶ Started Playing**
🏷 **Title:** [{songname}]({url})
⏱ **Duration:** `{duration}`
💡 **Status:** `Playing`
🎧 **Requested by:** {m.from_user.mention}
""",
                            )
                        except Exception as ep:
                            await huehue.edit(f"**Error:** `{ep}`")

@Client.on_message(filters.command(["videoplay", "vplay"], prefixes=f"{HNDLR}"))
async def videoplay(client, m: Message):
    replied = m.reply_to_message
    chat_id = m.chat.id
    if replied:
        if replied.video or replied.document:
            await m.delete()
            huehue = await replied.reply("**✧ Processing Videos...**")
            dl = await replied.download()
            link = replied.link
            Q = 720 if len(m.command) < 2 else int(m.text.split(None, 1)[1]) if m.text.split(None, 1)[1] in ["720", "480", "360"] else 720
            songname = replied.video.file_name[:70] if replied.video else replied.document.file_name[:70] if replied.document else "Video File"

            if chat_id in QUEUE:
                pos = add_to_queue(chat_id, songname, dl, link, "Video", Q)
                await huehue.delete()
                await m.reply_photo(
                    photo=IMAGE_THUMBNAIL,
                    caption=f"""
**▶ Queued at** `{pos}`
🏷 **Title:** [{songname}]({link})
💡 **Status:** `Playing`
🎧 **Requested by:** {m.from_user.mention}
""",
                )
            else:
                hmmm = HighQualityVideo() if Q == 720 else MediumQualityVideo() if Q == 480 else LowQualityVideo()
                await call_py.join_group_call(
                    chat_id,
                    AudioVideoPiped(dl, HighQualityAudio(), hmmm),
                    stream_type=StreamType().pulse_stream,
                )
                add_to_queue(chat_id, songname, dl, link, "Video", Q)
                await huehue.delete()
                await m.reply_photo(
                    photo=IMAGE_THUMBNAIL,
                    caption=f"""
**▶ Started Playing Video**
🏷 **Title:** [{songname}]({link})
💡 **Status:** `Playing`
🎧 **Requested by:** {m.from_user.mention}
""",
                )
    else:
        if len(m.command) < 2:
            await m.reply("**Reply to a video file or provide a search query**")
        else:
            await m.delete()
            huehue = await m.reply("**🔎 Searching... Please Wait**")
            query = m.text.split(None, 1)[1]
            search = ytsearch(query)
            Q = 720
            hmmm = HighQualityVideo()
            if search == 0:
                await huehue.edit("**No results found for the query**")
            else:
                songname, url, duration = search
                hm, ytlink = await ytdl(url, stream_type="video")
                if hm == 0:
                    await huehue.edit(f"**YTDL Error ⚠️**\n`{ytlink}`")
                else:
                    if chat_id in QUEUE:
                        pos = add_to_queue(chat_id, songname, ytlink, url, "Video", Q)
                        await huehue.delete()
                        await m.reply_photo(
                            photo=IMAGE_THUMBNAIL,
                            caption=f"""
**▶ Queued at** `{pos}`
🏷 **Title:** [{songname}]({url})
⏱ **Duration:** `{duration}`
💡 **Status:** `Playing`
🎧 **Requested by:** {m.from_user.mention}
""",
                        )
                    else:
                        try:
                            await call_py.join_group_call(
                                chat_id,
                                AudioVideoPiped(ytlink, HighQualityAudio(), hmmm),
                                stream_type=StreamType().pulse_stream,
                            )
                            add_to_queue(chat_id, songname, ytlink, url, "Video", Q)
                            await huehue.delete()
                            await m.reply_photo(
                                photo=IMAGE_THUMBNAIL,
                                caption=f"""
**▶ Started Playing Video**
🏷 **Title:** [{songname}]({url})
⏱ **Duration:** `{duration}`
💡 **Status:** `Playing`
🎧 **Requested by:** {m.from_user.mention}
""",
                            )
                        except Exception as ep:
                            await huehue.edit(f"**Error:** `{ep}`")

@Client.on_message(filters.command(["playfrom"], prefixes=f"{HNDLR}"))
async def playfrom(client, m: Message):
    chat_id = m.chat.id
    if len(m.command) < 2:
        await m.reply(f"**Usage:**\n\n`{HNDLR}playfrom [chat_id/username]`")
    else:
        args = m.text.split(maxsplit=1)[1]
        chat, limit = (args.split(";")[0], int(args.split(";")[1])) if ";" in args else (args, 10)
        limit = min(limit, 10)  # Cap limit to avoid overload
        await m.delete()
        huehue = await m.reply(f"**✧ Fetching {limit} songs from {chat}...**")
        try:
            async for x in bot.search_messages(chat, limit=limit, filter="audio"):
                location = await x.download()
                songname = x.audio.title[:30] + "..." if x.audio.title else x.audio.file_name[:30] + "..."
                link = x.link
                if chat_id in QUEUE:
                    add_to_queue(chat_id, songname, location, link, "Audio", 0)
                else:
                    await call_py.join_group_call(
                        chat_id,
                        AudioPiped(location),
                        stream_type=StreamType().pulse_stream,
                    )
                    add_to_queue(chat_id, songname, location, link, "Audio", 0)
                    await m.reply_photo(
                        photo=IMAGE_THUMBNAIL,
                        caption=f"""
**▶ Started Playing from {chat}**
🏷 **Title:** [{songname}]({link})
💡 **Status:** `Playing`
🎧 **Requested by:** {m.from_user.mention}
""",
                    )
            await huehue.delete()
            await m.reply(f"**Added {limit} songs to queue**\n**Use `{HNDLR}playlist` to view**")
        except Exception as e:
            await huehue.edit(f"**Error:** `{e}`")

@Client.on_message(filters.command(["playlist", "queue"], prefixes=f"{HNDLR}"))
async def playlist(client, m: Message):
    chat_id = m.chat.id
    if chat_id in QUEUE:
        chat_queue = get_queue(chat_id)
        if len(chat_queue) == 1:
            await m.delete()
            await m.reply(
                f"**✧ Now Playing:**\n[{chat_queue[0][0]}]({chat_queue[0][2]}) | `{chat_queue[0][3]}`",
                disable_web_page_preview=True,
            )
        else:
            QUE = f"**✧ Now Playing:**\n[{chat_queue[0][0]}]({chat_queue[0][2]}) | `{chat_queue[0][3]}`\n\n**⏯ Queue:**"
            for x in range(1, len(chat_queue)):
                QUE += f"\n**#{x}** - [{chat_queue[x][0]}]({chat_queue[x][2]}) | `{chat_queue[x][3]}`"
            await m.reply(QUE, disable_web_page_preview=True)
    else:
        await m.reply("**✧ Nothing is playing**")
