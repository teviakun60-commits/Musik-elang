
import os
import asyncio
import shutil
import logging
from pathlib import Path
from collections import defaultdict

import yt_dlp
from pyrogram import Client, filters, idle
from pytgcalls import PyTgCalls, filters as call_filters
from pytgcalls.types import StreamEnded
from fastapi import FastAPI
import uvicorn

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger("musicbot")

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
SESSION_STRING = os.environ["SESSION_STRING"]

bot = Client(
    "music_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

user = Client(
    "music_user",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING
)

calls = None

# Satu antrean dan worker untuk setiap grup.
queues = defaultdict(asyncio.Queue)
workers = {}
current = {}
finished = {}
stopping = set()

app = FastAPI()


@app.get("/")
async def home():
    return {"status": "Telegram Music Bot is running"}


@app.get("/health")
async def health():
    return {"status": "ok"}


async def download_song(query):
    source = query if query.startswith(
        ("https://", "http://")
    ) else "ytsearch1:" + query

    options = {
        "format": "bestaudio/best",
        "outtmpl": "/tmp/music_%(id)s.%(ext)s",
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "restrictfilenames": True,
        "ffmpeg_location": shutil.which("ffmpeg"),
    }

    loop = asyncio.get_running_loop()

    def work():
        with yt_dlp.YoutubeDL(options) as ydl:
            info = ydl.extract_info(source, download=True)

            if "entries" in info:
                info = next(
                    (item for item in info["entries"] if item),
                    None
                )
                if info is None:
                    raise RuntimeError("Lagu tidak ditemukan.")

            filename = ydl.prepare_filename(info)
            return filename, info.get("title", query)

    path, title = await loop.run_in_executor(None, work)

    if not os.path.isfile(path):
        raise RuntimeError("File audio tidak ditemukan.")

    return path, title


async def music_worker(chat_id):
    queue = queues[chat_id]

    try:
        while not queue.empty() and chat_id not in stopping:
            query, requester = await queue.get()
            path = None

            try:
                current[chat_id] = query
                await bot.send_message(
                    chat_id,
                    f"🔎 Mencari lagu:\n{query}"
                )

                path, title = await download_song(query)

                if chat_id in stopping:
                    break

                done = asyncio.Event()
                finished[chat_id] = done

                await calls.play(chat_id, path)

                await bot.send_message(
                    chat_id,
                    f"🎵 Sedang diputar:\n{title}\n"
                    f"👤 Peminta: {requester}"
                )

                # Menunggu lagu selesai atau dilewati/dihentikan.
                await done.wait()

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.exception("Gagal memutar lagu")
                try:
                    await bot.send_message(
                        chat_id,
                        f"❌ Gagal memutar:\n{query}\n\n{exc}"
                    )
                except Exception:
                    pass
            finally:
                if path:
                    try:
                        Path(path).unlink(missing_ok=True)
                    except Exception:
                        pass

                if finished.get(chat_id):
                    finished.pop(chat_id, None)
                current.pop(chat_id, None)
                queue.task_done()

    finally:
        workers.pop(chat_id, None)
        stopping.discard(chat_id)


@bot.on_message(filters.command("start"))
async def start_command(_, message):
    await message.reply_text(
        "🎶 MUSIC BOT AKTIF\n\n"
        "/play judul lagu — putar atau antrekan lagu\n"
        "/queue — lihat antrean\n"
        "/skip — lagu berikutnya\n"
        "/pause — jeda\n"
        "/resume — lanjutkan\n"
        "/stop — berhenti dan kosongkan antrean"
    )


@bot.on_message(filters.command("play"))
async def play_command(_, message):
    if message.chat.type not in ("group", "supergroup"):
        await message.reply_text(
            "Gunakan perintah ini di grup Telegram."
        )
        return

    if len(message.command) < 2:
        await message.reply_text(
            "Contoh: /play Sheila On 7 Dan"
        )
        return

    if not shutil.which("ffmpeg"):
        await message.reply_text("❌ FFmpeg tidak tersedia.")
        return

    query = " ".join(message.command[1:])
    chat_id = message.chat.id
    requester = message.from_user.first_name if message.from_user else "Anggota"

    stopping.discard(chat_id)
    await queues[chat_id].put((query, requester))
    position = queues[chat_id].qsize()

    if chat_id not in workers:
        workers[chat_id] = asyncio.create_task(
            music_worker(chat_id)
        )
        await message.reply_text(
            f"🎵 Ditambahkan: {query}\nMemulai pemutaran..."
        )
    else:
        await message.reply_text(
            f"➕ Masuk antrean #{position}: {query}"
        )


@bot.on_message(filters.command("queue"))
async def queue_command(_, message):
    chat_id = message.chat.id
    items = list(queues[chat_id]._queue)
    playing = current.get(chat_id)

    lines = ["📃 **ANTREAN MUSIK**"]

    if playing:
        lines.append(f"\n▶️ Sedang diproses/diputar: {playing}")

    if items:
        for i, (query, requester) in enumerate(items, 1):
            lines.append(f"{i}. {query} — {requester}")
    elif not playing:
        lines.append("\nAntrean kosong.")

    await message.reply_text("\n".join(lines))


@bot.on_message(filters.command("skip"))
async def skip_command(_, message):
    chat_id = message.chat.id

    if chat_id not in workers:
        await message.reply_text("Tidak ada lagu yang sedang diputar.")
        return

    done = finished.get(chat_id)
    if done:
        done.set()
        try:
            await calls.leave_call(chat_id)
        except Exception:
            pass
        await message.reply_text("⏭️ Lagu dilewati.")
    else:
        await message.reply_text(
            "Lagu sedang dicari/diunduh. Coba skip lagi sebentar."
        )


@bot.on_message(filters.command("pause"))
async def pause_command(_, message):
    try:
        await calls.pause(message.chat.id)
        await message.reply_text("⏸️ Musik dijeda.")
    except Exception as exc:
        await message.reply_text(f"❌ Tidak bisa jeda: {exc}")


@bot.on_message(filters.command("resume"))
async def resume_command(_, message):
    try:
        await calls.resume(message.chat.id)
        await message.reply_text("▶️ Musik dilanjutkan.")
    except Exception as exc:
        await message.reply_text(f"❌ Tidak bisa lanjut: {exc}")


@bot.on_message(filters.command("stop"))
async def stop_command(_, message):
    chat_id = message.chat.id
    stopping.add(chat_id)

    while not queues[chat_id].empty():
        try:
            queues[chat_id].get_nowait()
            queues[chat_id].task_done()
        except asyncio.QueueEmpty:
            break

    done = finished.get(chat_id)
    if done:
        done.set()

    try:
        await calls.leave_call(chat_id)
    except Exception:
        pass

    task = workers.get(chat_id)
    if task and not task.done():
        task.cancel()

    await message.reply_text("⏹️ Musik dihentikan, antrean dikosongkan.")


@bot.on_message(filters.command("help"))
async def help_command(_, message):
    await start_command(_, message)


async def run_web():
    config = uvicorn.Config(
        app,
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "10000")),
        log_level="info"
    )
    server = uvicorn.Server(config)
    await server.serve()


async def main():
    global calls

    if not shutil.which("ffmpeg"):
        raise RuntimeError("FFmpeg tidak ditemukan.")

    await bot.start()
    log.info("Bot Telegram aktif")

    await user.start()
    log.info("Akun pengguna aktif")

    calls = PyTgCalls(user)

    @calls.on_update(call_filters.stream_end())
    async def stream_end_handler(_, update: StreamEnded):
        done = finished.get(update.chat_id)
        if done:
            done.set()

    await calls.start()
    log.info("PyTgCalls aktif")

    web_task = asyncio.create_task(run_web())

    try:
        await idle()
    finally:
        web_task.cancel()
        for task in list(workers.values()):
            task.cancel()
        try:
            await calls.stop()
        except Exception:
            pass
        await user.stop()
        await bot.stop()


if __name__ == "__main__":
    asyncio.run(main())
